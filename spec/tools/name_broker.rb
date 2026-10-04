# frozen_string_literal: true

require "dbus/bus_name"

module DBus
  module Test
    # An in-process replacement for the *name registry* part of the D-Bus
    # daemon, so that integration tests can exercise name ownership without
    # spawning a real dbus-daemon.
    #
    # It implements Hello, RequestName, ReleaseName, ListQueuedOwners and
    # the owner-queue handoff when a connection dies, with the exact
    # ALLOW_REPLACEMENT / REPLACE_EXISTING / DO_NOT_QUEUE semantics of the
    # D-Bus specification. NameOwnerChanged signals are not routed; they are
    # recorded on every connection that said Hello, and in {#signals}.
    #
    # It is deliberately NOT a full bus: no message routing, no activation,
    # no match rules, no sockets.
    class NameBroker
      # Flags for {Connection#request_name}
      NAME_FLAG_ALLOW_REPLACEMENT = 0x1
      NAME_FLAG_REPLACE_EXISTING  = 0x2
      NAME_FLAG_DO_NOT_QUEUE      = 0x4

      # Return values of {Connection#request_name}
      REQUEST_NAME_REPLY_PRIMARY_OWNER = 1
      REQUEST_NAME_REPLY_IN_QUEUE      = 2
      REQUEST_NAME_REPLY_EXISTS        = 3
      REQUEST_NAME_REPLY_ALREADY_OWNER = 4

      # Return values of {Connection#release_name}
      RELEASE_NAME_REPLY_RELEASED     = 1
      RELEASE_NAME_REPLY_NON_EXISTENT = 2
      RELEASE_NAME_REPLY_NOT_OWNER    = 3

      # Every NameOwnerChanged signal emitted so far,
      # as [name, old_owner, new_owner] triples.
      # @return [Array<Array(String, String, String)>]
      attr_reader :signals

      def initialize
        @connections = []
        @entries = {} # well-known name => NameEntry
        @signals = []
        @unique_counter = 0
      end

      # @return [Connection] a new client connection;
      #   call {Connection#hello} on it before anything else
      def connect
        Connection.new(self).tap { |connection| @connections << connection }
      end

      # ListQueuedOwners
      # @param name [String] a well-known name
      # @return [Array<String>] unique names waiting in the queue, in order
      def list_queued_owners(name)
        entry = @entries[name]
        return [] if entry.nil?

        entry.queue.map { |request| request.connection.unique_name }
      end

      # @api private
      # Hello: assign a unique name to *connection*.
      # @return [String] the unique name
      def hello(connection)
        @unique_counter += 1
        connection.unique_name = ":1.#{@unique_counter}"
      end

      # @api private
      # RequestName, see the REQUEST_NAME_REPLY_* constants.
      def request_name(connection, name, flags)
        check_name(name)
        entry = @entries[name]
        return REQUEST_NAME_REPLY_ALREADY_OWNER if entry && entry.owner == connection
        return claim_free_name(connection, name, flags) if entry.nil?

        contested_request(entry, connection, name, flags)
      end

      # @api private
      # ReleaseName, see the RELEASE_NAME_REPLY_* constants.
      def release_name(connection, name)
        check_name(name)
        entry = @entries[name]
        return RELEASE_NAME_REPLY_NON_EXISTENT if entry.nil?

        if entry.owner == connection
          lose_name(name, connection)
          hand_over_or_forget(name, entry)
          RELEASE_NAME_REPLY_RELEASED
        elsif dequeue(entry, connection)
          RELEASE_NAME_REPLY_RELEASED
        else
          RELEASE_NAME_REPLY_NOT_OWNER
        end
      end

      # @api private
      # The connection died: drop it everywhere, handing its names over
      # to the next queued owners.
      def disconnect(connection)
        @connections.delete(connection)
        @entries.keys.each { |name| drop_connection(connection, name) }
        unique_name = connection.unique_name
        emit_signal(unique_name, unique_name, "") unless unique_name.nil?
      end

      # @api private
      # Record a NameOwnerChanged and deliver it to all connections.
      def emit_signal(name, old_owner, new_owner)
        triple = [name, old_owner, new_owner]
        @signals << triple
        @connections.each { |connection| connection.emit(*triple) }
      end

      private

      def claim_free_name(connection, name, flags)
        entry = NameEntry.new
        @entries[name] = entry
        become_owner(entry, name, connection, flags)
        REQUEST_NAME_REPLY_PRIMARY_OWNER
      end

      # RequestName for a name that already has a primary owner.
      def contested_request(entry, connection, name, flags)
        replaceable = (entry.owner_flags & NAME_FLAG_ALLOW_REPLACEMENT) != 0
        if (flags & NAME_FLAG_REPLACE_EXISTING) != 0 && replaceable
          replace_owner(entry, connection, name, flags)
        elsif (flags & NAME_FLAG_DO_NOT_QUEUE) != 0
          REQUEST_NAME_REPLY_EXISTS
        else
          enqueue(entry, connection, flags)
          REQUEST_NAME_REPLY_IN_QUEUE
        end
      end

      # REPLACE_EXISTING meets ALLOW_REPLACEMENT: the requester takes over
      # and the replaced owner keeps its flags at the head of the queue.
      def replace_owner(entry, connection, name, flags)
        entry.queue.unshift(QueuedRequest.new(entry.owner, entry.owner_flags))
        dequeue(entry, connection) # the requester may have been queued
        lose_name(name, entry.owner)
        become_owner(entry, name, connection, flags)
        REQUEST_NAME_REPLY_PRIMARY_OWNER
      end

      def drop_connection(connection, name)
        entry = @entries[name]
        if entry.owner == connection
          lose_name(name, connection)
          hand_over_or_forget(name, entry)
        else
          dequeue(entry, connection)
        end
      end

      # The primary owner went away; promote the queue head (with the flags
      # of its original request) or forget the name entirely.
      def hand_over_or_forget(name, entry)
        request = entry.queue.shift
        if request.nil?
          @entries.delete(name)
        else
          become_owner(entry, name, request.connection, request.flags)
        end
      end

      def become_owner(entry, name, connection, flags)
        entry.owner = connection
        entry.owner_flags = flags
        emit_signal(name, "", connection.unique_name)
      end

      def lose_name(name, connection)
        emit_signal(name, connection.unique_name, "")
      end

      def enqueue(entry, connection, flags)
        return if entry.queue.any? { |request| request.connection == connection }

        entry.queue << QueuedRequest.new(connection, flags)
      end

      # @return [Boolean] whether *connection* was in the queue
      def dequeue(entry, connection)
        size_before = entry.queue.size
        entry.queue.reject! { |request| request.connection == connection }
        entry.queue.size != size_before
      end

      def check_name(name)
        raise ArgumentError, "Invalid bus name #{name.inspect}" unless DBus::BusName.valid?(name)
        if name.start_with?(":")
          raise ArgumentError, "Cannot request or release a unique name #{name.inspect}"
        end
      end

      # The registry state of one well-known name.
      class NameEntry
        attr_accessor :owner, :owner_flags, :queue

        def initialize
          @owner = nil
          @owner_flags = 0
          @queue = []
        end
      end

      # A queued RequestName call; the flags are remembered so that they
      # still apply when the connection is promoted to primary owner.
      QueuedRequest = Struct.new(:connection, :flags)

      # One client of the broker, roughly a connection to the daemon.
      class Connection
        # @return [String, nil] unique name assigned by {#hello}
        attr_reader :unique_name
        # NameOwnerChanged signals this connection has seen,
        # as [name, old_owner, new_owner] triples.
        attr_reader :signals
        # @api private
        attr_writer :unique_name

        def initialize(broker)
          @broker = broker
          @unique_name = nil
          @signals = []
          @disconnected = false
        end

        # Hello: must be the first call on a connection.
        # @return [String] the unique name assigned by the broker
        def hello
          check_connected
          raise "Hello was already called" unless @unique_name.nil?

          @broker.hello(self)
          @unique_name
        end

        # RequestName, see {NameBroker} for flags and return values.
        def request_name(name, flags = 0)
          check_ready
          @broker.request_name(self, name, flags)
        end

        # ReleaseName, see {NameBroker} for return values.
        def release_name(name)
          check_ready
          @broker.release_name(self, name)
        end

        # Close the connection: owned names are handed over to the next
        # queued owners, queued requests are dropped.
        def disconnect
          return if @disconnected

          @broker.disconnect(self)
          @disconnected = true
        end

        def disconnected?
          @disconnected
        end

        # @api private
        # Called by the broker to deliver a NameOwnerChanged signal.
        def emit(name, old_owner, new_owner)
          @signals << [name, old_owner, new_owner]
        end

        private

        def check_connected
          raise "Connection is disconnected" if @disconnected
        end

        def check_ready
          check_connected
          raise "Call Hello first" if @unique_name.nil?
        end
      end
    end
  end
end
