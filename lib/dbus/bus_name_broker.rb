# frozen_string_literal: true

require_relative "bus_name"
require_relative "in_process_message_queue"
require_relative "message"
require_relative "type"

module DBus
  # In-process implementation of the bus daemon name ownership protocol.
  # It intentionally handles only name management, not routing or services.
  class BusNameBroker
    BUS_NAME = "org.freedesktop.DBus"
    BUS_PATH = "/org/freedesktop/DBus"
    BUS_INTERFACE = "org.freedesktop.DBus"

    NameClaim = Struct.new(:connection, :allow_replacement, :do_not_queue)

    def initialize
      @mutex = Mutex.new
      @next_serial = 0
      @connections = {}
      @names = Hash.new { |names, name| names[name] = [] }
    end

    # @return [BusNameBroker::Connection]
    def connect
      self.class::Connection.new(self)
    end

    def send(connection, message)
      @mutex.synchronize do
        if message.member == "Hello" && connection.unique_name.nil?
          @next_serial += 1
          unique_name = ":1.#{@next_serial - 1}"
          connection.unique_name = unique_name
          @connections[unique_name] = connection
          @names[unique_name] = [NameClaim.new(connection, false, true)]
        end
        message.sender ||= connection.unique_name
        handle(connection, message)
      end
    end

    def disconnect(connection)
      @mutex.synchronize do
        return unless @connections.key?(connection.unique_name)

        @connections.delete(connection.unique_name)
        released = []
        @names.each_key do |pending_name|
          claims = @names[pending_name]
          next unless claims.any? { |claim| claim.connection.equal?(connection) }

          claims.reject! { |claim| claim.connection.equal?(connection) }
          released << pending_name
        end

        well_known_names = released.reject { |pending_name| pending_name[0] == ':' }
        well_known_names.each do |name|
          transfer(name, connection, send_lost_signal: false)
        end
        @names.delete(connection.unique_name)
        broadcast_name_owner_changed(connection.unique_name, nil, connection.unique_name)
        @names.delete_if { |_name, claims| claims.empty? }
        connection.message_queue.close_after_dispatch
      end
    end

    private

    def handle(connection, message)
      case message.member
      when "Hello"
        hello(connection, message)
      when "RequestName"
        request_name(connection, message, *message.params)
      when "ReleaseName"
        release_name(connection, message, *message.params)
      when "ListQueuedOwners"
        list_queued_owners(connection, message, *message.params)
      else
        error_reply(message, connection,
                    "org.freedesktop.DBus.Error.UnknownMethod",
                    "BusNameBroker only manages bus names")
      end
    rescue StandardError => e
      error_reply(message, connection, "org.freedesktop.DBus.Error.Failed", e.message)
    end

    def hello(connection, request)
      unique_name = connection.unique_name
      reply(request, connection, Type::STRING, unique_name)
      broadcast_name_owner_changed(nil, unique_name, unique_name)
      send_name_signal(connection, "NameAcquired", unique_name)
    end

    def request_name(connection, request, requested_name_params, flags_params)
      requested_name = requested_name_params.last
      flags = flags_params.last
      BusName.new(requested_name)
      allow_replacement = (flags & Connection::NAME_FLAG_ALLOW_REPLACEMENT) != 0
      replace_existing = (flags & Connection::NAME_FLAG_REPLACE_EXISTING) != 0
      do_not_queue = (flags & Connection::NAME_FLAG_DO_NOT_QUEUE) != 0
      claims = @names[requested_name]
      existing = claims.first
      requester = claims.find { |claim| claim.connection.equal?(connection) }

      result =
        if requester
          if requester.equal?(existing)
            Connection::REQUEST_NAME_REPLY_ALREADY_OWNER
          else
            requester.allow_replacement = allow_replacement
            requester.do_not_queue = do_not_queue
            Connection::REQUEST_NAME_REPLY_IN_QUEUE
          end
        elsif existing.nil? || (replace_existing && existing.allow_replacement)
          old_owner = claims.shift
          claims.unshift(NameClaim.new(connection, allow_replacement, do_not_queue))
          if old_owner && !old_owner.do_not_queue
            claims.insert(1, old_owner)
          end
          Connection::REQUEST_NAME_REPLY_PRIMARY_OWNER
        elsif do_not_queue
          Connection::REQUEST_NAME_REPLY_EXISTS
        else
          claims << NameClaim.new(connection, allow_replacement, do_not_queue)
          Connection::REQUEST_NAME_REPLY_IN_QUEUE
        end

      reply(request, connection, Type::UINT32, result)

      if result == Connection::REQUEST_NAME_REPLY_PRIMARY_OWNER
        transfer(requested_name, old_owner&.connection)
      elsif result == Connection::REQUEST_NAME_REPLY_IN_QUEUE && requester.nil?
        send_name_signal(connection, "NameLost", requested_name)
      end
    end

    def release_name(connection, request, name_params)
      name = name_params.last
      claims = @names[name]
      claim = claims.find { |candidate| candidate.connection.equal?(connection) }
      if claim.nil?
        result = claims.empty? ? BusConnection::RELEASE_NAME_REPLY_NON_EXISTENT
                               : BusConnection::RELEASE_NAME_REPLY_NOT_OWNER
        reply(request, connection, Type::UINT32, result)
        return
      end

      was_owner = claim.equal?(claims.first)
      claims.delete(claim)
      reply(request, connection, Type::UINT32, BusConnection::RELEASE_NAME_REPLY_RELEASED)
      if was_owner
        transfer(name, connection)
      end
      @names.delete(name) if claims.empty?
    end

    def list_queued_owners(connection, request, name_params)
      name = name_params.last
      owners = @names.key?(name) ? @names[name].map { |claim| claim.connection.unique_name } : []
      reply(request, connection, "as", owners)
    end

    def transfer(name, lost_owner, send_lost_signal: true)
      claims = @names[name]
      new_owner = claims.first
      new_unique = new_owner&.connection&.unique_name
      old_unique = lost_owner&.unique_name
      broadcast_name_owner_changed(old_unique, new_unique, name)
      if new_owner
        send_name_signal(new_owner.connection, "NameAcquired", name)
      end
      if send_lost_signal && lost_owner
        send_name_signal(lost_owner, "NameLost", name)
      end
    end

    def broadcast_name_owner_changed(old_owner, new_owner, name)
      @connections.each_value do |connection|
        signal = signal_message("NameOwnerChanged",
                                Type::STRING, name,
                                Type::STRING, old_owner || "",
                                Type::STRING, new_owner || "")
        connection.message_queue.receive(signal)
      end
    end

    def send_name_signal(connection, name_signal, name)
      message = signal_message(name_signal, Type::STRING, name)
      message.destination = connection.unique_name
      connection.message_queue.receive(message)
    end

    def signal_message(member, *params)
      message = Message.new(Message::SIGNAL)
      message.path = BUS_PATH
      message.interface = BUS_INTERFACE
      message.member = member
      message.sender = BUS_NAME
      add_params(message, params)
      message
    end

    def reply(request, connection, *params)
      message = Message.method_return(request)
      message.sender = BUS_NAME
      message.destination = connection.unique_name if request.member == "Hello"
      add_params(message, params)
      connection.message_queue.receive(message)
    end

    def error_reply(request, connection, name, description)
      message = Message.error(request, name, description)
      message.sender = BUS_NAME
      connection.message_queue.receive(message)
    end

    def add_params(message, params)
      params.each_slice(2) { |type, value| message.add_param(type, value) }
      message.params.replace(plain_params(message))
    end

    def plain_params(message)
      message.params.map(&:last)
    end

    # A bus connection backed by the in-process name broker.
    class Connection < BusConnection
      attr_writer :unique_name

      # @api private
      def initialize(broker)
        @message_queue = MessageQueue.new(self, broker)
        initialize_connection_state
        @proxy = nil
        @broker = broker
        send_hello
      end

      def close
        @broker.disconnect(self)
      end

      def dispatch_message_queue
        @message_queue.dispatch { |message| process(message) }
      end

      def add_match(match_rule, &slot)
        connection_add_match = ::DBus::Connection.instance_method(:add_match)
        connection_add_match.bind(self).call(match_rule, &slot)
      end

      def remove_match(match_rule)
        connection_remove_match = ::DBus::Connection.instance_method(:remove_match)
        connection_remove_match.bind(self).call(match_rule)
      end

      private

      def wait_for_message
        @message_queue.pop
      end
    end
  end
end
