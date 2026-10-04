# frozen_string_literal: true

# This file is part of the ruby-dbus project
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License, version 2.1 as published by the Free Software Foundation.
# See the file "COPYING" for the exact licensing terms.

module DBus
  # A client-side mirror of a remote
  # {https://dbus.freedesktop.org/doc/dbus-specification.html#standard-interfaces-objectmanager
  # org.freedesktop.DBus.ObjectManager}.
  #
  # While the {DBus::ObjectManager} mixin *provides* that API on the server
  # side, this class *consumes* it: it maintains a local, always up-to-date
  # view of the remote object tree (paths, interfaces and properties).
  #
  # The view is initialized from `GetManagedObjects` and then kept current
  # with the `InterfacesAdded`, `InterfacesRemoved` and
  # `org.freedesktop.DBus.Properties.PropertiesChanged` signals.
  # `NameOwnerChanged` is tracked for the service name, so that the view is
  # rebuilt when the service is restarted or replaced, and so that late
  # signals from a previous owner of the name cannot re-populate the view.
  #
  # Initialization is race free: the signal matches are installed on the
  # connection *before* `GetManagedObjects` is called. Because messages
  # from a single sender arrive in order, any change signal received before
  # the `GetManagedObjects` reply is already reflected in the returned
  # snapshot and is discarded; any signal sent afterwards arrives after
  # the reply and is applied on top of the snapshot.
  #
  # Properties that are invalidated by `PropertiesChanged` (without a new
  # value) are kept as {UNKNOWN}. Reading such a property fetches it
  # on demand with `Properties.Get` and it becomes known again.
  #
  # The view is updated from signal handlers, that is, from the thread
  # that runs the main loop, like everything else in this library.
  #
  # @example
  #   bus = DBus.session_bus
  #   mirror = DBus::ObjectManagerMirror.new(bus["org.ruby.service"], "/org/ruby/MyInstance")
  #   mirror["/org/ruby/MyInstance/child1"]["org.ruby.TestChild"]["Name"] # => "Child1"
  #   main = DBus::Main.new
  #   main << bus
  #   main.run # the mirror stays up to date as signals get dispatched
  class ObjectManagerMirror
    # The value of a property that has been invalidated by
    # `PropertiesChanged` (without providing a new value) and has not
    # been re-fetched yet.
    # Reading it with {ManagedInterface#[]} fetches it on demand.
    UNKNOWN = ::Object.new
    def UNKNOWN.inspect
      "DBus::ObjectManagerMirror::UNKNOWN"
    end
    UNKNOWN.freeze

    OBJECT_MANAGER_INTERFACE = ObjectManager::OBJECT_MANAGER_INTERFACE
    # same as ProxyObjectInterface::PROPERTY_INTERFACE, which is not loaded yet
    PROPERTIES_INTERFACE = "org.freedesktop.DBus.Properties"
    DBUS_INTERFACE = "org.freedesktop.DBus"

    # Introspection of the standard Properties interface, so that
    # on-demand Get calls do not need to introspect the service.
    PROPERTIES_XML = <<~XML
      <node>
        <interface name="org.freedesktop.DBus.Properties">
          <method name="Get">
            <arg direction="in" type="s"/>
            <arg direction="in" type="s"/>
            <arg direction="out" type="v"/>
          </method>
          <signal name="PropertiesChanged">
            <arg type="s"/>
            <arg type="a{sv}"/>
            <arg type="as"/>
          </signal>
        </interface>
      </node>
    XML

    # @return [ProxyService] the service providing the ObjectManager
    attr_reader :service
    # @return [ObjectPath] the path of the remote ObjectManager object
    attr_reader :path
    # @return [String,nil] the unique name of the current owner of the
    #   service name; nil if the name has no owner (or always, for
    #   a {PeerConnection} where names do not exist)
    attr_reader :owner

    # @param service [ProxyService] the service to mirror
    # @param path [String,ObjectPath] path of its ObjectManager object
    def initialize(service, path)
      @service = service
      @connection = service.connection
      # BusName, nil for a ProxyPeerService
      @name = service.name
      @path = ObjectPath.new(path)

      @objects = {}
      @property_proxies = {}
      @owner = nil
      @synced = false
      # incremented whenever a resync starts, so that a reply belonging
      # to a superseded GetManagedObjects call is discarded
      @generation = 0
      @rules = []

      subscribe
      resync
    end

    # The currently known managed objects.
    # @return [Hash{String => ManagedObject}] object path => object
    def objects
      @objects.dup
    end

    # @return [Array<String>] paths of the currently known managed objects
    def paths
      @objects.keys
    end

    # @param path [String,ObjectPath]
    # @return [ManagedObject,nil] the object at *path*, nil if unknown
    def object(path)
      @objects[path.to_s]
    end
    alias [] object

    # The view as plain nested hashes, like `GetManagedObjects` returns it.
    # Invalidated properties appear as {UNKNOWN}.
    # @return [Hash{String => Hash{String => Hash{String => Object}}}]
    def to_h
      @objects.transform_values(&:to_h)
    end

    # Stop mirroring: remove the match rules from the connection.
    # The view is left in its last known state.
    # @return [void]
    def detach
      @rules.each { |rule| @connection.remove_match(rule) }
      @rules = []
    end

    # @api private
    # Fetch a single property from the service, on demand.
    # Called by {ManagedInterface} when reading an {UNKNOWN} property.
    # @return [Object] the property value
    # @raise [DBus::Error] if the Get call fails
    def fetch_property(path, interface_name, property_name)
      proxy = @property_proxies[path] ||= properties_proxy(path)
      proxy[PROPERTIES_INTERFACE].Get(interface_name, property_name)
    end

    private

    # Subscribe to the signals that keep the view up to date.
    # On a {BusConnection}, #add_match synchronously calls AddMatch on the
    # daemon, so no matching signal can be missed once this returns.
    def subscribe
      om_rule = MatchRule.new
      om_rule.type = "signal"
      om_rule.interface = OBJECT_MANAGER_INTERFACE
      add_match(om_rule) { |msg| handle_object_manager_signal(msg) }

      pc_rule = MatchRule.new
      pc_rule.type = "signal"
      pc_rule.interface = PROPERTIES_INTERFACE
      pc_rule.member = "PropertiesChanged"
      add_match(pc_rule) { |msg| handle_properties_changed(msg) }

      # a peer connection has no bus daemon and no names to track
      return if @name.nil?

      noc_rule = MatchRule.new
      noc_rule.type = "signal"
      noc_rule.interface = DBUS_INTERFACE
      noc_rule.member = "NameOwnerChanged"
      noc_rule.sender = DBUS_INTERFACE
      add_match(noc_rule) { |msg| handle_name_owner_changed(msg) }
    end

    def add_match(rule, &slot)
      @rules << rule
      @connection.add_match(rule, &slot)
    end

    # (Re)initialize the local view from GetManagedObjects.
    #
    # Signals received while the snapshot is being fetched are discarded:
    # they were sent before the GetManagedObjects reply and are therefore
    # already contained in the snapshot (messages from a single sender
    # arrive in order on a single connection). Signals sent after the
    # reply arrive after it and are applied normally.
    # @return [void]
    def resync
      @generation += 1
      generation = @generation
      @synced = false

      reply = call_get_managed_objects
      # a newer resync (from a NameOwnerChanged dispatched while we were
      # waiting for the reply) has superseded this one
      return unless generation == @generation

      if reply.is_a?(Error)
        # most likely the name has no owner (anymore)
        @owner = nil
        replace_objects({})
      else
        # the reply sender is the current owner of the name; addressing
        # the call to the well-known name makes the daemon route it to
        # whoever owns the name when the call is dispatched
        @owner = @name.nil? ? nil : reply.sender
        replace_objects(reply.params.first || {})
      end
      @synced = true
    end

    # @return [Message,DBus::Error] the reply to GetManagedObjects
    def call_get_managed_objects
      msg = Message.new(Message::METHOD_CALL)
      msg.path = @path
      msg.interface = OBJECT_MANAGER_INTERFACE
      msg.member = "GetManagedObjects"
      msg.destination = @name&.to_s
      msg.sender = @connection.unique_name if @connection.respond_to?(:unique_name)
      reply = nil
      @connection.send_sync(msg) { |rmsg| reply = rmsg }
      reply
    end

    # @param managed [Hash{String => Hash{String => Hash{String => Object}}}]
    def replace_objects(managed)
      @property_proxies = {}
      @objects = managed.each_with_object({}) do |(path, interfaces), hash|
        object = ManagedObject.new(self, path.to_s)
        interfaces.each { |name, properties| object.add_interface(name, properties) }
        hash[object.path] = object
      end
    end

    # Should a signal message be applied to the view?
    def accept_signal?(msg)
      return false unless @synced
      # a peer connection has a single possible sender, accept it
      return true if @name.nil?
      # ignore late signals from a previous owner of the name
      !@owner.nil? && msg.sender == @owner
    end

    def handle_object_manager_signal(msg)
      return unless accept_signal?(msg)

      case msg.member
      when "InterfacesAdded"
        path, interfaces_and_properties = msg.params
        interfaces_added(path, interfaces_and_properties)
      when "InterfacesRemoved"
        path, interfaces = msg.params
        interfaces_removed(path, interfaces)
      end
    end

    def interfaces_added(path, interfaces_and_properties)
      object = @objects[path.to_s] ||= ManagedObject.new(self, path.to_s)
      interfaces_and_properties.each do |name, properties|
        object.add_interface(name, properties)
      end
    end

    def interfaces_removed(path, interface_names)
      object = @objects[path.to_s]
      return if object.nil?

      interface_names.each { |name| object.remove_interface(name) }
      @objects.delete(object.path) if object.interface_names.empty?
    end

    def handle_properties_changed(msg)
      return unless accept_signal?(msg)

      interface_name, changed, invalidated = msg.params
      object = @objects[msg.path.to_s]
      return if object.nil?

      object.properties_changed(interface_name, changed, invalidated)
    end

    def handle_name_owner_changed(msg)
      return unless msg.sender == DBUS_INTERFACE

      name, _old_owner, new_owner = msg.params
      return unless name == @name

      if new_owner.empty?
        # the service is gone; the view stays empty until it reappears
        @generation += 1
        @owner = nil
        replace_objects({})
        @synced = true
      else
        # a (possibly different) owner now provides the name; rebuild
        # the view from it. Late signals from the previous owner are
        # ignored because their sender no longer matches the owner.
        resync
      end
    end

    # @return [ProxyObject] a proxy for the standard Properties interface
    #   of the object at *path*, built without introspecting the service
    def properties_proxy(path)
      ProxyObjectFactory.new(PROPERTIES_XML, @connection, @name&.to_s, path,
                             api: ApiOptions::A1).build
    end

    # A client-side view of a single managed object (one object path).
    class ManagedObject
      # @return [String] the object path
      attr_reader :path
      # @return [ObjectManagerMirror] the mirror this object belongs to
      attr_reader :mirror

      def initialize(mirror, path)
        @mirror = mirror
        @path = path
        @interfaces = {}
      end

      # @return [Hash{String => ManagedInterface}] interface name => interface
      def interfaces
        @interfaces.dup
      end

      # @return [Array<String>] names of the interfaces of this object
      def interface_names
        @interfaces.keys
      end

      # @param name [String]
      # @return [ManagedInterface,nil] the interface *name*, nil if unknown
      def interface(name)
        @interfaces[name.to_s]
      end
      alias [] interface

      # @return [Hash{String => Hash{String => Object}}]
      #   interface name => property name => value
      def to_h
        @interfaces.transform_values(&:properties)
      end

      # @api private
      # Add (or replace, with its full set of properties) an interface.
      def add_interface(name, properties)
        @interfaces[name.to_s] = ManagedInterface.new(self, name.to_s, properties)
      end

      # @api private
      def remove_interface(name)
        @interfaces.delete(name.to_s)
      end

      # @api private
      def properties_changed(name, changed, invalidated)
        interface = @interfaces[name] || add_interface(name, {})
        interface.update(changed, invalidated)
      end
    end

    # A client-side view of a single interface of a managed object.
    class ManagedInterface
      # @return [String] the interface name
      attr_reader :name
      # @return [ManagedObject] the object this interface belongs to
      attr_reader :object

      def initialize(object, name, properties)
        @object = object
        @name = name
        @properties = properties.dup
      end

      # All properties as currently known. Invalidated (and not yet
      # re-fetched) properties have the value {ObjectManagerMirror::UNKNOWN}.
      # @return [Hash{String => Object}] property name => value
      def properties
        @properties.dup
      end

      # @return [Array<String>] names of the properties of this interface
      def property_names
        @properties.keys
      end

      # @param name [String]
      # @return [Boolean] is the property value known locally?
      def known?(name)
        name = name.to_s
        @properties.key?(name) && !@properties[name].equal?(UNKNOWN)
      end

      # The value of a property.
      # If it is currently {ObjectManagerMirror::UNKNOWN} (invalidated),
      # it is fetched on demand with `Properties.Get` and becomes known.
      # @param name [String]
      # @return [Object] the property value
      # @raise [KeyError] if the property is not part of the view
      # @raise [DBus::Error] if the on-demand Get call fails
      def [](name)
        name = name.to_s
        value = @properties.fetch(name)
        if value.equal?(UNKNOWN)
          value = @object.mirror.fetch_property(@object.path, @name, name)
          @properties[name] = value
        end
        value
      end

      # @api private
      def update(changed, invalidated)
        changed.each { |name, value| @properties[name.to_s] = value }
        invalidated.each { |name| @properties[name.to_s] = UNKNOWN }
      end
    end
  end
end
