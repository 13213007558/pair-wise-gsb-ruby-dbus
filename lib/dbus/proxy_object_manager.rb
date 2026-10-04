# frozen_string_literal: true

# This file is part of the ruby-dbus project
# Copyright (C) 2026 Martin Vidner
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License, version 2.1 as published by the Free Software Foundation.
# See the file "COPYING" for the exact licensing terms.

module DBus
  # Client-side mirror of the org.freedesktop.DBus.ObjectManager interface.
  #
  # It keeps a local view of object paths, interfaces and properties.
  # Properties invalidated by PropertiesChanged are represented by
  # {UNKNOWN_PROPERTY} until they are read on demand with Properties.Get.
  class ProxyObjectManager
    # Placeholder for a property whose current value is not known locally.
    UNKNOWN_PROPERTY = :unknown

    PROPERTIES_INTERFACE = "org.freedesktop.DBus.Properties"
    OBJECT_MANAGER_INTERFACE = "org.freedesktop.DBus.ObjectManager"
    BUS_INTERFACE = "org.freedesktop.DBus"

    # @return [ProxyObjectInterface] the remote ObjectManager interface
    attr_reader :interface
    # @return [String,nil] unique bus name of the service owner represented by the mirror
    attr_reader :owner

    # @param interface [ProxyObjectInterface] an ObjectManager proxy interface
    def initialize(interface)
      unless interface.is_a?(ProxyObjectInterface) &&
             interface.name == OBJECT_MANAGER_INTERFACE &&
             interface.object.bus.is_a?(BusConnection)
        raise ArgumentError, "expected an #{OBJECT_MANAGER_INTERFACE} proxy interface on a bus"
      end

      @interface = interface
      @object = interface.object
      @bus = @object.bus
      @destination = @object.destination
      @manager_path = @object.path
      @objects = {}
      @owner = nil
      @snapshot_serial = 0
      @pending_signals = []
      @snapshot_in_progress = false
      subscribe
      begin
        refresh
      rescue DBus::Error => e
        raise unless missing_owner_error?(e)

        @owner = nil
        fail_snapshot(1)
      end
    end

    # @return [Hash{String => Hash{String => Hash{String => Object}}}]
    #   a copied snapshot of object -> interface -> property -> value
    def managed_objects
      deep_copy(@objects)
    end
    alias objects managed_objects

    # @return [Array<String>] currently mirrored object paths
    def paths
      @objects.keys.map(&:to_s)
    end

    # @return [Array<String>] interfaces present at *path*
    def interfaces(path)
      with_object(path).keys
    end

    # @return [Hash{String => Object}] properties of *interface* at *path*
    def properties(path, interface)
      deep_copy(with_interface(path, interface))
    end

    # Return a locally known property value, or read it with Properties.Get if
    # it is locally {UNKNOWN_PROPERTY}.
    # @return [Object]
    def property(path, interface, property)
      value = with_interface(path, interface).fetch(property)
      value = get_property(path, interface, property) if value == UNKNOWN_PROPERTY
      value
    end
    alias get property

    # Force replacing the local view by GetManagedObjects.
    # @return [Hash]
    def refresh
      serial = start_snapshot
      expected_owner = @owner
      destination = @owner || @destination
      begin
        snapshot_owner, managed_objects = request_managed_objects(destination)
        apply_snapshot(serial, expected_owner, snapshot_owner, managed_objects)
        managed_objects
      rescue StandardError
        fail_snapshot(serial)
        raise
      end
    end

    private

    def subscribe
      add_signal_match(OBJECT_MANAGER_INTERFACE, "InterfacesAdded") do |msg|
        queue_or_handle_signal(msg) { handle_interfaces_added(msg) }
      end
      add_signal_match(OBJECT_MANAGER_INTERFACE, "InterfacesRemoved") do |msg|
        queue_or_handle_signal(msg) { handle_interfaces_removed(msg) }
      end
      add_signal_match(PROPERTIES_INTERFACE, "PropertiesChanged") do |msg|
        queue_or_handle_signal(msg) { handle_properties_changed(msg) }
      end

      rule = MatchRule.new
      rule.type = "signal"
      rule.sender = BUS_INTERFACE
      rule.interface = BUS_INTERFACE
      rule.member = "NameOwnerChanged"
      @bus.add_match(rule, &method(:handle_name_owner_changed))
    end

    def add_signal_match(interface_name, member, &block)
      rule = MatchRule.new
      rule.type = "signal"
      rule.interface = interface_name
      rule.member = member
      @bus.add_match(rule, &block)
    end

    def start_snapshot
      @snapshot_serial += 1
      @pending_signals = []
      @snapshot_in_progress = true
      @snapshot_serial
    end

    def apply_snapshot(serial, expected_owner, snapshot_owner, managed_objects)
      return unless serial == @snapshot_serial
      return unless expected_owner.nil? ? @owner.nil? : expected_owner == snapshot_owner

      @owner = snapshot_owner
      @objects = normalize_managed_objects(managed_objects)
      @pending_signals.each do |message|
        dispatch_service_signal(message) if message.sender == @owner
      end
      @pending_signals = []
      @snapshot_in_progress = false
    end

    def fail_snapshot(serial)
      return unless serial == @snapshot_serial

      @pending_signals = []
      @snapshot_in_progress = false
    end

    def queue_or_handle_signal(message)
      return unless managed_path?(message.path)

      if snapshot_in_progress?
        @pending_signals << message
      else
        return unless message.sender == @owner
        yield
      end
    end

    def snapshot_in_progress?
      @snapshot_in_progress
    end

    def dispatch_service_signal(message)
      case message.member
      when "InterfacesAdded"
        handle_interfaces_added(message)
      when "InterfacesRemoved"
        handle_interfaces_removed(message)
      when "PropertiesChanged"
        handle_properties_changed(message)
      end
    end

    def handle_interfaces_added(message)
      path, interfaces = message.params
      @objects[path.to_s] ||= {}
      interfaces.each do |interface_name, properties|
        @objects[path.to_s][interface_name] = normalize_properties(properties)
      end
    end

    def handle_interfaces_removed(message)
      path, interface_names = message.params
      object_interfaces = @objects[path.to_s]
      return unless object_interfaces

      interface_names.each { |interface_name| object_interfaces.delete(interface_name) }
      @objects.delete(path.to_s) if object_interfaces.empty?
    end

    def handle_properties_changed(message)
      interface_name, changed, invalidated = message.params
      properties = @objects.dig(message.path.to_s, interface_name)
      return unless properties

      changed.each { |name, value| properties[name] = value }
      invalidated.each { |name| properties[name] = UNKNOWN_PROPERTY }
    end

    def handle_name_owner_changed(message)
      name, _old_owner, new_owner = message.params
      return unless name == @destination

      serial = start_snapshot
      if new_owner.empty?
        @owner = nil
        apply_snapshot(serial, nil, nil, {})
      else
        @owner = new_owner
        begin
          _owner, managed_objects = request_managed_objects(new_owner)
          apply_snapshot(serial, new_owner, new_owner, managed_objects)
        rescue DBus::Error
          fail_snapshot(serial)
        end
      end
    end

    def missing_owner_error?(error)
      error.name.end_with?(".NameHasNoOwner", ".ServiceUnknown")
    end

    def request_managed_objects(destination)
      message = Message.new(Message::METHOD_CALL)
      message.destination = destination
      message.path = @manager_path
      message.interface = OBJECT_MANAGER_INTERFACE
      message.member = "GetManagedObjects"
      message.sender = @bus.unique_name
      result = nil
      @bus.send_sync(message) do |reply|
        raise DBus::Error.new(reply) if reply.message_type == Message::ERROR

        result = [reply.sender, reply.params.first]
      end
      result
    end

    def get_property(path, interface_name, property_name)
      message = Message.new(Message::METHOD_CALL)
      message.destination = @owner || @destination
      message.path = path
      message.interface = PROPERTIES_INTERFACE
      message.member = "Get"
      message.sender = @bus.unique_name
      message.add_param(Type::STRING, interface_name)
      message.add_param(Type::STRING, property_name)
      value = nil
      @bus.send_sync(message) do |reply|
        raise DBus::Error.new(reply) if reply.message_type == Message::ERROR

        value = reply.params.first
      end
      properties = with_interface(path, interface_name)
      properties[property_name] = value if properties[property_name] == UNKNOWN_PROPERTY
      value
    end

    def normalize_managed_objects(managed_objects)
      managed_objects.each_with_object({}) do |(path, interfaces), objects|
        object_interfaces = interfaces.transform_values do |properties_hash|
          normalize_properties(properties_hash)
        end
        objects[path.to_s] = object_interfaces
      end
    end

    def normalize_properties(properties_hash)
      properties_hash.each_with_object({}) do |(name, value), result|
        result[name] = value
      end
    end

    def with_object(path)
      @objects.fetch(path.to_s) do
        raise DBus::Error, "no mirrored object at #{path}"
      end
    end

    def with_interface(path, interface_name)
      with_object(path).fetch(interface_name) do
        raise DBus::Error, "no mirrored interface #{interface_name} at #{path}"
      end
    end

    def managed_path?(path)
      path == @manager_path || path.start_with?(@manager_path == "/" ? "/" : "#{@manager_path}/")
    end

    def deep_copy(value)
      case value
      when Hash
        value.each_with_object({}) do |(key, item), result|
          result[deep_copy(key)] = deep_copy(item)
        end
      when Array
        value.map { |item| deep_copy(item) }
      else
        value
      end
    end
  end

  ObjectManagerMirror = ProxyObjectManager
end
