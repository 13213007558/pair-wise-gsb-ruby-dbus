#!/usr/bin/env rspec
# frozen_string_literal: true

require_relative "spec_helper"
require "dbus"

class FakeManagerBus < DBus::BusConnection
  attr_reader :unique_name, :add_match_events

  def initialize
    @message_queue = Object.new
    @method_call_replies = {}
    @method_call_msgs = {}
    @signal_matchrules = {}
    @unique_name = ":1.client"
    @rules = {}
    @add_match_events = []
  end

  def add_match(rule, &slot)
    @add_match_events << rule.to_s
    existed = @rules.key?(rule.to_s)
    @rules[rule.to_s] = slot
    existed
  end

  def emit_signal(message)
    @rules.each do |rule, slot|
      slot.call(message) if DBus::MatchRule.new.from_s(rule).match(message)
    end
  end
end

describe DBus::ProxyObjectManager do
  let(:service_name) { "org.ruby.test" }
  let(:bus) { FakeManagerBus.new }
  let(:service) { DBus::ProxyService.new(service_name, bus) }
  let(:manager_object) { service.object("/org/ruby") }
  let(:manager_interface) do
    DBus::ProxyObjectInterface.new(manager_object, described_class::OBJECT_MANAGER_INTERFACE)
  end

  def signal(member:, interface:, path:, sender:, params:)
    message = DBus::Message.new(DBus::Message::SIGNAL)
    message.interface = interface
    message.member = member
    message.path = path
    message.sender = sender
    params.each { |value| message.params << value }
    bus.emit_signal(message)
  end

  def return_from(call, sender:, params:, &callback)
    reply = DBus::MethodReturnMessage.new.reply_to(call)
    reply.sender = sender
    params.each { |value| reply.params << value }
    callback.call(reply)
  end

  def managed_snapshot
    {
      "/org/ruby/child" => {
        "org.ruby.Child" => { "Name" => "Before", "Age" => 1 }
      }
    }
  end

  def reply_with_managed_objects(sender: ":1.owner", objects: managed_snapshot)
    allow(bus).to receive(:send_sync) do |call, &callback|
      return_from(call, sender: sender, params: [objects], &callback)
    end
  end

  it "installs signal matches before fetching the initial snapshot" do
    allow(bus).to receive(:send_sync) do |call, &callback|
      expect(bus.add_match_events.size).to eq(4)
      return_from(call, sender: ":1.owner", params: [{}], &callback)
    end

    described_class.new(manager_interface)

    object_manager_rule = "interface='org.freedesktop.DBus.ObjectManager'"
    properties_rule = "interface='org.freedesktop.DBus.Properties'"
    owner_rule = "sender='org.freedesktop.DBus',interface='org.freedesktop.DBus'"
    expect(bus.add_match_events).to include(
      a_string_matching(/#{Regexp.escape(object_manager_rule)}.*member='InterfacesAdded'/),
      a_string_matching(/#{Regexp.escape(object_manager_rule)}.*member='InterfacesRemoved'/),
      a_string_matching(/#{Regexp.escape(properties_rule)}.*member='PropertiesChanged'/),
      a_string_matching(/#{Regexp.escape(owner_rule)}.*member='NameOwnerChanged'/)
    )
  end

  it "keeps signals that arrive between subscription and the initial snapshot" do
    allow(bus).to receive(:send_sync) do |call, &callback|
      signal(
        member: "InterfacesAdded",
        interface: described_class::OBJECT_MANAGER_INTERFACE,
        path: "/org/ruby/child",
        sender: ":1.owner",
        params: ["/org/ruby/child", { "org.ruby.Child" => { "Name" => "Late" } }]
      )
      return_from(call, sender: ":1.owner", params: [{}], &callback)
    end

    mirror = described_class.new(manager_interface)

    expect(mirror.paths).to eq(["/org/ruby/child"])
    expect(mirror.property("/org/ruby/child", "org.ruby.Child", "Name"))
      .to eq("Late")
  end

  it "drops an initial snapshot reply from a former owner after takeover" do
    allow(bus).to receive(:send_sync) do |call, &callback|
      allow(bus).to receive(:send_sync) do |nested_call, &nested_callback|
        return_from(
          nested_call,
          sender: ":2.owner",
          params: ["/org/ruby/new" => { "org.ruby.Child" => { "Name" => "New" } }],
          &nested_callback
        )
      end
      signal(
        member: "NameOwnerChanged",
        interface: "org.freedesktop.DBus",
        path: "/org/freedesktop/DBus",
        sender: "org.freedesktop.DBus",
        params: [service_name, ":1.owner", ":2.owner"]
      )
      return_from(
        call,
        sender: ":1.owner",
        params: ["/org/ruby/old" => { "org.ruby.Child" => { "Name" => "Old" } }],
        &callback
      )
    end

    mirror = described_class.new(manager_interface)

    expect(mirror.owner).to eq(":2.owner")
    expect(mirror.paths).to eq(["/org/ruby/new"])
  end

  it "represents invalidated properties as unknown and fills them in on demand" do
    reply_with_managed_objects
    mirror = described_class.new(manager_interface)

    signal(
      member: "PropertiesChanged",
      interface: described_class::PROPERTIES_INTERFACE,
      path: "/org/ruby/child",
      sender: ":1.owner",
      params: ["org.ruby.Child", { "Age" => 2 }, ["Name"]]
    )

    expect(mirror.properties("/org/ruby/child", "org.ruby.Child"))
      .to eq("Name" => described_class::UNKNOWN_PROPERTY, "Age" => 2)

    allow(bus).to receive(:send_sync) do |call, &callback|
      expect(call.destination).to eq(":1.owner")
      expect(call.path).to eq("/org/ruby/child")
      expect(call.interface).to eq(described_class::PROPERTIES_INTERFACE)
      expect(call.member).to eq("Get")
      return_from(call, sender: ":1.owner", params: ["After"], &callback)
    end

    expect(mirror.property("/org/ruby/child", "org.ruby.Child", "Name"))
      .to eq("After")
    expect(mirror.properties("/org/ruby/child", "org.ruby.Child"))
      .to eq("Name" => "After", "Age" => 2)
  end

  it "adds and removes interfaces from ObjectManager signals" do
    reply_with_managed_objects(objects: {})
    mirror = described_class.new(manager_interface)

    signal(
      member: "InterfacesAdded",
      interface: described_class::OBJECT_MANAGER_INTERFACE,
      path: "/org/ruby/child",
      sender: ":1.owner",
      params: ["/org/ruby/child", { "org.ruby.Child" => {} }]
    )
    expect(mirror.interfaces("/org/ruby/child")).to eq(["org.ruby.Child"])

    signal(
      member: "InterfacesRemoved",
      interface: described_class::OBJECT_MANAGER_INTERFACE,
      path: "/org/ruby/child",
      sender: ":1.owner",
      params: ["/org/ruby/child", ["org.ruby.Child"]]
    )
    expect(mirror.paths).to eq([])
  end

  it "drops signals from a former owner after NameOwnerChanged" do
    snapshots = [
      { "/org/ruby/old" => { "org.ruby.Child" => { "Name" => "Old" } } },
      { "/org/ruby/new" => { "org.ruby.Child" => { "Name" => "New" } } }
    ]
    reply_with_managed_objects(objects: snapshots.first)
    mirror = described_class.new(manager_interface)

    allow(bus).to receive(:send_sync) do |call, &callback|
      return_from(call, sender: ":2.owner", params: [snapshots[1]], &callback)
    end
    signal(
      member: "NameOwnerChanged",
      interface: "org.freedesktop.DBus",
      path: "/org/freedesktop/DBus",
      sender: "org.freedesktop.DBus",
      params: [service_name, ":1.owner", ":2.owner"]
    )

    signal(
      member: "InterfacesAdded",
      interface: described_class::OBJECT_MANAGER_INTERFACE,
      path: "/org/ruby/old",
      sender: ":1.owner",
      params: ["/org/ruby/old", { "org.ruby.Late" => {} }]
    )

    expect(mirror.owner).to eq(":2.owner")
    expect(mirror.paths).to eq(["/org/ruby/new"])
    expect(mirror.interfaces("/org/ruby/new")).to eq(["org.ruby.Child"])
    expect { mirror.interfaces("/org/ruby/old") }.to raise_error(DBus::Error, /no mirrored object/)
  end

  it "starts empty when the service does not have an owner and refreshes on activation" do
    allow(bus).to receive(:send_sync) do |call, &callback|
      reply = DBus::ErrorMessage.new("org.freedesktop.DBus.Error.NameHasNoOwner", "no owner")
      reply = reply.reply_to(call)
      reply.sender = "org.freedesktop.DBus"
      callback.call(reply)
    end
    mirror = described_class.new(manager_interface)
    expect(mirror.paths).to eq([])

    allow(bus).to receive(:send_sync) do |call, &callback|
      return_from(
        call,
        sender: ":3.owner",
        params: ["/org/ruby/new" => { "org.ruby.Child" => { "Name" => "New" } }],
        &callback
      )
    end
    signal(
      member: "NameOwnerChanged",
      interface: "org.freedesktop.DBus",
      path: "/org/freedesktop/DBus",
      sender: "org.freedesktop.DBus",
      params: [service_name, "", ":3.owner"]
    )

    expect(mirror.owner).to eq(":3.owner")
    expect(mirror.paths).to eq(["/org/ruby/new"])
  end

  it "is available through ProxyObjectInterface#object_manager" do
    reply_with_managed_objects(objects: {})

    expect(manager_interface.object_manager).to be_a(described_class)
  end
end
