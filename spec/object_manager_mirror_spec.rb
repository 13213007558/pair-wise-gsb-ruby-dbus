#!/usr/bin/env rspec
# frozen_string_literal: true

require_relative "spec_helper"
require "dbus"

# A fake Connection that plays both the bus daemon and the remote service.
# It implements the parts of DBus::Connection that ObjectManagerMirror uses:
# - add_match/remove_match + signal dispatch (using the real MatchRule)
# - send_sync (for GetManagedObjects)
# - send_sync_or_async (for Properties.Get, via a ProxyObjectInterface)
class FakeConnection
  attr_reader :unique_name, :log
  # ->(call_msg) { reply_msg_or_error } set by the test
  attr_accessor :method_handler

  def initialize
    @unique_name = ":1.1"
    @matches = {}
    @log = []
  end

  def add_match(match_rule, &slot)
    @log << [:add_match, match_rule.to_s]
    @matches[match_rule.to_s] = slot
  end

  def remove_match(match_rule)
    @log << [:remove_match, match_rule.to_s]
    @matches.delete(match_rule.to_s)
  end

  def match_rule_strings
    @matches.keys
  end

  # Emulate Connection#process for an incoming signal
  def receive_signal(msg)
    @matches.each do |mrs, slot|
      slot.call(msg) if DBus::MatchRule.new.from_s(mrs).match(msg)
    end
  end

  def send_sync(msg, &block)
    @log << [:call, msg.member]
    block.call(handle_call(msg))
  end

  def send_sync_or_async(msg, &_block)
    @log << [:call, msg.member]
    reply = handle_call(msg)
    raise reply if reply.is_a?(DBus::Error)

    reply.params
  end

  private

  def handle_call(msg)
    raise "FakeConnection: no method_handler set" unless method_handler

    method_handler.call(msg)
  end
end

describe DBus::ObjectManagerMirror do
  let(:connection) { FakeConnection.new }
  let(:service) { DBus::ProxyService.new("org.ruby.service", connection) }
  let(:om_path) { "/org/ruby/MyInstance" }
  let(:owner) { ":1.42" }

  let(:child1_path) { "/org/ruby/MyInstance/child1" }
  let(:child2_path) { "/org/ruby/MyInstance/child2" }
  let(:snapshot) do
    {
      child1_path => {
        "org.ruby.TestChild" => { "Name" => "Child1" }
      }
    }
  end

  # the GetManagedObjects reply the fake service will make
  let(:gmo_reply) { reply_message(sender: owner, params: [snapshot]) }

  def reply_message(sender:, params:)
    msg = DBus::Message.new(DBus::Message::METHOD_RETURN)
    msg.sender = sender
    msg.instance_variable_set(:@params, params)
    msg
  end

  def signal_message(sender:, path:, interface:, member:, params: [])
    msg = DBus::Message.new(DBus::Message::SIGNAL)
    msg.sender = sender
    msg.path = path
    msg.interface = interface
    msg.member = member
    msg.instance_variable_set(:@params, params)
    msg
  end

  def interfaces_added(sender:, path:, interfaces_and_properties:)
    signal_message(sender: sender, path: om_path,
                   interface: "org.freedesktop.DBus.ObjectManager",
                   member: "InterfacesAdded",
                   params: [path, interfaces_and_properties])
  end

  def interfaces_removed(sender:, path:, interfaces:)
    signal_message(sender: sender, path: om_path,
                   interface: "org.freedesktop.DBus.ObjectManager",
                   member: "InterfacesRemoved",
                   params: [path, interfaces])
  end

  def properties_changed(sender:, path:, interface:, changed: {}, invalidated: [])
    signal_message(sender: sender, path: path,
                   interface: "org.freedesktop.DBus.Properties",
                   member: "PropertiesChanged",
                   params: [interface, changed, invalidated])
  end

  def name_owner_changed(name:, old_owner:, new_owner:, sender: "org.freedesktop.DBus")
    signal_message(sender: sender, path: "/org/freedesktop/DBus",
                   interface: "org.freedesktop.DBus",
                   member: "NameOwnerChanged",
                   params: [name, old_owner, new_owner])
  end

  # make the fake service answer GetManagedObjects with gmo_reply
  def answer_get_managed_objects
    connection.method_handler = lambda do |call|
      expect(call.member).to eq "GetManagedObjects"
      expect(call.interface).to eq "org.freedesktop.DBus.ObjectManager"
      expect(call.path).to eq om_path
      expect(call.destination).to eq "org.ruby.service"
      gmo_reply
    end
  end

  subject(:mirror) { described_class.new(service, om_path) }

  before { answer_get_managed_objects }

  describe "#initialize" do
    it "subscribes to the signals before taking the snapshot" do
      mirror
      kinds = connection.log.map(&:first)
      # all subscriptions happen before the GetManagedObjects call,
      # so no signal can be missed in between
      expect(kinds).to eq([:add_match] * 3 + [:call])
    end

    it "takes the snapshot with GetManagedObjects" do
      expect(mirror.to_h).to eq(snapshot)
      expect(mirror.paths).to eq([child1_path])
    end

    it "learns the service owner from the reply sender" do
      expect(mirror.owner).to eq(owner)
    end

    context "when the service name has no owner" do
      let(:gmo_reply) { DBus::Error.new("no owner", "org.freedesktop.DBus.Error.ServiceUnknown") }

      it "starts with an empty view" do
        expect(mirror.owner).to be_nil
        expect(mirror.to_h).to eq({})
      end
    end
  end

  describe "initialization without a window" do
    it "discards signals that the snapshot already covers" do
      # the service emits InterfacesAdded and PropertiesChanged and only
      # then answers GetManagedObjects; the snapshot already contains
      # the final state, so the signals must not be applied on top of it
      connection.method_handler = lambda do |call|
        connection.receive_signal(
          interfaces_added(sender: owner, path: child1_path,
                           interfaces_and_properties: {
                             "org.ruby.TestChild" => { "Name" => "Stale" }
                           })
        )
        connection.receive_signal(
          properties_changed(sender: owner, path: child1_path,
                             interface: "org.ruby.TestChild",
                             changed: { "Name" => "Stale2" })
        )
        gmo_reply
      end

      expect(mirror.to_h).to eq(snapshot)
    end

    it "applies signals that arrive after the snapshot" do
      mirror
      connection.receive_signal(
        interfaces_added(sender: owner, path: child2_path,
                         interfaces_and_properties: {
                           "org.ruby.TestChild" => { "Name" => "Child2" }
                         })
      )
      expect(mirror.paths).to contain_exactly(child1_path, child2_path)
    end
  end

  describe "InterfacesAdded" do
    it "adds a new object with its interfaces and properties" do
      mirror
      connection.receive_signal(
        interfaces_added(sender: owner, path: child2_path,
                         interfaces_and_properties: {
                           "org.ruby.TestChild" => { "Name" => "Child2" }
                         })
      )

      object = mirror[child2_path]
      expect(object.interface_names).to eq(["org.ruby.TestChild"])
      expect(object["org.ruby.TestChild"]["Name"]).to eq("Child2")
    end

    it "merges a new interface into a known object" do
      mirror
      connection.receive_signal(
        interfaces_added(sender: owner, path: child1_path,
                         interfaces_and_properties: {
                           "org.ruby.Other" => { "Size" => 42 }
                         })
      )

      object = mirror[child1_path]
      expect(object.interface_names).to contain_exactly("org.ruby.TestChild", "org.ruby.Other")
      expect(object["org.ruby.TestChild"]["Name"]).to eq("Child1")
      expect(object["org.ruby.Other"]["Size"]).to eq(42)
    end
  end

  describe "InterfacesRemoved" do
    it "removes the listed interfaces but keeps the others" do
      mirror
      connection.receive_signal(
        interfaces_added(sender: owner, path: child1_path,
                         interfaces_and_properties: { "org.ruby.Other" => {} })
      )
      connection.receive_signal(
        interfaces_removed(sender: owner, path: child1_path,
                           interfaces: ["org.ruby.Other"])
      )

      expect(mirror[child1_path].interface_names).to eq(["org.ruby.TestChild"])
    end

    it "removes the object when its last interface is gone" do
      mirror
      connection.receive_signal(
        interfaces_removed(sender: owner, path: child1_path,
                           interfaces: ["org.ruby.TestChild"])
      )

      expect(mirror.paths).to eq([])
      expect(mirror[child1_path]).to be_nil
    end
  end

  describe "PropertiesChanged" do
    it "applies changed properties" do
      mirror
      connection.receive_signal(
        properties_changed(sender: owner, path: child1_path,
                           interface: "org.ruby.TestChild",
                           changed: { "Name" => "New Name" })
      )

      expect(mirror[child1_path]["org.ruby.TestChild"]["Name"]).to eq("New Name")
    end

    it "keeps invalidated properties as UNKNOWN" do
      mirror
      connection.receive_signal(
        properties_changed(sender: owner, path: child1_path,
                           interface: "org.ruby.TestChild",
                           invalidated: ["Name"])
      )

      iface = mirror[child1_path]["org.ruby.TestChild"]
      expect(iface.properties["Name"]).to be(DBus::ObjectManagerMirror::UNKNOWN)
      expect(iface.known?("Name")).to be false
    end

    it "fetches an invalidated property on demand and then knows it" do
      mirror
      connection.receive_signal(
        properties_changed(sender: owner, path: child1_path,
                           interface: "org.ruby.TestChild",
                           invalidated: ["Name"])
      )

      gets = 0
      connection.method_handler = lambda do |call|
        gets += 1
        expect(call.member).to eq "Get"
        expect(call.interface).to eq "org.freedesktop.DBus.Properties"
        expect(call.path).to eq child1_path
        expect(call.params).to eq [["s", "org.ruby.TestChild"], ["s", "Name"]]
        reply_message(sender: owner, params: ["Fetched Name"])
      end

      iface = mirror[child1_path]["org.ruby.TestChild"]
      expect(iface["Name"]).to eq("Fetched Name")
      expect(iface.known?("Name")).to be true
      # a second read is served from the local view
      expect(iface["Name"]).to eq("Fetched Name")
      expect(gets).to eq(1)
    end

    it "ignores signals for objects that are not in the view" do
      mirror
      expect do
        connection.receive_signal(
          properties_changed(sender: owner, path: "/org/ruby/Elsewhere",
                             interface: "org.ruby.TestChild",
                             changed: { "Name" => "Nope" })
        )
      end.not_to raise_error
      expect(mirror.paths).to eq([child1_path])
    end
  end

  describe "NameOwnerChanged" do
    context "when the service vanishes" do
      it "clears the view" do
        mirror
        connection.receive_signal(
          name_owner_changed(name: "org.ruby.service", old_owner: owner, new_owner: "")
        )

        expect(mirror.owner).to be_nil
        expect(mirror.to_h).to eq({})
      end

      it "ignores late signals from the gone owner" do
        mirror
        connection.receive_signal(
          name_owner_changed(name: "org.ruby.service", old_owner: owner, new_owner: "")
        )
        connection.receive_signal(
          interfaces_added(sender: owner, path: child2_path,
                           interfaces_and_properties: {
                             "org.ruby.TestChild" => { "Name" => "Zombie" }
                           })
        )

        expect(mirror.to_h).to eq({})
      end
    end

    context "when a new owner takes over" do
      let(:new_owner) { ":1.77" }
      let(:new_snapshot) do
        {
          child2_path => {
            "org.ruby.TestChild" => { "Name" => "Child2" }
          }
        }
      end

      def take_over
        connection.receive_signal(
          name_owner_changed(name: "org.ruby.service", old_owner: owner, new_owner: new_owner)
        )
      end

      before do
        mirror
        # the new owner serves a different snapshot
        connection.method_handler = lambda do |_call|
          reply_message(sender: new_owner, params: [new_snapshot])
        end
        take_over
      end

      it "rebuilds the view from the new owner" do
        expect(mirror.owner).to eq(new_owner)
        expect(mirror.to_h).to eq(new_snapshot)
      end

      it "ignores late signals from the old owner" do
        connection.receive_signal(
          interfaces_added(sender: owner, path: child1_path,
                           interfaces_and_properties: {
                             "org.ruby.TestChild" => { "Name" => "Zombie" }
                           })
        )
        connection.receive_signal(
          properties_changed(sender: owner, path: child2_path,
                             interface: "org.ruby.TestChild",
                             changed: { "Name" => "Zombie" })
        )

        expect(mirror.to_h).to eq(new_snapshot)
      end

      it "applies signals from the new owner" do
        connection.receive_signal(
          properties_changed(sender: new_owner, path: child2_path,
                             interface: "org.ruby.TestChild",
                             changed: { "Name" => "Renamed" })
        )

        expect(mirror[child2_path]["org.ruby.TestChild"]["Name"]).to eq("Renamed")
      end
    end

    it "ignores NameOwnerChanged for other names" do
      mirror
      connection.receive_signal(
        name_owner_changed(name: "org.other.Service", old_owner: ":1.5", new_owner: "")
      )

      expect(mirror.owner).to eq(owner)
      expect(mirror.to_h).to eq(snapshot)
    end

    it "ignores NameOwnerChanged not coming from the bus driver" do
      mirror
      connection.receive_signal(
        name_owner_changed(name: "org.ruby.service", old_owner: owner, new_owner: "",
                           sender: ":1.666")
      )

      expect(mirror.owner).to eq(owner)
      expect(mirror.to_h).to eq(snapshot)
    end

    it "picks the service up when it appears later" do
      connection.method_handler = ->(_call) { DBus::Error.new("no owner") }
      mirror
      expect(mirror.to_h).to eq({})

      answer_get_managed_objects
      connection.receive_signal(
        name_owner_changed(name: "org.ruby.service", old_owner: "", new_owner: owner)
      )

      expect(mirror.owner).to eq(owner)
      expect(mirror.to_h).to eq(snapshot)
    end

    it "discards a snapshot that a nested resync has superseded" do
      # while our GetManagedObjects is in flight, the owner changes;
      # the NameOwnerChanged handler fetches a fresh snapshot from the
      # new owner, and the late reply for the old call must be discarded
      new_owner = ":1.77"
      new_snapshot = { child2_path => { "org.ruby.TestChild" => { "Name" => "Child2" } } }

      connection.method_handler = lambda do |call|
        if call.destination == "org.ruby.service" && !@switched
          @switched = true
          connection.method_handler = ->(_c) { reply_message(sender: new_owner, params: [new_snapshot]) }
          connection.receive_signal(
            name_owner_changed(name: "org.ruby.service", old_owner: owner, new_owner: new_owner)
          )
          gmo_reply # the late reply from the old owner
        else
          reply_message(sender: new_owner, params: [new_snapshot])
        end
      end

      expect(mirror.owner).to eq(new_owner)
      expect(mirror.to_h).to eq(new_snapshot)
    end
  end

  describe "#detach" do
    it "removes the match rules and stops updating" do
      mirror
      mirror.detach

      expect(connection.match_rule_strings).to eq([])
      connection.receive_signal(
        interfaces_added(sender: owner, path: child2_path,
                         interfaces_and_properties: {
                           "org.ruby.TestChild" => { "Name" => "Child2" }
                         })
      )
      expect(mirror.paths).to eq([child1_path])
    end
  end

  describe "ManagedInterface" do
    it "raises KeyError for a property that is not in the view" do
      iface = mirror[child1_path]["org.ruby.TestChild"]
      expect { iface["Nosuch"] }.to raise_error(KeyError)
    end
  end
end
