#!/usr/bin/env rspec
# frozen_string_literal: true

require_relative "spec_helper"
require "dbus"

describe DBus::BusNameBroker do
  let(:broker) { described_class.new }
  let(:connections) { Array.new(3) { broker.connect } }
  let(:name) { "org.rubygems.ruby_dbus.NameBrokerTest" }

  def drain(*connections)
    signals = connections.each_with_object({}) do |connection, result|
      result[connection.unique_name] = []
    end
    connections.each do |connection|
      connection.dispatch_message_queue
      queue = signals[connection.unique_name]
      connection.singleton_class.define_method(:signals) { queue }
      connection.on_name_acquired { |signaled_name| queue << ["NameAcquired", signaled_name] }
      connection.on_name_lost { |signaled_name| queue << ["NameLost", signaled_name] }
      connection.add_match(DBus::MatchRule.new.tap do |rule|
        rule.type = "signal"
        rule.interface = "org.freedesktop.DBus"
        rule.member = "NameOwnerChanged"
      end) do |message|
        queue << ["NameOwnerChanged", *message.params]
      end
      connection.dispatch_message_queue
    end
    signals
  end

  def request_name(connection, *args, **kwargs)
    result = connection.proxy.RequestName(name, *args, **kwargs).first
    connections.each(&:dispatch_message_queue)
    result
  end

  def release_name(connection)
    result = connection.proxy.ReleaseName(name).first
    connections.each(&:dispatch_message_queue)
    result
  end

  it "assigns unique names with Hello" do
    expect(connections.map(&:unique_name)).to eq [":1.0", ":1.1", ":1.2"]
  end

  it "supports all ReleaseName result codes" do
    a, b, c = connections
    signals = drain(a, b, c)

    expect(a.proxy.ReleaseName(name).first)
      .to eq DBus::BusConnection::RELEASE_NAME_REPLY_NON_EXISTENT

    expect(request_name(a, 0)).to eq DBus::Connection::REQUEST_NAME_REPLY_PRIMARY_OWNER
    expect(c.proxy.ReleaseName(name).first)
      .to eq DBus::BusConnection::RELEASE_NAME_REPLY_NOT_OWNER

    expect(request_name(b, 0)).to eq DBus::Connection::REQUEST_NAME_REPLY_IN_QUEUE
    expect(b.proxy.ListQueuedOwners(name).first).to eq [a.unique_name, b.unique_name]
    signals.each_value(&:clear)

    expect(b.proxy.ReleaseName(name).first)
      .to eq DBus::BusConnection::RELEASE_NAME_REPLY_RELEASED
    connections.each(&:dispatch_message_queue)
    expect(a.proxy.ListQueuedOwners(name).first).to eq [a.unique_name]
    expect(signals.values).to all(eq([]))

    expect(a.proxy.ReleaseName(name).first)
      .to eq DBus::BusConnection::RELEASE_NAME_REPLY_RELEASED
    connections.each(&:dispatch_message_queue)
    expect(a.proxy.ListQueuedOwners(name).first).to eq []
    expect(signals[a.unique_name]).to eq [
      ["NameOwnerChanged", name, a.unique_name, ""],
      ["NameLost", name]
    ]
  end

  it "implements all RequestName flag combinations" do
    a, b = connections
    drain(a, b)

    flags = 0.upto(7).to_a
    allow_replace = flags.select { |flag| (flag & DBus::Connection::NAME_FLAG_ALLOW_REPLACEMENT) != 0 }
    replace = flags.select { |flag| (flag & DBus::Connection::NAME_FLAG_REPLACE_EXISTING) != 0 }
    do_not_queue = flags.select { |flag| (flag & DBus::Connection::NAME_FLAG_DO_NOT_QUEUE) != 0 }

    allow_replace.each do |owner_flags|
      expect(request_name(a, owner_flags)).to eq DBus::Connection::REQUEST_NAME_REPLY_PRIMARY_OWNER
      old_owner_queues = (owner_flags & DBus::Connection::NAME_FLAG_DO_NOT_QUEUE).zero?

      flags.each do |request_flags|
        a.signals.clear
        b.signals.clear

        if replace.include?(request_flags)
          expected = DBus::Connection::REQUEST_NAME_REPLY_PRIMARY_OWNER
        elsif do_not_queue.include?(request_flags)
          expected = DBus::Connection::REQUEST_NAME_REPLY_EXISTS
        else
          expected = DBus::Connection::REQUEST_NAME_REPLY_IN_QUEUE
        end

        expect(request_name(b, request_flags)).to eq(expected)
        expect(a.proxy.ListQueuedOwners(name).first).to eq(
          if expected == DBus::Connection::REQUEST_NAME_REPLY_PRIMARY_OWNER
            old_owner_queues ? [b.unique_name, a.unique_name] : [b.unique_name]
          elsif expected == DBus::Connection::REQUEST_NAME_REPLY_IN_QUEUE
            [a.unique_name, b.unique_name]
          else
            [a.unique_name]
          end
        )
        expect(a.signals).to eq(
          if expected == DBus::Connection::REQUEST_NAME_REPLY_PRIMARY_OWNER
            [
              ["NameOwnerChanged", name, a.unique_name, b.unique_name],
              ["NameLost", name]
            ]
          else
            []
          end
        )
        expect(b.signals).to eq(
          case expected
          when DBus::Connection::REQUEST_NAME_REPLY_PRIMARY_OWNER
            [
              ["NameOwnerChanged", name, a.unique_name, b.unique_name],
              ["NameAcquired", name]
            ]
          when DBus::Connection::REQUEST_NAME_REPLY_IN_QUEUE
            [["NameLost", name]]
          else
            []
          end
        )

        if expected == DBus::Connection::REQUEST_NAME_REPLY_PRIMARY_OWNER
          a.signals.clear
          b.signals.clear
          expect(request_name(b, request_flags | DBus::Connection::NAME_FLAG_DO_NOT_QUEUE))
            .to eq DBus::Connection::REQUEST_NAME_REPLY_ALREADY_OWNER
          expect(b.signals).to eq []
          expect(release_name(b)).to eq DBus::BusConnection::RELEASE_NAME_REPLY_RELEASED
        elsif expected == DBus::Connection::REQUEST_NAME_REPLY_IN_QUEUE
          expect(release_name(b)).to eq DBus::BusConnection::RELEASE_NAME_REPLY_RELEASED
        else
          expect(release_name(b)).to eq DBus::BusConnection::RELEASE_NAME_REPLY_NOT_OWNER
        end
        connections.each(&:dispatch_message_queue)
        if expected == DBus::Connection::REQUEST_NAME_REPLY_PRIMARY_OWNER
          expect(a.proxy.ListQueuedOwners(name).first).to eq(
            old_owner_queues ? [a.unique_name] : []
          )
          unless old_owner_queues
            expect(request_name(a, owner_flags)).to eq DBus::Connection::REQUEST_NAME_REPLY_PRIMARY_OWNER
          end
        end
      end

      expect(release_name(a)).to eq DBus::BusConnection::RELEASE_NAME_REPLY_RELEASED
    end

    expect(request_name(a, 4)).to eq DBus::Connection::REQUEST_NAME_REPLY_PRIMARY_OWNER
    flags.each do |request_flags|
      a.signals.clear
      b.signals.clear
      expected = if do_not_queue.include?(request_flags)
                   DBus::Connection::REQUEST_NAME_REPLY_EXISTS
                 else
                   DBus::Connection::REQUEST_NAME_REPLY_IN_QUEUE
                 end

      expect(request_name(b, request_flags)).to eq(expected)
      expect(b.signals).to eq(
        if expected == DBus::Connection::REQUEST_NAME_REPLY_IN_QUEUE
          [["NameLost", name]]
        else
          []
        end
      )
      expected_release = if expected == DBus::Connection::REQUEST_NAME_REPLY_IN_QUEUE
                           DBus::BusConnection::RELEASE_NAME_REPLY_RELEASED
                         else
                           DBus::BusConnection::RELEASE_NAME_REPLY_NOT_OWNER
                         end
      expect(release_name(b)).to eq expected_release
    end
  end

  it "hands queued names over on release and disconnect" do
    a, b, c = connections
    signals = drain(a, b, c)

    expect(request_name(a, 0)).to eq DBus::Connection::REQUEST_NAME_REPLY_PRIMARY_OWNER
    expect(request_name(b, 0)).to eq DBus::Connection::REQUEST_NAME_REPLY_IN_QUEUE
    expect(request_name(c, 0)).to eq DBus::Connection::REQUEST_NAME_REPLY_IN_QUEUE
    expect(a.proxy.ListQueuedOwners(name).first)
      .to eq [a.unique_name, b.unique_name, c.unique_name]

    signals.each_value(&:clear)
    expect(release_name(a)).to eq DBus::BusConnection::RELEASE_NAME_REPLY_RELEASED
    expect(b.proxy.ListQueuedOwners(name).first)
      .to eq [b.unique_name, c.unique_name]
    expect(signals[a.unique_name]).to eq [
      ["NameOwnerChanged", name, a.unique_name, b.unique_name],
      ["NameLost", name]
    ]
    expect(signals[b.unique_name]).to eq [
      ["NameOwnerChanged", name, a.unique_name, b.unique_name],
      ["NameAcquired", name]
    ]
    expect(signals[c.unique_name]).to eq [
      ["NameOwnerChanged", name, a.unique_name, b.unique_name]
    ]

    signals.each_value(&:clear)
    b.close
    connections.each(&:dispatch_message_queue)
    expect(c.proxy.ListQueuedOwners(name).first).to eq [c.unique_name]
    expect(signals[a.unique_name]).to eq [
      ["NameOwnerChanged", name, b.unique_name, c.unique_name],
      ["NameOwnerChanged", b.unique_name, b.unique_name, ""]
    ]
    expect(signals[c.unique_name]).to eq [
      ["NameOwnerChanged", name, b.unique_name, c.unique_name],
      ["NameAcquired", name],
      ["NameOwnerChanged", b.unique_name, b.unique_name, ""]
    ]
  end

  it "inserts a replaced owner before existing waiters" do
    a, b, c = connections
    signals = drain(a, b, c)

    expect(request_name(a, DBus::Connection::NAME_FLAG_ALLOW_REPLACEMENT))
      .to eq DBus::Connection::REQUEST_NAME_REPLY_PRIMARY_OWNER
    expect(request_name(b, 0)).to eq DBus::Connection::REQUEST_NAME_REPLY_IN_QUEUE
    expect(request_name(c, DBus::Connection::NAME_FLAG_REPLACE_EXISTING))
      .to eq DBus::Connection::REQUEST_NAME_REPLY_PRIMARY_OWNER
    expect(a.proxy.ListQueuedOwners(name).first)
      .to eq [c.unique_name, a.unique_name, b.unique_name]

    signals.each_value(&:clear)
    expect(release_name(c)).to eq DBus::BusConnection::RELEASE_NAME_REPLY_RELEASED
    expect(a.proxy.ListQueuedOwners(name).first)
      .to eq [a.unique_name, b.unique_name]
    expect(signals[a.unique_name]).to eq [
      ["NameOwnerChanged", name, c.unique_name, a.unique_name],
      ["NameAcquired", name]
    ]
    expect(signals[b.unique_name]).to eq [
      ["NameOwnerChanged", name, c.unique_name, a.unique_name]
    ]
    expect(signals[c.unique_name]).to eq [
      ["NameOwnerChanged", name, c.unique_name, a.unique_name],
      ["NameLost", name]
    ]
  end
end
