#!/usr/bin/env rspec
# frozen_string_literal: true

require_relative "spec_helper"
require "dbus"

describe DBus::Connection do
  let(:connection) do
    connection = described_class.allocate
    connection.instance_variable_set(:@signal_matchrules, {})
    connection
  end

  it "allows multiple handlers for the same match rule" do
    rule = DBus::MatchRule.new.from_s("type='signal',member='Changed'")
    received = []
    connection.add_match(rule) { received << :first }
    connection.add_match(rule) { received << :second }

    message = DBus::Message.new(DBus::Message::SIGNAL)
    message.member = "Changed"
    connection.process(message)

    expect(received).to eq(%i[first second])
  end

  it "reports whether removing a match rule found one" do
    rule = DBus::MatchRule.new.from_s("type='signal',member='Changed'")

    expect(connection.remove_match(rule)).to be(false)
    connection.add_match(rule) { nil }
    expect(connection.remove_match(rule)).to be(true)
  end
end
