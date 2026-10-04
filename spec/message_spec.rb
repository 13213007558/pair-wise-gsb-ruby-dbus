#!/usr/bin/env rspec
# frozen_string_literal: true

require_relative "spec_helper"
require "dbus"

# Pedantic full coverage test.
# The happy paths are covered via calling classes
describe DBus::Message do
  describe "UNIX_FDs" do
    it "resolves h indexes and closes descriptors that are not referenced" do
      referenced = File.open(File::NULL)
      unreferenced = File.open(File::NULL)
      message = described_class.new(described_class::METHOD_CALL)
      message.path = "/test"
      message.interface = "org.example.Test"
      message.member = "Pass"
      message.add_param("h", referenced)
      packet = message.marshall
      packet[92, 4] = [2].pack("L<")

      received = described_class.new.unmarshall_buffer(
        packet, unix_fds: [referenced, unreferenced]
      ).first

      expect(received.params).to eq([referenced])
      expect(received.unix_fds).to eq([referenced])
      expect(unreferenced).to be_closed
      referenced.close
    end

    it "resolves h indexes, rejects invalid indexes, and closes unreferenced FDs" do
      referenced = File.open(File::NULL)
      unreferenced = File.open(File::NULL)
      message = described_class.new(described_class::METHOD_CALL)
      message.path = "/test"
      message.interface = "org.example.Test"
      message.member = "Pass"
      message.add_param("h", referenced)
      packet = message.marshall
      packet[-4, 4] = [2].pack("L<")
      packet[92, 4] = [2].pack("L<")

      received = described_class.new
      expect do
        received.unmarshall_buffer(packet, unix_fds: [referenced, unreferenced])
      end.to raise_error(DBus::InvalidPacketException, /UNIX_FD index 2 is out of range/)

      expect(referenced).to be_closed
      expect(unreferenced).to be_closed
    end
  end

  describe "#marshall" do
    it "raises when the object path is /org/freedesktop/DBus/Local" do
      m = DBus::Message.new(DBus::Message::SIGNAL)
      # the path is valid, it just must not be sent
      m.path = DBus::ObjectPath.new("/org/freedesktop/DBus/Local")
      m.interface = "org.example.spam"
      m.member = "Spam"

      expect { m.marshall }.to raise_error(RuntimeError, /Cannot send a message with the reserved path/)
    end
  end
end
