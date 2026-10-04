#!/usr/bin/env rspec
# frozen_string_literal: true

require_relative "spec_helper"
require "dbus"

describe DBus::MessageQueue do
  around do |example|
    Dir.mktmpdir do |dir|
      @tmpdir = dir
      example.call
    end
  end

  let(:sockets) { Socket.pair(:UNIX, :STREAM) }
  let(:left) { sockets.first }
  let(:right) { sockets.last }
  let(:sender) { described_class.new(nil, socket: left, unix_fd: true) }
  let(:receiver) { described_class.new(nil, socket: right, unix_fd: true) }

  after do
    [left, right].each { |socket| socket.close unless socket.closed? }
  end

  def temp_file(name, content)
    path = File.join(@tmpdir, name)
    File.write(path, content)
    File.open(path)
  end

  def fd_message(*files)
    message = DBus::Message.new(DBus::Message::METHOD_CALL)
    message.path = "/test"
    message.interface = "org.example.Test"
    message.member = "Pass"
    files.each { |file| message.add_param(DBus::Type::UNIX_FD, file) }
    message
  end

  it "makes a sent message refer to but not own the caller's descriptor" do
    file = temp_file("ownership", "owner")
    message = fd_message(file)

    sender.push(message)

    expect(message.unix_fds.map(&:fileno)).to eq([file.fileno])
    expect(message.unix_fds.first.autoclose?).to eq(false)
    expect(message).to_not be_owns_unix_fds
    expect { message.close_unix_fds }.to_not change(file, :closed?)
    file.close
  end

  it "passes UNIX_FDs and maps repeated h indexes to one descriptor" do
    file = temp_file("one", "fd contents")
    message = fd_message(file, file)

    sender.push(message)
    received = receiver.pop
    first_fd = received.params[0]
    second_fd = received.params[1]

    expect(first_fd).to be_an(IO)
    expect(first_fd.fileno).to_not eq(file.fileno)
    expect(second_fd.fileno).to eq(first_fd.fileno)
    expect(received.unix_fds).to eq([first_fd])
    expect(first_fd.read).to eq("fd contents")

    first_fd.close
    file.close
  end

  it "passes a UNIX_FD nested in a variant" do
    file = temp_file("variant", "variant fd")
    message = DBus::Message.new(DBus::Message::METHOD_CALL)
    message.path = "/test"
    message.interface = "org.example.Test"
    message.member = "PassVariant"
    message.add_param("v", DBus::Data::Variant.new(file, member_type: "h"))

    sender.push(message)
    received = receiver.pop
    received_fd = received.params[0]

    expect(received_fd).to be_an(IO)
    expect(received_fd.read).to eq("variant fd")

    received_fd.close
    file.close
  end

  it "accepts a message carrying FDs that arrives in multiple chunks" do
    file = temp_file("chunked", "chunked fd")
    packet = fd_message(file).marshall

    packet.bytesize.times do |index|
      if index.zero?
        left.sendmsg(packet[index], 0, nil, Socket::AncillaryData.unix_rights(file))
      else
        left.write(packet[index])
      end
      expect(receiver.pop(blocking: false)).to be_nil if index < packet.bytesize - 1
    end

    received = receiver.pop(blocking: false)
    received_fd = received.params[0]
    expect(received_fd.read).to eq("chunked fd")
    received_fd.close
    file.close
  end

  it "closes received FDs when an h index is invalid" do
    file = temp_file("invalid-index", "bad index")
    packet = fd_message(file).marshall
    packet[-4, 4] = [1].pack("L<")

    expect do
      left.sendmsg(packet, 0, nil, Socket::AncillaryData.unix_rights(file))
      receiver.pop
    end.to raise_error(DBus::InvalidPacketException, /UNIX_FD index 1 is out of range/)

    expect(right).to be_closed
    expect(file).to_not be_closed
    file.close
  end

  it "closes received FDs when UNIX_FDS count disagrees with SCM_RIGHTS" do
    file = temp_file("wrong-count", "wrong count")
    other_file = temp_file("extra", "extra fd")
    packet = fd_message(file).marshall

    expect do
      left.sendmsg(packet, 0, nil, Socket::AncillaryData.unix_rights(file, other_file))
      receiver.pop
    end.to raise_error(DBus::InvalidPacketException, /UNIX_FDS header says 1, received 2/)

    expect(right).to be_closed
    expect(receiver.instance_variable_get(:@received_fd_groups)).to be_empty
    file.close
    other_file.close
  end

  it "closes descriptors that arrive but are not referenced by an h value" do
    referenced = temp_file("referenced", "used")
    unreferenced = temp_file("unreferenced", "unused")
    packet = fd_message(referenced, unreferenced).marshall
    packet[-4, 4] = [0].pack("L<")

    sender.socket.sendmsg(
      packet, 0, nil,
      Socket::AncillaryData.unix_rights(referenced, unreferenced)
    )
    received = receiver.pop
    received_fd = received.params[0]

    expect(received.unix_fds).to eq([received_fd])
    expect(receiver.instance_variable_get(:@received_fd_groups)).to be_empty

    received_fd.close
    referenced.close
    unreferenced.close
  end

  it "refuses to send FDs unless UNIX_FD passing was negotiated" do
    no_negotiation = described_class.new(nil, socket: right)
    file = temp_file("no-negotiation", "nope")
    message = fd_message(file)

    expect { no_negotiation.push(message) }.to raise_error(IOError, /not negotiated/)
    expect(file).to_not be_closed
    file.close
  end
end
