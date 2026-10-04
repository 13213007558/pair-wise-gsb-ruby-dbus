#!/usr/bin/env rspec
# frozen_string_literal: true

require_relative "spec_helper"
require "dbus"
require "tempfile"

describe DBus::MessageQueue, "UNIX_FD" do
  let(:transport) { Socket.pair(:UNIX, :STREAM) }
  let(:local) { transport.first }
  let(:peer) { transport.last }
  let(:queue) { described_class.new(local, unix_fd: true) }

  after do
    [local, peer].each { |s| s.close unless s.closed? }
  end

  def message_with_io(io)
    message = DBus::Message.new(DBus::Message::METHOD_CALL)
    message.path = "/org/example"
    message.interface = "org.example.Files"
    message.member = "Open"
    message.add_param("h", io)
    message
  end

  def force_gc
    GC.start
    sleep 0.01
    GC.start
  end

  it "sends and receives an FD with the h index" do
    file = Tempfile.create("ruby-dbus-fd")
    file.write("fd contents")
    file.rewind

    message = message_with_io(file)
    queue.push(message)
    data, _addr, _flags, control = peer.recvmsg(scm_rights: true)
    sent_fd = control.unix_rights.first
    expect(sent_fd.fileno).to_not eq(file.fileno)
    expect(sent_fd.read).to eq("fd contents")
    sent_fd.rewind

    reply = DBus::Message.new(DBus::Message::METHOD_RETURN)
    reply.reply_serial = message.serial
    reply.add_param("h", sent_fd)
    packet, fds = reply.marshall_with_fds
    control = Socket::AncillaryData.unix_rights(*fds)
    peer.sendmsg(packet, 0, nil, control)
    sent_fd.close

    received = queue.pop(blocking: false)
    received_fd = received.params.first
    expect(received_fd).to be_an(IO)
    expect(received.unix_fds).to contain_exactly(received_fd)
    expect(received_fd.read).to eq("fd contents")
    expect(file).to_not be_closed
    received.close_unix_fds
  ensure
    file&.close
    File.unlink(file.path) if file
  end

  it "does not close the caller FD after sending" do
    file = Tempfile.create("ruby-dbus-fd")
    queue.push(message_with_io(file))
    peer.recvmsg(scm_rights: true).last.unix_rights.each(&:close)

    expect(file).to_not be_closed
  ensure
    file&.close
    File.unlink(file.path) if file
  end

  it "assigns stable indexes for repeated and multiple FDs" do
    first = Tempfile.create("ruby-dbus-fd")
    second = Tempfile.create("ruby-dbus-fd")
    message = DBus::Message.new(DBus::Message::METHOD_CALL)
    message.path = "/org/example"
    message.interface = "org.example.Files"
    message.member = "OpenMany"
    message.add_param("h", first)
    message.add_param("h", second)
    message.add_param("h", first)

    packet, fds = message.marshall_with_fds
    expect(fds.map(&:fileno)).to eq([first.fileno, second.fileno])

    control = Socket::AncillaryData.unix_rights(first, second)
    peer.sendmsg(packet, 0, nil, control)
    received = queue.pop(blocking: false)
    expect(received.params).to eq([received.unix_fds[0], received.unix_fds[1], received.unix_fds[0]])
    received.close_unix_fds
  ensure
    first&.close
    second&.close
    [first, second].compact.each { |file| File.unlink(file.path) }
  end

  it "rejects sending FDs when they were not negotiated" do
    disabled_queue = described_class.new(local, unix_fd: false)
    file = Tempfile.create("ruby-dbus-fd")

    expect { disabled_queue.push(message_with_io(file)) }
      .to raise_error(DBus::InvalidPacketException, /not negotiated/)
    expect(IO.select([peer], nil, nil, 0)).to be_nil
  ensure
    file&.close
    File.unlink(file.path) if file
  end

  it "rejects a raw h index at the message API" do
    message = DBus::Message.new(DBus::Message::METHOD_CALL)
    expect { message.add_param("h", 3) }
      .to raise_error(ArgumentError, /UNIX_FD parameter must be an IO/)
  end

  it "keeps an FD pending until the rest of a split message arrives" do
    file = Tempfile.create("ruby-dbus-fd")
    file.write("file")
    file.rewind
    packet, _fds = message_with_io(file).marshall_with_fds
    control = Socket::AncillaryData.unix_rights(file)
    peer.sendmsg(packet.byteslice(0, packet.bytesize - 1), 0, nil, control)

    expect(queue.pop(blocking: false)).to be_nil
    force_gc
    pending_fds = queue.instance_variable_get(:@unix_fd_chunks).flat_map { |chunk| chunk[:fds] }
    expect(pending_fds).to contain_exactly(an_instance_of(IO))

    peer.write(packet.byteslice(-1))
    received = queue.pop(blocking: false)
    expect(received.params.first.read).to eq("file")
    received.close_unix_fds
  ensure
    file&.close
    File.unlink(file.path) if file
  end

  it "closes a pending FD if a half message reaches EOF" do
    file = Tempfile.create("ruby-dbus-fd")
    packet, _fds = message_with_io(file).marshall_with_fds
    control = Socket::AncillaryData.unix_rights(file)
    peer.sendmsg(packet.byteslice(0, packet.bytesize - 1), 0, nil, control)
    queue.pop(blocking: false)
    pending_fds = queue.instance_variable_get(:@unix_fd_chunks).flat_map { |chunk| chunk[:fds] }
    expect(pending_fds).to contain_exactly(an_instance_of(IO))
    peer.close

    expect { queue.pop(blocking: false) }.to raise_error(EOFError)
    expect(pending_fds.first).to be_closed
    expect(IO.select([local], nil, nil, 0)).to eq([[local], [], []])
  ensure
    file&.close
    File.unlink(file.path) if file
  end

  it "closes a pending FD when the queue is explicitly closed" do
    passed_local, passed_peer = Socket.pair(:UNIX, :STREAM)
    packet = message_with_io(passed_local).marshall_with_fds.first
    control = Socket::AncillaryData.unix_rights(passed_local)
    peer.sendmsg(packet.byteslice(0, packet.bytesize - 1), 0, nil, control)
    passed_local.close
    queue.pop(blocking: false)

    queue.close
    expect(passed_peer.recv(1)).to be_nil
  ensure
    passed_local.close unless passed_local.closed? if passed_local
    passed_peer.close unless passed_peer.closed? if passed_peer
  end

  it "closes the received FD when an index is invalid" do
    passed_local, passed_peer = Socket.pair(:UNIX, :STREAM)
    message = message_with_io(passed_local)
    packet, fds = message.marshall_with_fds
    packet.setbyte(packet.bytesize - 4, 1)
    control = Socket::AncillaryData.unix_rights(passed_local)
    peer.sendmsg(packet, 0, nil, control)
    passed_local.close

    expect { queue.pop(blocking: false) }
      .to raise_error(DBus::InvalidPacketException, /index 1 is out of range/)
    expect(queue).to have_attributes(unix_fd: true)
    expect(passed_peer.recv(1)).to be_nil
  ensure
    passed_local.close unless passed_local.closed?
    passed_peer.close unless passed_peer.closed?
  end

  it "closes an unexpected received FD when no UNIX_FDS count is declared" do
    passed_local, passed_peer = Socket.pair(:UNIX, :STREAM)
    message = DBus::Message.new(DBus::Message::SIGNAL)
    message.path = "/org/example"
    message.interface = "org.example.Files"
    message.member = "Unexpected"
    packet = message.marshall
    control = Socket::AncillaryData.unix_rights(passed_local)
    peer.sendmsg(packet, 0, nil, control)
    passed_local.close

    expect { queue.pop(blocking: false) }
      .to raise_error(DBus::InvalidPacketException, /without UNIX_FDS header/)
    expect(passed_peer.recv(1)).to be_nil
    passed_peer.close
  ensure
    passed_local.close unless passed_local.closed?
    passed_peer.close unless passed_peer.closed?
  end

  it "closes received FDs when a delivered message is dropped" do
    file = Tempfile.create("ruby-dbus-fd")
    queue.push(message_with_io(file))
    _data, _addr, _flags, control = peer.recvmsg(scm_rights: true)
    reply_packet = message_with_io(file).marshall_with_fds.first
    peer.sendmsg(reply_packet, 0, nil, control)

    received = queue.pop(blocking: false)
    received_fd = received.params.first
    expect(received_fd).to_not be_closed
    received = nil
    force_gc
    expect(received_fd).to be_closed
    expect(file).to_not be_closed
  ensure
    file&.close
    File.unlink(file.path) if file
  end

  it "transfers one FD without keeping ownership of it" do
    first_local, first_peer = Socket.pair(:UNIX, :STREAM)
    second_local, second_peer = Socket.pair(:UNIX, :STREAM)
    packet = message_with_io(first_local).tap { |m| m.add_param("h", second_local) }.marshall_with_fds.first
    control = Socket::AncillaryData.unix_rights(first_local, second_local)
    peer.sendmsg(packet, 0, nil, control)
    first_local.close
    second_local.close

    received = queue.pop(blocking: false)
    kept = received.take_unix_fd(0)
    received = nil
    force_gc

    kept_peer = [first_peer, second_peer].find do |sock|
      begin
        sock.read_nonblock(1)
      rescue IO::WaitReadable
        sock
      end
    end
    expect(kept_peer).not_to be_nil
    closed_peer = [first_peer, second_peer] - [kept_peer]
    expect(closed_peer.first.recv(1)).to be_nil
    kept.close
  ensure
    first_local.close unless first_local.closed? if first_local
    second_local.close unless second_local.closed? if second_local
    first_peer.close unless first_peer.closed? if first_peer
    second_peer.close unless second_peer.closed? if second_peer
  end
end
