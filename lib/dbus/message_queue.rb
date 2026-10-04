# frozen_string_literal: true

# This file is part of the ruby-dbus project
# Copyright (C) 2007 Arnaud Cornet and Paul van Tilburg
# Copyright (C) 2009-2014 Martin Vidner
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License, version 2.1 as published by the Free Software Foundation.
# See the file "COPYING" for the exact licensing terms.

require "fcntl"
require "socket"

module DBus
  # Encapsulates a socket so that we can {#push} and {#pop} {Message}s.
  #
  # While a received message is incomplete, this queue owns the descriptors
  # received with SCM_RIGHTS. Descriptors for a valid message are transferred
  # to the Message; descriptors of invalid or unreferenced messages are closed.
  class MessageQueue
    # The socket that is used to connect with the bus.
    attr_reader :socket

    # The buffer size for messages.
    MSG_BUF_SIZE = 4096

    # @param address [String,nil]
    # @param socket [IO,nil] an already connected socket for tests or peers
    # @param unix_fd [Boolean] whether Unix FD passing was negotiated
    def initialize(address = nil, socket: nil, unix_fd: false)
      DBus.logger.debug "MessageQueue: #{address}"
      @address = address
      @buffer = ""
      # Reduce allocations by using a single buffer for our socket
      @read_buffer = String.new(capacity: MSG_BUF_SIZE)
      @is_tcp = false
      @unix_fd = unix_fd
      @mutex = Mutex.new
      @socket = socket
      connect if @socket.nil?

      # Entries are [stream_offset, Array<IO>]
      @received_fd_groups = []
      @consumed_bytes = 0
    end

    # @param blocking [Boolean]
    #   true:  wait to return a {Message};
    #   false: may return `nil`
    # @return [Message,nil] one message or nil if unavailable
    # @raise EOFError
    # @todo failure modes
    def pop(blocking: true)
      # FIXME: this is not enough, the R/W test deadlocks on shared connections
      @mutex.synchronize do
        buffer_from_socket_nonblock
        message = message_from_buffer_nonblock
        if blocking
          # we can block
          while message.nil?
            r, _d, _d = IO.select([@socket])
            if r && r[0] == @socket
              buffer_from_socket_nonblock
              message = message_from_buffer_nonblock
            end
          end
        end
        message
      end
    end

    def push(message)
      @mutex.synchronize do
        packet = message.marshall
        if message.unix_fds.empty?
          @socket.write(packet)
        elsif !@unix_fd || @socket.closed?
          raise IOError, "UNIX_FD passing was not negotiated for this connection"
        else
        fds = message.unix_fds
        control = Socket::AncillaryData.unix_rights(*fds)
          @socket.sendmsg(packet, 0, nil, control)
        end
      end
    end
    alias << push

    private

    # Connect to the bus and initialize the connection.
    def connect
      addresses = @address.split ";"
      # connect to first one that succeeds
      addresses.find do |a|
        transport, keyvaluestring = a.split ":"
        kv_list = keyvaluestring.split ","
        kv_hash = {}
        kv_list.each do |kv|
          key, escaped_value = kv.split "="
          value = escaped_value.gsub(/%(..)/) { |_m| [Regexp.last_match(1)].pack "H2" }
          kv_hash[key] = value
        end
        case transport
        when "unix"
          connect_to_unix kv_hash
        when "tcp"
          connect_to_tcp kv_hash
        when "launchd"
          connect_to_launchd kv_hash
        else
          # ignore, report?
        end
      end
      # returns the address that worked or nil.
      # how to report failure?
    end

    # Connect to a bus over tcp and initialize the connection.
    def connect_to_tcp(params)
      host = params["host"]
      port = params["port"]
      if host && port
        begin
          # initialize the tcp socket
          @socket = TCPSocket.new(host, port.to_i)
          @socket.fcntl(Fcntl::F_SETFD, Fcntl::FD_CLOEXEC)
          init_connection
          @is_tcp = true
        rescue Exception => e
          puts "Oops:", e
          puts "Error: Could not establish connection to: #{host}:#{port}, will now exit."
          exit(1) # a little harsh
        end
      else
        # Danger, Will Robinson: the specified "path" is not usable
        puts "Error: supplied params: #{@params}, unusable! sorry."
      end
    end

    # Connect to an abstract unix bus and initialize the connection.
    def connect_to_unix(params)
      @socket = Socket.new(Socket::Constants::PF_UNIX, Socket::Constants::SOCK_STREAM, 0)
      @socket.fcntl(Fcntl::F_SETFD, Fcntl::FD_CLOEXEC)
      if !params["abstract"].nil?
        sockaddr = if HOST_END == LIL_END
                     "\1\0\0#{params["abstract"]}"
                   else
                     "\0\1\0#{params["abstract"]}"
                   end
      elsif !params["path"].nil?
        sockaddr = Socket.pack_sockaddr_un(params["path"])
      end
      @socket.connect(sockaddr)
      init_connection
    end

    def connect_to_launchd(params)
      socket_var = params["env"]
      socket = `launchctl getenv #{socket_var}`.chomp
      connect_to_unix "path" => socket
    end

    # Initialize the connection to the bus.
    def init_connection
      client = Authentication::Client.new(@socket)
      client.authenticate
      @unix_fd = client.unix_fd
    end

    public # FIXME: fix Main loop instead

    # Get and remove one message from the buffer.
    # @return [Message,nil] the message or nil if unavailable
    def message_from_buffer_nonblock
      return nil if @buffer.empty?

      ret = nil
      begin
        size, fd_count = Message.frame_info(@buffer)
        return nil if @buffer.bytesize < size

        group_offset = @consumed_bytes
        groups = fd_groups_for_frame(group_offset, size)
        received_fds = groups.flat_map(&:last)
        if fd_count != received_fds.size
          drop_invalid_message(
            InvalidPacketException,
            "UNIX_FDS header says #{fd_count}, received #{received_fds.size}"
          )
        end

        ret, consumed = Message.new.unmarshall_buffer(@buffer, unix_fds: received_fds)
        unless consumed == size
          drop_invalid_message(
            InvalidPacketException,
            "Declared frame size #{size} differs from parsed size #{consumed}"
          )
        end

        @buffer.slice!(0, size)
        @consumed_bytes += size
        @received_fd_groups -= groups
      rescue IncompleteBufferException
        # fall through, let ret remain nil
      rescue InvalidPacketException
        drop_current_invalid_message
      end
      ret
    end

    # Fill (append) the buffer from data that might be available on the
    # socket.
    # @return [void]
    # @raise EOFError
    def buffer_from_socket_nonblock
      if @socket.is_a?(Socket) && @socket.local_address.afamily == Socket::AF_UNIX
        data, _addr, _flags, *controls = recvmsg_nonblock_with_rights
        case data
        when ""
          return
        when :wait_readable
          return
        when nil
          close_received_fds
          raise EOFError
        end

        @buffer += data
        group_start = @consumed_bytes + @buffer.bytesize - data.bytesize
        fds = received_fds_from_controls(controls)
        @received_fd_groups << [group_start, fds]
      else
        @buffer += @socket.read_nonblock(MSG_BUF_SIZE, @read_buffer)
      end
    rescue EOFError
      raise # the caller expects it
    rescue Errno::EAGAIN
      # fine, would block
    rescue Exception => e
      puts "Oops:", e
      raise if @is_tcp # why?

      puts "WARNING: read_nonblock failed, falling back to .recv"
      @buffer += @socket.recv(MSG_BUF_SIZE)
    end

    def recvmsg_nonblock_with_rights
      @socket.recvmsg_nonblock(MSG_BUF_SIZE, exception: false, scm_rights: true)
    rescue ArgumentError
      @socket.recvmsg_nonblock(MSG_BUF_SIZE, exception: false)
    end

    def received_fds_from_controls(controls)
      controls.select do |control|
        control.is_a?(Socket::AncillaryData) &&
          control.cmsg_is?(:SOCKET, :SCM_RIGHTS)
      end.flat_map(&:unix_rights).each do |fd|
        fd.fcntl(Fcntl::F_SETFD, Fcntl::FD_CLOEXEC)
      end
    end

    def close_received_fds
      @received_fd_groups.each do |_offset, fds|
        fds.each(&:close)
      end
      @received_fd_groups.clear
    end

    def fd_groups_for_frame(offset, size)
      @received_fd_groups.select do |fd_offset, _|
        fd_offset >= offset && fd_offset < offset + size
      end
    end

    def drop_current_invalid_message
      close_received_fds
      @socket.close unless @socket.closed?
      raise
    end

    def drop_invalid_message(exception_class, message)
      close_received_fds
      @socket.close unless @socket.closed?
      raise exception_class, message
    end
  end
end
