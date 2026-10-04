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
  class MessageQueue
    # The socket that is used to connect with the bus.
    attr_reader :socket

    # @return [Boolean] whether UNIX_FD passing was negotiated
    attr_reader :unix_fd

    # The buffer size for messages.
    MSG_BUF_SIZE = 4096

    def initialize(address_or_socket, unix_fd: nil)
      DBus.logger.debug "MessageQueue: #{address_or_socket}"
      @buffer = ""
      # Reduce allocations by using a single buffer for our socket
      @read_buffer = String.new(capacity: MSG_BUF_SIZE)
      @is_tcp = false
      @unix_fd = false
      @unix_fd_chunks = []
      @stream_position = 0
      @mutex = Mutex.new
      if address_or_socket.is_a?(::IO)
        @socket = address_or_socket
        @unix_fd = unix_fd == true
      else
        @address = address_or_socket
        connect
      end
      finalizer = self.class.send(:finalize_queue, @socket, @unix_fd_chunks)
      ObjectSpace.define_finalizer(self, finalizer)
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
        if message.unix_fds.any? && !@unix_fd
          raise InvalidPacketException, "UNIX_FD passing was not negotiated"
        end

        packet, file_descriptors = message.marshall_with_fds
        if file_descriptors.any?
          control = Socket::AncillaryData.unix_rights(*file_descriptors)
          @socket.sendmsg(packet, 0, nil, control)
        else
          @socket.write(packet)
        end
      end
    end
    alias << push

    def close
      @mutex.synchronize { close_locked }
    end

    private

    def close_locked
      ObjectSpace.undefine_finalizer(self)
      close_pending_unix_fds
      @socket.close unless @socket.closed?
    end

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
      @unix_fd = client.authenticate
    end

    public # FIXME: fix Main loop instead

    # Get and remove one message from the buffer. The queue owns received FDs
    # while the message is incomplete; a parsed message owns them afterwards.
    # Malformed messages and EOF close all pending FDs.
    # @return [Message,nil] the message or nil if unavailable
    def message_from_buffer_nonblock
      return nil if @buffer.empty?

      ret = nil
      file_descriptors = nil
      begin
        size = Message.frame_size(@buffer)
        return nil if size.nil?

        file_descriptors = unix_fds_for_message(size)
        ret, consumed = Message.new.unmarshall_buffer(@buffer, file_descriptors)
        unless consumed == size
          raise InvalidPacketException,
                "consumed #{consumed} bytes from a #{size}-byte message"
        end
        @buffer.slice!(0, size)
        @stream_position += size
      rescue IncompleteBufferException
        # fall through, let ret remain nil
      rescue Exception
        close_unix_fds(file_descriptors) unless file_descriptors.nil?
        close_pending_unix_fds
        @buffer = ""
        @stream_position = 0
        raise
      end
      ret
    end

    # Fill (append) the buffer from data that might be available on the
    # socket.
    # @return [void]
    # @raise EOFError
    def buffer_from_socket_nonblock
      if @unix_fd
        data, _address, _flags, control = @socket.recvmsg_nonblock(MSG_BUF_SIZE, scm_rights: true)
        file_descriptors = if control && control.cmsg_is?(:SOCKET, :RIGHTS)
                              control.unix_rights
                            else
                              []
                            end
        raise EOFError if (data.nil? || data.empty?) && file_descriptors.empty?

        data = "" if data.nil?
        @unix_fd_chunks << {
          start: @stream_position + @buffer.bytesize,
          fds: file_descriptors
        }
        @buffer += data
      else
        @buffer += @socket.read_nonblock(MSG_BUF_SIZE, @read_buffer)
      end
    rescue EOFError
      close_pending_unix_fds
      raise # the caller expects it
    rescue Errno::EAGAIN, IO::WaitReadable
      # fine, would block
    rescue Exception => e
      raise if @unix_fd || @is_tcp

      puts "Oops:", e
      puts "WARNING: read_nonblock failed, falling back to .recv"
      @buffer += @socket.recv(MSG_BUF_SIZE)
    end

    def unix_fds_for_message(size)
      message_end = @stream_position + size
      file_descriptors = []
      @unix_fd_chunks.reject! do |chunk|
        next false unless chunk[:start] < message_end

        file_descriptors.concat(chunk[:fds])
        true
      end
      file_descriptors
    end

    def close_pending_unix_fds
      file_descriptors = @unix_fd_chunks.flat_map { |chunk| chunk[:fds] }
      file_descriptors.each { |io| io.close unless io.closed? }
      @unix_fd_chunks.clear
    end

    def close_unix_fds(file_descriptors)
      file_descriptors.each { |io| io.close unless io.closed? }
    end

    def self.finalize_queue(socket, chunks)
      proc do
        chunks.flat_map { |chunk| chunk[:fds] }.each { |io| io.close unless io.closed? }
        next if socket.nil?

        socket.close unless socket.closed?
      end
    end
    private_class_method :finalize_queue
  end
end
