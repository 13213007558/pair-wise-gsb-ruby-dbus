# frozen_string_literal: true

# dbus.rb - Module containing the low-level D-Bus implementation
#
# This file is part of the ruby-dbus project
# Copyright (C) 2007 Arnaud Cornet and Paul van Tilburg
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License, version 2.1 as published by the Free Software Foundation.
# See the file "COPYING" for the exact licensing terms.

require_relative "raw_message"

# = D-Bus main module
#
# Module containing all the D-Bus modules and classes.
module DBus
  # = D-Bus message class
  #
  # Class that holds any type of message that travels over the bus.
  class Message
    # The serial number of the message.
    @@serial = 1
    # Mutex that protects updates on the serial number.
    @@serial_mutex = Mutex.new
    # Type of a message (by specification).
    MESSAGE_SIGNATURE = "yyyyuua(yv)"

    # FIXME: following message type constants should be under Message::Type IMO
    # well, yeah sure
    #
    # Invalid message type.
    INVALID = 0
    # Method call message type.
    METHOD_CALL = 1
    # Method call return value message type.
    METHOD_RETURN = 2
    # Error message type.
    ERROR = 3
    # Signal message type.
    SIGNAL = 4

    # Names used by signal match rules
    TYPE_NAMES = ["invalid", "method_call", "method_return", "error", "signal"].freeze

    # Message flag signyfing that no reply is expected.
    NO_REPLY_EXPECTED = 0x1
    # Message flag signifying that no automatic start is required/must be
    # performed.
    NO_AUTO_START = 0x2

    # The type of the message.
    attr_reader :message_type
    # The path of the object instance the message must be sent to/is sent from.
    attr_accessor :path
    # The interface of the object that must be used/was used.
    attr_accessor :interface
    # The interface member (method/signal name) of the object that must be
    # used/was used.
    attr_accessor :member
    # The name of the error (in case of an error message type).
    attr_accessor :error_name
    # The destination connection of the object that must be used/was used.
    attr_accessor :destination
    # The sender of the message.
    attr_accessor :sender
    # The signature of the message contents.
    attr_accessor :signature
    # @return [Integer] (u32)
    #   The serial number of the message this message is a reply for.
    attr_accessor :reply_serial
    # The protocol.
    attr_reader :protocol
    # @return [Integer] (u32)
    #   The serial of the message.
    attr_reader :serial
    # The parameters of the message.
    attr_reader :params
    # @return [Array<IO>] UNIX file descriptors carried by this message
    attr_reader :unix_fds

    # Create a message with message type _mtype_ with default values and a
    # unique serial number.
    def initialize(mtype = INVALID)
      @message_type = mtype

      @flags = 0
      @protocol = 1
      @body_length = 0
      @signature = ""
      @@serial_mutex.synchronize do
        @serial = @@serial
        @@serial += 1
      end
      @params = []
      @unix_fds = []
      @owns_unix_fds = false
      @destination = nil
      @interface = nil
      @error_name = nil
      @member = nil
      @path = nil
      @reply_serial = nil
      @flags = NO_REPLY_EXPECTED if mtype == METHOD_RETURN
    end

    def to_s
      "#{message_type} sender=#{sender} -> dest=#{destination} " \
        "serial=#{serial} reply_serial=#{reply_serial} " \
        "path=#{path}; interface=#{interface}; member=#{member} " \
        "error_name=#{error_name}"
    end

    # @return [String] name of message type, as used in match rules:
    #    "method_call", "method_return", "signal", "error"
    def message_type_s
      TYPE_NAMES[message_type] || "unknown_type_#{message_type}"
    end

    # Create a regular reply to a message _msg_.
    def self.method_return(msg)
      MethodReturnMessage.new.reply_to(msg)
    end

    # Create an error reply to a message _msg_.
    def self.error(msg, error_name, description = nil)
      ErrorMessage.new(error_name, description).reply_to(msg)
    end

    # Mark this message as a reply to a another message _msg_, taking
    # the serial number of _msg_ as reply serial and the sender of _msg_ as
    # destination.
    def reply_to(msg)
      @reply_serial = msg.serial
      @destination = msg.sender
      self
    end

    # Add a parameter _val_ of type _type_ to the message.
    # For an outgoing UNIX_FD ("h"), the message borrows *val*; the sender
    # retains ownership and must keep it open until {MessageQueue#push}.
    def add_param(type, val)
      type = type.chr if type.is_a?(Integer)
      @signature += type.to_s
      if type.to_s == "h"
        io = unix_fd_io(val)
        unless io
          raise ArgumentError, "UNIX_FD parameter must be an IO"
        end

        @unix_fds << io unless @unix_fds.any? { |fd| fd.fileno == io.fileno }
      end
      @params << [type, val]
    end

    # "l" or "B"
    ENDIANNESS_CHAR = ENV.fetch("RUBY_DBUS_ENDIANNESS", HOST_END)
    ENDIANNESS = RawMessage.endianness(ENDIANNESS_CHAR)

    # FIXME: what are these? a message element constant enumeration?
    # See method below, in a message, you have and array of optional parameters
    # that come with an index, to determine their meaning. The values are in
    # spec, more a definition than an enumeration.

    PATH = 1
    INTERFACE = 2
    MEMBER = 3
    ERROR_NAME = 4
    REPLY_SERIAL = 5
    DESTINATION = 6
    SENDER = 7
    SIGNATURE = 8
    UNIX_FDS = 9

    RESERVED_PATH = "/org/freedesktop/DBus/Local"

    # Marshall the message with its current set parameters and return
    # it in a packet form.
    # @return [String]
    def marshall
      marshall_with_fds.first
    end

    # Marshall the message and return its bytes and the IO objects that must
    # be sent as SCM_RIGHTS. SCM_RIGHTS duplicates the descriptors, so the
    # sender retains and is responsible for closing the original objects.
    # @return [Array(String,Array<IO>)]
    def marshall_with_fds
      ObjectSpace.undefine_finalizer(self)
      if @path == RESERVED_PATH
        # the bus would disconnect us, better explain why
        raise "Cannot send a message with the reserved path #{RESERVED_PATH}: #{inspect}"
      end

      unix_fds = []
      params_marshaller = PacketMarshaller.new(endianness: ENDIANNESS, unix_fds: unix_fds)
      @params.each do |type, value|
        params_marshaller.append(type, value)
      end
      @body_length = params_marshaller.packet.bytesize

      marshaller = PacketMarshaller.new(endianness: ENDIANNESS, unix_fds: unix_fds)
      marshaller.append(Type::BYTE, ENDIANNESS_CHAR.ord)
      marshaller.append(Type::BYTE, @message_type)
      marshaller.append(Type::BYTE, @flags)
      marshaller.append(Type::BYTE, @protocol)
      marshaller.append(Type::UINT32, @body_length)
      marshaller.append(Type::UINT32, @serial)

      headers = []
      headers << [PATH,         ["o", @path]]         if @path
      headers << [INTERFACE,    ["s", @interface]]    if @interface
      headers << [MEMBER,       ["s", @member]]       if @member
      headers << [ERROR_NAME,   ["s", @error_name]]   if @error_name
      headers << [REPLY_SERIAL, ["u", @reply_serial]] if @reply_serial
      headers << [DESTINATION,  ["s", @destination]]  if @destination
      #           SENDER is not sent, the message bus fills it in instead
      headers << [SIGNATURE,    ["g", @signature]]    if @signature != ""
      headers << [UNIX_FDS,     ["u", unix_fds.size]] if unix_fds.any?
      marshaller.append("a(yv)", headers)

      marshaller.align(8)

      @unix_fds = unix_fds
      @owns_unix_fds = false
      [marshaller.packet + params_marshaller.packet, unix_fds]
    end

    # Unmarshall a packet contained in the buffer _buf_ and set the
    # parameters of the message object according the data found in the
    # buffer.
    # @return [Array(Message,Integer)]
    #   the detected message (self) and
    #   the index pointer of the buffer where the message data ended.
    def unmarshall_buffer(buf, unix_fds = nil)
      ObjectSpace.undefine_finalizer(self)
      @unix_fds = []
      @owns_unix_fds = false
      unix_fd_count = nil
      pu = PacketUnmarshaller.new(buf, RawMessage.endianness(buf[0]))
      mdata = pu.unmarshall(MESSAGE_SIGNATURE)
      _, @message_type, @flags, @protocol, @body_length, @serial,
        headers = mdata

      headers.each do |struct|
        case struct[0]
        when PATH
          @path = struct[1]
        when INTERFACE
          @interface = struct[1]
        when MEMBER
          @member = struct[1]
        when ERROR_NAME
          @error_name = struct[1]
        when REPLY_SERIAL
          @reply_serial = struct[1]
        when DESTINATION
          @destination = struct[1]
        when SENDER
          @sender = struct[1]
        when SIGNATURE
          @signature = struct[1]
        when UNIX_FDS
          unix_fd_count = struct[1]
        end
      end
      pu.align_body
      body_position = pu.consumed_size
      if unix_fd_count
        unix_fds ||= []
        unless unix_fd_count == unix_fds.size
          raise InvalidPacketException,
                "UNIX_FDS header says #{unix_fd_count}, received #{unix_fds.size}"
        end
      elsif unix_fds && !unix_fds.empty?
        raise InvalidPacketException, "received #{unix_fds.size} UNIX_FDs without UNIX_FDS header"
      end
      if @body_length.positive? && @signature
        body_pu = PacketUnmarshaller.new(buf, RawMessage.endianness(buf[0]), unix_fds)
        body_pu.seek(body_position)
        @params = body_pu.unmarshall(@signature, @body_length)
      elsif unix_fd_count.to_i.positive?
        raise InvalidPacketException, "UNIX_FDS header present but message body has no UNIX_FD"
      end
      @unix_fds = (unix_fds || []).dup
      @owns_unix_fds = true
      define_unix_fd_finalizer
      consumed_size = body_position + @body_length
      consumed_size = pu.consumed_size if @body_length.zero?
      [self, consumed_size]
    end

    # Close FDs still owned by this parsed message. A parsed message owns its
    # received FDs until the application calls {#disown_unix_fds}.
    def close_unix_fds
      return unless @owns_unix_fds

      ObjectSpace.undefine_finalizer(self)
      @unix_fds.each { |io| io.close unless io.closed? }
      @owns_unix_fds = false
    end

    # Transfer ownership of one FD (and the remaining FDs through
    # {#disown_unix_fds}) to the caller. FDs not taken remain owned and are
    # closed when the message is discarded.
    def take_unix_fd(index)
      io = @unix_fds[index]
      raise IndexError if io.nil?
      ObjectSpace.undefine_finalizer(self)
      @unix_fds.delete(io)
      @owns_unix_fds = @unix_fds.any?
      define_unix_fd_finalizer if @owns_unix_fds
      io
    end

    # Transfer ownership of this message's FDs to its caller. After this call
    # the message and its finalizer will not close them.
    def disown_unix_fds
      ObjectSpace.undefine_finalizer(self)
      @owns_unix_fds = false
      @unix_fds
    end

    # Return the framed message size, or nil if the buffer is incomplete.
    def self.frame_size(buf)
      return nil if buf.bytesize < 16

      endianness = RawMessage.endianness(buf[0])
      format = Data::UInt32.format[endianness]
      body_length = buf.byteslice(4, 4).unpack1(format)
      header_array_length = buf.byteslice(12, 4).unpack1(format)
      header_size = (16 + header_array_length + 7) & ~7
      size = header_size + body_length
      raise InvalidPacketException, "message too large (#{size} bytes)" if size > 134_217_728

      return nil if buf.bytesize < size

      size
    end

    def self.finalize_unix_fds(file_descriptors)
      proc do
        file_descriptors.each(&:close)
      end
    end

    def define_unix_fd_finalizer
      finalizer = self.class.send(:finalize_unix_fds, @unix_fds.dup)
      ObjectSpace.define_finalizer(self, finalizer)
    end

    def unix_fd_io(value)
      value = value.value if value.is_a?(Data::UnixFD)
      return value if value.is_a?(::IO)
      return value.to_io if value.respond_to?(:to_io)

      nil
    end

    # Make a new exception from ex, mark it as being caused by this message
    # @api private
    def annotate_exception(exc)
      new_exc = exc.exception("#{exc}; caused by #{self}")
      new_exc.set_backtrace(exc.backtrace)
      new_exc
    end
  end

  class MethodReturnMessage < Message
    def initialize
      super(METHOD_RETURN)
    end
  end

  class ErrorMessage < Message
    def initialize(error_name, description = nil)
      super(ERROR)
      @error_name = error_name
      add_param(Type::STRING, description) unless description.nil?
    end

    def self.from_exception(exc)
      name = if exc.is_a? DBus::Error
               exc.name
             else
               "org.freedesktop.DBus.Error.Failed"
               # exc.class.to_s # RuntimeError is not a valid name, has no dot
             end
      description = exc.message
      msg = new(name, description)
      msg.add_param(DBus.type("as"), exc.backtrace)
      msg
    end
  end
end
