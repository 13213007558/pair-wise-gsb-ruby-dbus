# frozen_string_literal: true

module DBus
  class BusNameBroker
    # A message queue that stays inside the test process.
    class MessageQueue
      # @param connection [BusNameBroker::Connection]
      # @param broker [BusNameBroker]
      def initialize(connection, broker)
        @connection = connection
        @broker = broker
        @mutex = Mutex.new
        @condition = ConditionVariable.new
        @messages = []
        @closed = false
        @dropped = false
      end

      # @param blocking [Boolean]
      # @return [Message,nil]
      def pop(blocking: true)
        @mutex.synchronize do
          if blocking
            @condition.wait(@mutex) while @messages.empty? && !@closed
          end
          @messages.shift
        end
      end

      def push(message)
        raise EOFError if @dropped

        @broker.send(@connection, message)
      end
      alias << push

      # Deliver a message from the broker directly to this connection.
      def receive(message)
        @mutex.synchronize do
          return if @dropped
          raise EOFError if @closed

          @messages << message
          @condition.signal
        end
      end

      # Deliver queued name-change signals to the connection without blocking.
      def dispatch
        while (message = pop(blocking: false))
          yield message
        end
      end

      # Simulate the transport disappearing.
      def close
        @mutex.synchronize do
          @closed = true
          @dropped = true
          @condition.broadcast
        end
      end

      def close_after_dispatch
        @mutex.synchronize do
          @dropped = true
          @closed = true
        end
      end
    end
  end
end
