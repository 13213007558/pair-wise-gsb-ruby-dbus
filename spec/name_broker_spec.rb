# frozen_string_literal: true

require_relative "spec_helper"
require_relative "tools/name_broker"

describe DBus::Test::NameBroker do
  subject(:broker) { described_class.new }

  let(:name) { "org.example.Contended" }

  # three fake connections contending for the same well-known name
  let!(:conn_a) { broker.connect.tap(&:hello) }
  let!(:conn_b) { broker.connect.tap(&:hello) }
  let!(:conn_c) { broker.connect.tap(&:hello) }

  let(:allow_replacement) { described_class::NAME_FLAG_ALLOW_REPLACEMENT }
  let(:replace_existing)  { described_class::NAME_FLAG_REPLACE_EXISTING }
  let(:do_not_queue)      { described_class::NAME_FLAG_DO_NOT_QUEUE }

  describe "Hello" do
    it "assigns unique names in order" do
      expect(conn_a.unique_name).to eq ":1.1"
      expect(conn_b.unique_name).to eq ":1.2"
      expect(conn_c.unique_name).to eq ":1.3"
    end

    it "returns the unique name" do
      connection = broker.connect
      expect(connection.hello).to eq connection.unique_name
    end

    it "refuses a second Hello" do
      expect { conn_a.hello }.to raise_error(/Hello/)
    end

    it "refuses RequestName before Hello" do
      connection = broker.connect
      expect { connection.request_name(name) }.to raise_error(/Hello/)
    end
  end

  describe "RequestName" do
    it "makes the first requester the primary owner" do
      reply = conn_a.request_name(name)
      expect(reply).to eq described_class::REQUEST_NAME_REPLY_PRIMARY_OWNER
      expect(broker.signals).to eq [[name, "", conn_a.unique_name]]
    end

    it "queues a plain request for an owned name" do
      conn_a.request_name(name)
      reply = conn_b.request_name(name)
      expect(reply).to eq described_class::REQUEST_NAME_REPLY_IN_QUEUE
      expect(broker.list_queued_owners(name)).to eq [conn_b.unique_name]
    end

    it "rejects DO_NOT_QUEUE for an owned name without queueing" do
      conn_a.request_name(name)
      reply = conn_c.request_name(name, do_not_queue)
      expect(reply).to eq described_class::REQUEST_NAME_REPLY_EXISTS
      expect(broker.list_queued_owners(name)).to be_empty
    end

    it "tells the primary owner it already owns the name" do
      conn_a.request_name(name)
      reply = conn_a.request_name(name, do_not_queue)
      expect(reply).to eq described_class::REQUEST_NAME_REPLY_ALREADY_OWNER
    end

    it "does not queue a connection twice" do
      conn_a.request_name(name)
      expect(conn_b.request_name(name)).to eq described_class::REQUEST_NAME_REPLY_IN_QUEUE
      expect(conn_b.request_name(name)).to eq described_class::REQUEST_NAME_REPLY_IN_QUEUE
      expect(broker.list_queued_owners(name)).to eq [conn_b.unique_name]
    end

    it "queues REPLACE_EXISTING when the owner did not ALLOW_REPLACEMENT" do
      conn_a.request_name(name)
      reply = conn_b.request_name(name, replace_existing)
      expect(reply).to eq described_class::REQUEST_NAME_REPLY_IN_QUEUE
      expect(broker.list_queued_owners(name)).to eq [conn_b.unique_name]
    end

    it "lets REPLACE_EXISTING take over from an owner that allowed it" do
      conn_a.request_name(name, allow_replacement)
      reply = conn_b.request_name(name, replace_existing)
      expect(reply).to eq described_class::REQUEST_NAME_REPLY_PRIMARY_OWNER
      # the replaced owner keeps its flags at the head of the queue
      expect(broker.list_queued_owners(name)).to eq [conn_a.unique_name]
      expect(broker.signals).to eq [
        [name, "", conn_a.unique_name],
        [name, conn_a.unique_name, ""],
        [name, "", conn_b.unique_name]
      ]
    end

    it "prefers replacement over DO_NOT_QUEUE when both flags are given" do
      conn_a.request_name(name, allow_replacement)
      reply = conn_b.request_name(name, replace_existing | do_not_queue)
      expect(reply).to eq described_class::REQUEST_NAME_REPLY_PRIMARY_OWNER
    end

    it "moves a queued requester out of the queue when it takes over" do
      conn_a.request_name(name, allow_replacement)
      conn_b.request_name(name)
      conn_c.request_name(name)
      expect(conn_b.request_name(name, replace_existing))
        .to eq described_class::REQUEST_NAME_REPLY_PRIMARY_OWNER
      expect(broker.list_queued_owners(name))
        .to eq [conn_a.unique_name, conn_c.unique_name]
    end

    it "rejects invalid and unique names" do
      expect { conn_a.request_name("not a name") }.to raise_error(ArgumentError)
      expect { conn_a.request_name(":1.42") }.to raise_error(ArgumentError)
    end
  end

  describe "ReleaseName" do
    it "reports NON_EXISTENT for a name nobody owns" do
      expect(conn_a.release_name(name)).to eq described_class::RELEASE_NAME_REPLY_NON_EXISTENT
    end

    it "reports NOT_OWNER for a connection that neither owns nor queues" do
      conn_a.request_name(name)
      expect(conn_c.release_name(name)).to eq described_class::RELEASE_NAME_REPLY_NOT_OWNER
    end

    it "drops a queued requester without any signal" do
      conn_a.request_name(name)
      conn_b.request_name(name)
      expect(conn_b.release_name(name)).to eq described_class::RELEASE_NAME_REPLY_RELEASED
      expect(broker.list_queued_owners(name)).to be_empty
      expect(broker.signals).to eq [[name, "", conn_a.unique_name]]
    end

    it "hands the name over to the queue head" do
      conn_a.request_name(name)
      conn_b.request_name(name)
      conn_c.request_name(name)
      expect(conn_a.release_name(name)).to eq described_class::RELEASE_NAME_REPLY_RELEASED
      expect(broker.list_queued_owners(name)).to eq [conn_c.unique_name]
      expect(broker.signals).to eq [
        [name, "", conn_a.unique_name],
        [name, conn_a.unique_name, ""],
        [name, "", conn_b.unique_name]
      ]
    end

    it "forgets the name when nobody is queued" do
      conn_a.request_name(name)
      conn_a.release_name(name)
      expect(conn_a.release_name(name)).to eq described_class::RELEASE_NAME_REPLY_NON_EXISTENT
    end
  end

  describe "ListQueuedOwners" do
    it "is empty for an unknown name" do
      expect(broker.list_queued_owners("org.example.Nobody")).to eq []
    end

    it "lists queued owners in order, without the primary owner" do
      conn_a.request_name(name)
      conn_b.request_name(name)
      conn_c.request_name(name)
      expect(broker.list_queued_owners(name))
        .to eq [conn_b.unique_name, conn_c.unique_name]
    end
  end

  describe "disconnect" do
    it "hands owned names over to the next queued owner" do
      conn_a.request_name(name)
      conn_b.request_name(name)
      conn_c.request_name(name)
      conn_a.disconnect
      expect(broker.list_queued_owners(name)).to eq [conn_c.unique_name]
      expect(broker.signals).to eq [
        [name, "", conn_a.unique_name],
        [name, conn_a.unique_name, ""],
        [name, "", conn_b.unique_name],
        [conn_a.unique_name, conn_a.unique_name, ""]
      ]
    end

    it "silently drops queued requests of the dead connection" do
      conn_a.request_name(name)
      conn_b.request_name(name)
      conn_b.disconnect
      expect(broker.list_queued_owners(name)).to be_empty
      expect(broker.signals).to eq [
        [name, "", conn_a.unique_name],
        [conn_b.unique_name, conn_b.unique_name, ""]
      ]
    end

    it "forgets names whose owner died with an empty queue" do
      conn_a.request_name(name)
      conn_a.disconnect
      expect(conn_b.request_name(name))
        .to eq described_class::REQUEST_NAME_REPLY_PRIMARY_OWNER
    end

    it "stops delivering signals to the dead connection" do
      conn_a.request_name(name)
      conn_b.disconnect
      conn_a.release_name(name)
      expect(conn_b.signals).to eq [[name, "", conn_a.unique_name]]
    end

    it "refuses further calls on the connection" do
      conn_a.disconnect
      expect { conn_a.request_name(name) }.to raise_error(/disconnected/)
      expect { conn_a.hello }.to raise_error(/disconnected/)
    end
  end

  describe "a three-way contention script" do
    it "produces the exact return codes and all NameOwnerChanged signals" do
      # A takes the name, allowing replacement
      expect(conn_a.request_name(name, allow_replacement))
        .to eq described_class::REQUEST_NAME_REPLY_PRIMARY_OWNER

      # B replaces A; A falls back to the head of the queue
      expect(conn_b.request_name(name, replace_existing))
        .to eq described_class::REQUEST_NAME_REPLY_PRIMARY_OWNER
      expect(broker.list_queued_owners(name)).to eq [conn_a.unique_name]

      # C has to queue behind A
      expect(conn_c.request_name(name)).to eq described_class::REQUEST_NAME_REPLY_IN_QUEUE
      expect(broker.list_queued_owners(name))
        .to eq [conn_a.unique_name, conn_c.unique_name]

      # B dies; A (which still allows replacement) becomes owner again
      conn_b.disconnect
      expect(broker.list_queued_owners(name)).to eq [conn_c.unique_name]

      # C replaces A; A is queued again
      expect(conn_c.request_name(name, replace_existing))
        .to eq described_class::REQUEST_NAME_REPLY_PRIMARY_OWNER
      expect(broker.list_queued_owners(name)).to eq [conn_a.unique_name]

      # A gives up its queued request: RELEASED, no signal
      expect(conn_a.release_name(name)).to eq described_class::RELEASE_NAME_REPLY_RELEASED
      expect(broker.list_queued_owners(name)).to be_empty

      # C dies; the name is forgotten
      conn_c.disconnect
      expect(conn_a.release_name(name)).to eq described_class::RELEASE_NAME_REPLY_NON_EXISTENT

      expect(broker.signals).to eq [
        [name, "", conn_a.unique_name],               # A owns it
        [name, conn_a.unique_name, ""],               # B replaces A
        [name, "", conn_b.unique_name],
        [name, conn_b.unique_name, ""],               # B dies, A is promoted
        [name, "", conn_a.unique_name],
        [conn_b.unique_name, conn_b.unique_name, ""], # B's unique name is gone
        [name, conn_a.unique_name, ""],               # C replaces A
        [name, "", conn_c.unique_name],
        [name, conn_c.unique_name, ""],               # C dies, queue is empty
        [conn_c.unique_name, conn_c.unique_name, ""]
      ]

      # A stayed connected the whole time and saw everything
      expect(conn_a.signals).to eq broker.signals
      # B died mid-script and missed its own loss signals
      expect(conn_b.signals).to eq broker.signals.first(3)
    end
  end
end
