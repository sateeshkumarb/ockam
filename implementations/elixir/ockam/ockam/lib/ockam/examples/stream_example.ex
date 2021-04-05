defmodule Ockam.Examples.StreamExample do
  @tcp %Ockam.Transport.TCPAddress{ip: {127, 0, 0, 1}, port: 4000}

  alias Ockam.Message

  require Logger

  def run() do
    ensure_tcp()

    stream_name = "FOO"

    {:ok, consumer_address} = __MODULE__.Consumer.create(stream_name: stream_name)

    {:ok, publisher_address} = __MODULE__.Publisher.create(stream_name: stream_name)

    :timer.sleep(5000)

    publisher = Ockam.Node.whereis(publisher_address)
    consumer = Ockam.Node.whereis(consumer_address)

    __MODULE__.Publisher.send(publisher, "FOOO")

    __MODULE__.Consumer.fetch(consumer, 0, 10)

    %{consumer: consumer_address, publisher: publisher_address}
  end

  def send_message(publisher_address, message) do
    __MODULE__.Publisher.send(Ockam.Node.whereis(publisher_address), message)
  end

  def fetch_messages(consumer_address, index, limit) do
    __MODULE__.Consumer.fetch(Ockam.Node.whereis(consumer_address), index, limit)
  end

  def ensure_tcp() do
    Ockam.Transport.TCP.create_listener(port: 3000, route_outgoing: true)
  end

  def send_create_stream(stream_name, address) do
    Ockam.Router.route(%{
      onward_route: [@tcp, "stream_service"],
      return_route: [address],
      payload: create_request(stream_name)
    })
  end

  def create_request(stream_name) do
    :bare.encode(%{mailbox_name: stream_name}, {:struct, [mailbox_name: :string]})
  end

  def parse_create_response(message) do
    case Message.payload(message) do
      "" -> {:ok, Message.return_route(message)}
      _ -> :error
    end
  end

  # type PushRequest {
  #   message_id: i64
  #   data: data
  # }
  def push_request_schema() do
    {:struct, [message_id: :i64, data: :data]}
  end

  # type PullRequest {
  #   index: i64
  #   limit: i64
  # }
  def pull_request_schema() do
    {:struct, [index: :i64, limit: :i64]}
  end

  # type Request (PushRequest | PullRequest)
  def request_schema() do
    {:union, [push_request_schema(), pull_request_schema()]}
  end
end

defmodule Ockam.Examples.StreamExample.Consumer do
  use Ockam.Worker

  alias Ockam.Examples.StreamExample

  require Logger

  def fetch(server, index, limit) do
    GenServer.call(server, {:fetch, index, limit})
  end

  @impl true
  def setup(options, state) do
    stream_name = Keyword.fetch!(options, :stream_name)

    StreamExample.send_create_stream(stream_name, state.address)

    {:ok, Map.put(state, :stream_name, stream_name)}
  end

  @impl true
  def handle_message(message, state) do
    Logger.info("Consumer received message #{inspect(message)}")

    case StreamExample.parse_create_response(message) do
      {:ok, return_route} ->
        {:ok, Map.put(state, :stream_address, return_route)}

      :error ->
        case decode_message(message) do
          {:message, data, index} ->
            Logger.info("Message #{inspect(data)} with index #{inspect(index)}")
            {:ok, state}

          other ->
            Logger.error("Unexpected message #{inspect(other)}")
            {:ok, state}
        end
    end
  end

  def handle_call({:fetch, index, limit}, _from, state) do
    case Map.get(state, :stream_address) do
      nil ->
        {:reply, :no_stream, state}

      address ->
        fetch_messages(index, limit, address, state)
        {:reply, :ok, state}
    end
  end

  def fetch_messages(index, limit, address, state) do
    Ockam.Router.route(%{
      onward_route: address,
      return_route: [state.address],
      payload: fetch_request(index, limit)
    })
  end

  def fetch_request(index, limit) do
    :bare.encode(
      {StreamExample.pull_request_schema(), %{index: index, limit: limit}},
      StreamExample.request_schema()
    )
  end

  def decode_message(message) do
    payload = Ockam.Message.payload(message)

    case :bare.decode(payload, {:struct, [index: :i64, data: :data]}) do
      {:ok, %{index: index, data: data}, ""} ->
        {:message, data, index}

      other ->
        {:decode_error, other}
    end
  end
end

defmodule Ockam.Examples.StreamExample.Publisher do
  use Ockam.Worker

  alias Ockam.Examples.StreamExample

  require Logger

  def send(server, message) do
    GenServer.call(server, {:send, message})
  end

  @impl true
  def setup(options, state) do
    stream_name = Keyword.fetch!(options, :stream_name)

    StreamExample.send_create_stream(stream_name, state.address)

    {:ok, Map.put(state, :stream_name, stream_name)}
  end

  @impl true
  def handle_message(message, state) do
    Logger.info("Publisher received message #{inspect(message)}")

    case StreamExample.parse_create_response(message) do
      {:ok, return_route} ->
        {:ok, Map.put(state, :stream_address, return_route)}

      :error ->
        case decode_message(message) do
          {:publish_confirm, id} ->
            Logger.info("Publish confirm for message id #{inspect(id)}")
            {:ok, state}

          other ->
            Logger.error("Unexpected message #{inspect(other)}")
            {:ok, state}
        end
    end
  end

  def decode_message(message) do
    payload = Ockam.Message.payload(message)

    case :bare.decode(payload, {:struct, [status: {:enum, [:ok, :error]}, message_id: :i64]}) do
      {:ok, %{status: :ok, message_id: id}, ""} ->
        {:publish_confirm, id}

      {:ok, %{status: :error, message_id: id}, ""} ->
        Logger.error("Publish confirm error")
        {:publish_confirm_error, id}

      other ->
        {:decode_error, other}
    end
  end

  @impl true
  def handle_call({:send, message}, _from, state) do
    case Map.get(state, :stream_address) do
      nil ->
        {:reply, :no_stream, state}

      address ->
        state = send_message(message, address, state)
        {:reply, :ok, state}
    end
  end

  def send_message(message, address, state) do
    next_id = Map.get(state, :message_id, 0) + 1

    Ockam.Router.route(%{
      onward_route: address,
      return_route: [state.address],
      payload: message_request(message, next_id)
    })

    Map.put(state, :message_id, next_id)
  end

  def message_request(message, id) do
    :bare.encode(
      {StreamExample.push_request_schema(), %{data: message, message_id: id}},
      StreamExample.request_schema()
    )
  end
end
