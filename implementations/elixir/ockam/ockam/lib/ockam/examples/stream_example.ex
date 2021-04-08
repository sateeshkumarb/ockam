defmodule Ockam.Examples.StreamExample do
  @tcp %Ockam.Transport.TCPAddress{ip: {127, 0, 0, 1}, port: 4000}

  alias Ockam.Message

  require Logger

  def run() do
    ensure_tcp()

    stream_name = "FOO"

    {:ok, consumer_address} = __MODULE__.Consumer.create(stream_name: stream_name)

    {:ok, publisher_address} = __MODULE__.Publisher.create(stream_name: stream_name)

    Logger.info("Consumer #{inspect(consumer_address)} Publisher: #{inspect(publisher_address)}")

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
    :bare.encode(%{mailbox_name: stream_name}, {:struct, [mailbox_name: {:optional, :string}]})
  end

  def parse_response(message) do
    Logger.info("Received message #{inspect(message)}")
    payload = Message.payload(message)
    case :bare.decode(payload, response_schema()) do
      {:ok, {type, data}, ""} ->
        init = init_schema()
        push_confirm = push_confirm_schema()
        pull_response = pull_response_schema()
        error = error_schema()

        case type do
          ^init -> {:init, data}
          ^push_confirm -> {:push_confirm, data}
          ^pull_response -> {:pull_response, data}
          ^error -> {:error_response, data}
        end
      _ ->
        case :bare.decode(payload, get_index_response_schema()) do
          {:ok, %{} = data, ""} ->
            {:index, data}
          other ->
            Logger.error("Error parsing response: #{inspect(payload)} #{inspect(other)}")
            {:error, other}
        end
    end
  end


  # type Init {
  #   mailbox_name: string
  # }
  def init_schema() do
    {:struct, [mailbox_name: :string]}
  end

  # type PushRequest {
  #   request_id: uint
  #   data: data
  # }
  def push_request_schema() do
    {:struct, [request_id: :uint, data: :data]}
  end

  # type PullRequest {
  #   request_id: uint
  #   index: uint
  #   limit: uint
  # }
  def pull_request_schema() do
    {:struct, [request_id: :uint, index: :uint, limit: :uint]}
  end

  # type Request (PushRequest | PullRequest)
  def request_schema() do
    {:union, [push_request_schema(), pull_request_schema()]}
  end

  # enum Status {
  #   OK
  #   ERROR
  # }
  # type PushConfirm {
  #   request_id: uint
  #   status: Status,
  #   index: uint
  # }
  def push_confirm_schema() do
    {:struct, [request_id: :uint, status: {:enum, [:ok, :error]}, index: :uint]}
  end

  # type MailboxMessage {
  #   index: uint
  #   data: data
  # }
  def message_schema() do
    {:struct, [index: :uint, data: :data]}
  end

  # type PullResponse {
  #   request_id: uint
  #   messages: []MailboxMessage
  # }
  def pull_response_schema() do
    {:struct, [request_id: :uint, messages: {:array, message_schema()}]}
  end

  # type Error {
  #   reason: string
  # }
  def error_schema() do
    {:struct, [reason: :string]}
  end

  # type Response (Init | PushConfirm | PullResponse | Error)
  def response_schema() do
    {:union, [init_schema(), push_confirm_schema(), pull_response_schema(), error_schema()]}
  end

  def get_index_response_schema() do
    {:struct, [client_id: :string, index: :i64, mailbox_name: :string]}
  end
end

defmodule Ockam.Examples.StreamExample.Consumer do
  use Ockam.Worker

  alias Ockam.Examples.StreamExample

  alias Ockam.Message

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

    case StreamExample.parse_response(message) do
      {:init, _data} ->
        {:ok, Map.put(state, :stream_address, Message.return_route(message))}

      {:pull_response, data} ->
        Logger.info("Response for request: #{inspect(data.request_id)} Messages: #{inspect(data.messages)}")
        {:ok, state}

      other ->
        Logger.error("Unexpected message #{inspect(other)}")
        {:ok, state}
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
      {StreamExample.pull_request_schema(), %{request_id: :rand.uniform(100), index: index, limit: limit}},
      StreamExample.request_schema()
    )
  end
end

defmodule Ockam.Examples.StreamExample.Publisher do
  use Ockam.Worker

  alias Ockam.Examples.StreamExample

  alias Ockam.Message

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

    case StreamExample.parse_response(message) do
      {:init, _data} ->
        {:ok, Map.put(state, :stream_address, Message.return_route(message))}
      {:push_confirm, data} ->
        Logger.info("Publish confirm for request id #{inspect(data.request_id)} with index #{inspect(data.index)}")
      other ->
        Logger.error("Unexpected message #{inspect(other)}")
        {:ok, state}
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
    next_id = Map.get(state, :request_id, 0) + 1

    Ockam.Router.route(%{
      onward_route: address,
      return_route: [state.address],
      payload: message_request(message, next_id)
    })

    Map.put(state, :request_id, next_id)
  end

  def message_request(message, id) do
    :bare.encode(
      {StreamExample.push_request_schema(), %{data: message, request_id: id}},
      StreamExample.request_schema()
    )
  end
end
