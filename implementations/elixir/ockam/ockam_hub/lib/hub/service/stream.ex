defmodule Ockam.Hub.Service.Stream do
  use Ockam.Worker

  alias Ockam.Hub.Service.Stream.Instance

  alias Ockam.Message
  alias Ockam.Router

  require Logger

  @type state() :: map()

  @impl true
  def handle_message(%{payload: payload} = message, state) do
    state =
      case decode_payload(payload) do
        {:ok, %{mailbox_name: name}} ->
          # TODO: keep track of streams
          ensure_stream(name, message, state)

        {:error, error} ->
          return_error(error, message, state)
      end

    {:ok, state}
  end

  def decode_payload(payload) do
    case :bare.decode(payload, request_spec()) do
      {:ok, %{mailbox_name: name}, ""} ->
        {:ok, %{mailbox_name: name}}

      other ->
        {:error, {:invalid_payload, other}}
    end
  end

  def return_error(error, message, state) do
    Logger.error("Error creating stream: #{inspect(error)}")

    Ockam.Router.route(%{
      onward_route: Message.return_route(message),
      return_route: [state.address],
      payload: error_message()
    })
  end

  ## TODO: response type with optional error
  def error_message() do
    :bare.encode(%{error: "Invalid request"}, {:struct, [error: :string]})
  end

  @spec ensure_stream(String.t(), map(), state()) :: state()
  def ensure_stream(name, message, state) do
    case find_stream(name, state) do
      {:ok, stream} ->
        notify_create(stream, message, state)

      :error ->
        create_stream(name, message, state)
    end
  end

  @spec find_stream(String.t(), state()) :: {:ok, pid()} | :error
  def find_stream(name, state) do
    streams = Map.get(state, :streams, %{})
    Map.fetch(streams, name)
  end

  @spec register_stream(String.t(), String.t(), state()) :: state()
  def register_stream(name, address, state) do
    ## TODO: maybe use address in the registry?
    case Ockam.Node.whereis(address) do
      nil ->
        raise("Stream not found on address #{address}")

      pid when is_pid(pid) ->
        streams = Map.get(state, :streams, %{})
        Map.put(state, :streams, Map.put(streams, name, pid))
    end
  end

  @spec notify_create(pid(), map(), state()) :: state()
  def notify_create(stream, message, state) do
    return_route = Message.return_route(message)
    Instance.notify(stream, return_route)
    state
  end

  @spec create_stream(String.t(), map(), state()) :: state()
  def create_stream(name, message, state) do
    return_route = Message.return_route(message)

    {:ok, address} = Instance.create(reply_route: return_route, stream_name: name)

    register_stream(name, address, state)
  end

  # type CreateMailboxRequest {
  #   mailbox_name: string
  # }
  def request_spec() do
    {:struct, [mailbox_name: :string]}
  end
end

defmodule Ockam.Hub.Service.Stream.Instance do
  use Ockam.Worker

  require Logger

  @type request() :: binary()

  def notify(server, return_route) do
    GenServer.cast(server, {:notify, return_route})
  end

  @impl true
  def handle_cast({:notify, return_route}, state) do
    reply_create_ok(return_route, state)
    {:noreply, state}
  end

  @impl true
  def setup(options, state) do
    reply_route = Keyword.fetch!(options, :reply_route)
    stream_name = Keyword.fetch!(options, :stream_name)

    reply_create_ok(reply_route, state)

    {:ok, Map.merge(state, %{reply_route: reply_route, stream_name: stream_name})}
  end

  @impl true
  def handle_message(%{payload: payload, return_route: return_route}, state) do
    case decode_payload(payload) do
      {:ok, data} ->
        handle_data(data, return_route, state)

      {:error, err} ->
        handle_decode_error(err, return_route, state)
    end
  end

  @spec decode_payload(binary()) :: {:ok, request()} | {:error, any()}
  def decode_payload(payload) do
    push_request = push_request_schema()
    pull_request = pull_request_schema()

    case :bare.decode(payload, request_schema()) do
      {:ok, {^push_request, data}, ""} ->
        {:ok, {:push, data}}

      {:ok, {^pull_request, data}, ""} ->
        {:ok, {:pull, data}}

      other ->
        {:error, other}
    end
  end

  def handle_decode_error(err, return_route, state) do
    Logger.error("Error decoding request: #{inspect(err)}")
    error_reply = :bare.encode(%{reason: "invalid_request"}, error_schema())
    send_reply(error_reply, return_route, state)
  end

  def handle_data({:push, push_request}, return_route, state) do
    %{message_id: id, data: data} = push_request
    {result, state} = save_message(data, state)
    reply_push_confirm(result, id, return_route, state)
    {:ok, state}
  end

  def handle_data({:pull, pull_request}, return_route, state) do
    %{index: index, limit: limit} = pull_request
    messages = fetch_messages(index, limit, state)
    send_messages(messages, return_route, state)
    {:ok, state}
  end

  ## Queue API
  ## TODO: this needs to be extracted
  def save_message(data, state) do
    queue = Map.get(state, :queue, %{})
    latest = Map.get(queue, :latest, 0)
    next = latest + 1
    message = %{index: next, data: data}

    new_queue =
      queue
      |> Map.put(next, message)
      |> Map.put(:latest, next)

    {:ok, Map.put(state, :queue, new_queue)}
  end

  def fetch_messages(index, limit, state) do
    queue = Map.get(state, :queue, %{})
    earliest = Map.get(queue, :earliest, 0)
    start_from = max(index, earliest)
    end_on = start_from + limit - 1

    ## Naive impl. Gaps are ignored as there shouldn't be any
    :lists.seq(start_from, end_on)
    |> Enum.map(fn i -> Map.get(queue, i) end)
    |> Enum.reject(&is_nil/1)
  end

  ## Replies

  def reply_create_ok(reply_route, state) do
    send_reply("", reply_route, state)
  end

  def reply_push_confirm(result, id, return_route, state) do
    push_confirm = encode_push_confirm(result, id)
    send_reply(push_confirm, return_route, state)
  end

  def send_messages(messages, return_route, state) do
    Enum.each(messages, fn message ->
      reply = encode_message(message)
      send_reply(reply, return_route, state)
    end)
  end

  defp send_reply(data, reply_route, state) do
    :ok =
      Ockam.Router.route(%{
        onward_route: reply_route,
        return_route: [state.address],
        payload: data
      })
  end

  ### Encode helpers

  def encode_push_confirm(:ok, id) do
    :bare.encode(%{status: :ok, message_id: id}, push_confirm_schema())
  end

  def encode_push_confirm({:error, error}, id) do
    Logger.error("Error saving message: #{inspect(error)}")
    :bare.encode(%{status: :error, message_id: id}, push_confirm_schema())
  end

  def encode_message(%{index: index, data: data}) do
    :bare.encode(%{index: index, data: data}, message_schema())
  end

  ## BARE schemas:

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

  # enum Status {
  #   OK
  #   ERROR
  # }
  # type PushConfirm {
  #   message_id: i64
  #   status: Status
  # }
  def push_confirm_schema() do
    {:struct, [status: {:enum, [:ok, :error]}, message_id: :i64]}
  end

  # type MailboxMessage {
  #   index: i64
  #   data: data
  # }
  def message_schema() do
    {:struct, [index: :i64, data: :data]}
  end

  # type Error {
  #   reason: string
  # }
  def error_schema() do
    {:struct, [reason: :string]}
  end
end
