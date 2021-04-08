defmodule Ockam.Hub.Service.Stream do
  @moduledoc false

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
    case :bare.decode(payload, request_schema()) do
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
    :bare.encode(%{reason: "Invalid request"}, error_schema())
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
  def create_stream(create_name, message, state) do
    name =
      case create_name do
        :undefined ->
          create_mailbox_name(state)

        _defined ->
          create_name
      end

    return_route = Message.return_route(message)

    {:ok, address} = Instance.create(reply_route: return_route, mailbox_name: name)

    register_stream(name, address, state)
  end

  def create_mailbox_name(state) do
    random_string = "generated_" <> Base.encode16(:crypto.strong_rand_bytes(4), case: :lower)

    case find_stream(random_string, state) do
      {:ok, _} -> create_mailbox_name(state)
      :error -> random_string
    end
  end

  # type CreateMailboxRequest {
  #   mailbox_name: string
  # }
  def request_schema() do
    {:struct, [mailbox_name: {:optional, :string}]}
  end

  def error_schema() do
    {:struct, [reason: :string]}
  end
end

defmodule Ockam.Hub.Service.Stream.Instance do
  @moduledoc false

  use Ockam.Worker

  require Logger

  @type request() :: binary()
  @type state() :: map()

  def notify(server, return_route) do
    GenServer.cast(server, {:notify, return_route})
  end

  @impl true
  def handle_cast({:notify, return_route}, state) do
    reply_init(state.mailbox_name, return_route, state)
    {:noreply, state}
  end

  @impl true
  def setup(options, state) do
    reply_route = Keyword.fetch!(options, :reply_route)
    mailbox_name = Keyword.fetch!(options, :mailbox_name)

    state = Map.merge(state, %{reply_route: reply_route, mailbox_name: mailbox_name})

    reply_init(mailbox_name, reply_route, state)

    {:ok, state}
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
    error_reply = :bare.encode({error_schema(), %{reason: "invalid_request"}}, response_schema())
    send_reply(error_reply, return_route, state)
  end

  def handle_data({:push, push_request}, return_route, state) do
    %{request_id: id, data: data} = push_request
    {result, state} = save_message(data, state)
    reply_push_confirm(result, id, return_route, state)
    {:ok, state}
  end

  def handle_data({:pull, pull_request}, return_route, state) do
    %{request_id: request_id, index: index, limit: limit} = pull_request
    messages = fetch_messages(index, limit, state)
    reply_pull_response(messages, request_id, return_route, state)
    {:ok, state}
  end

  ## Queue API
  ## TODO: this needs to be extracted

  @spec save_message(any(), state()) :: {{:ok, integer()} | {:error, any()}, state()}
  def save_message(data, state) do
    storage = Map.get(state, :storage, %{})
    latest = Map.get(storage, :latest, 0)
    next = latest + 1
    message = %{index: next, data: data}

    new_storage =
      storage
      |> Map.put(next, message)
      |> Map.put(:latest, next)

    {{:ok, next}, Map.put(state, :storage, new_storage)}
  end

  @spec fetch_messages(integer(), integer(), state()) :: [%{index: integer(), data: any()}]
  def fetch_messages(index, limit, state) do
    storage = Map.get(state, :storage, %{})
    earliest = Map.get(storage, :earliest, 0)
    start_from = max(index, earliest)
    end_on = start_from + limit - 1

    ## Naive impl. Gaps are ignored as there shouldn't be any
    :lists.seq(start_from, end_on)
    |> Enum.map(fn i -> Map.get(storage, i) end)
    |> Enum.reject(&is_nil/1)
  end

  ## Replies

  def reply_init(mailbox_name, reply_route, state) do
    init_payload = encode_init(mailbox_name)
    send_reply(init_payload, reply_route, state)
  end

  def reply_push_confirm(result, id, return_route, state) do
    push_confirm = encode_push_confirm(result, id)
    send_reply(push_confirm, return_route, state)
  end

  def reply_pull_response(messages, request_id, return_route, state) do
    response = encode_pull_response(messages, request_id)
    send_reply(response, return_route, state)
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

  def encode_init(mailbox_name) do
    :bare.encode({init_schema(), %{mailbox_name: mailbox_name}}, response_schema())
  end

  def encode_push_confirm({:ok, index}, id) do
    :bare.encode({push_confirm_schema(), %{status: :ok, request_id: id, index: index}}, response_schema())
  end

  def encode_push_confirm({:error, error}, id) do
    Logger.error("Error saving message: #{inspect(error)}")
    :bare.encode({push_confirm_schema(), %{status: :error, request_id: id}}, response_schema())
  end

  def encode_pull_response(messages, request_id) do
    :bare.encode({pull_response_schema(), %{request_id: request_id, messages: messages}}, response_schema())
  end

  ## BARE schemas:

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
end
