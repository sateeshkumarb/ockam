defmodule Ockam.Hub.Service.Stream do
  @moduledoc false

  use Ockam.Worker
  use Ockam.MessageProtocol

  alias Ockam.Hub.Service.Stream.Instance

  alias Ockam.Message
  alias Ockam.Router

  require Logger

  @type state() :: map()

  @impl true
  def protocol_mapping() do
    Ockam.Protocol.mapping([
      {:server, Ockam.Protocol.Stream.Create},
      {:server, Ockam.Protocol.Error}
    ])
  end

  @impl true
  def handle_message(%{payload: payload} = message, state) do
    state =
      case decode_payload(payload) do
        {:ok, "stream_create", %{stream_name: name}} ->
          ensure_stream(name, message, state)

        {:error, error} ->
          return_error(error, message, state)
      end

    {:ok, state}
  end

  def return_error(error, message, state) do
    Logger.error("Error creating stream: #{inspect(error)}")

    Ockam.Router.route(%{
      onward_route: Message.return_route(message),
      return_route: [state.address],
      payload: encode_payload("error", %{reason: "Invalid request"})
    })
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
          create_stream_name(state)

        _defined ->
          create_name
      end

    return_route = Message.return_route(message)

    {:ok, address} = Instance.create(reply_route: return_route, stream_name: name)

    register_stream(name, address, state)
  end

  def create_stream_name(state) do
    random_string = "generated_" <> Base.encode16(:crypto.strong_rand_bytes(4), case: :lower)

    case find_stream(random_string, state) do
      {:ok, _} -> create_stream_name(state)
      :error -> random_string
    end
  end
end

defmodule Ockam.Hub.Service.Stream.Instance do
  @moduledoc false

  use Ockam.Worker
  use Ockam.MessageProtocol

  require Logger

  @type request() :: binary()
  @type state() :: map()

  def notify(server, return_route) do
    GenServer.cast(server, {:notify, return_route})
  end

  @impl true
  def protocol_mapping() do
    Ockam.Protocol.mapping([
      {:server, Ockam.Protocol.Stream.Create},
      {:server, Ockam.Protocol.Stream.Push},
      {:server, Ockam.Protocol.Stream.Pull},
      {:server, Ockam.Protocol.Error}
    ])
  end

  @impl true
  def handle_cast({:notify, return_route}, state) do
    reply_init(state.stream_name, return_route, state)
    {:noreply, state}
  end

  @impl true
  def setup(options, state) do
    reply_route = Keyword.fetch!(options, :reply_route)
    stream_name = Keyword.fetch!(options, :stream_name)

    state = Map.merge(state, %{reply_route: reply_route, stream_name: stream_name})

    reply_init(stream_name, reply_route, state)

    {:ok, state}
  end

  @impl true
  def handle_message(%{payload: payload, return_route: return_route}, state) do
    case decode_payload(payload) do
      {:ok, type, data} ->
        handle_data(type, data, return_route, state)

      {:error, err} ->
        handle_decode_error(err, return_route, state)
    end
  end

  def handle_decode_error(err, return_route, state) do
    Logger.error("Error decoding request: #{inspect(err)}")
    error_reply = encode_error("Invalid request")
    send_reply(error_reply, return_route, state)
  end

  def handle_data("stream_push", push_request, return_route, state) do
    Logger.info("Push message #{inspect(push_request)}")
    %{request_id: id, data: data} = push_request
    {result, state} = save_message(data, state)
    reply_push_confirm(result, id, return_route, state)
    {:ok, state}
  end

  def handle_data("stream_pull", pull_request, return_route, state) do
    Logger.info("Pull request #{inspect(pull_request)}")
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

  def reply_init(stream_name, reply_route, state) do
    Logger.info("INIT stream #{inspect({stream_name, reply_route})}")
    init_payload = encode_init(stream_name)
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

  def encode_init(stream_name) do
    encode_payload("stream_create", %{stream_name: stream_name})
  end

  def encode_push_confirm({:ok, index}, id) do
    encode_payload("stream_push", %{status: :ok, request_id: id, index: index})
  end

  def encode_push_confirm({:error, error}, id) do
    Logger.error("Error saving message: #{inspect(error)}")

    encode_payload("stream_push", %{status: :error, request_id: id, index: 0})
  end

  def encode_pull_response(messages, request_id) do
    encode_payload("stream_pull", %{request_id: request_id, messages: messages})
  end

  def encode_error(reason) do
    encode_payload("error", %{reason: reason})
  end
end
