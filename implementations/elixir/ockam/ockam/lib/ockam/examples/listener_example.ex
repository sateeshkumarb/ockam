defmodule Ockam.Examples.ListenerExample do
  alias Ockam.Transport.TCP

  def run() do
    ensure_tcp()
    __MODULE__.Lis.create(reply_to: self())

    receive do
      {:registered, route} ->
        listener_alias = List.last(route)
        __MODULE__.Initiator.create(listener_alias: listener_alias)
    end
  end

  def ensure_tcp() do
    TCP.create_listener(port: 3000, route_outgoing: true)
  end
end

defmodule Ockam.Examples.ListenerExample.Initiator do
  use Ockam.Worker

  alias Ockam.Message
  alias Ockam.Router

  alias Ockam.Transport.TCPAddress

  require Logger

  @impl true
  def setup(options, state) do
    listener_alias = Keyword.fetch!(options, :listener_alias)
    initialize(listener_alias, state.address)

    {:ok, Map.put(state, :hi_counter, 1)}
  end

  @impl true
  def handle_message(%{payload: _} = message, %{hi_counter: counter} = state) do
    Logger.info("Received message on initiator #{inspect(message)}")

    :timer.sleep(2000)

    Router.route(%{
      onward_route: Message.return_route(message),
      return_route: [state.address],
      payload: "HI! <> #{counter}"
    })

    {:ok, Map.put(state, :hi_counter, counter + 1)}
  end

  def initialize(listener_alias, address) do
    # host = {138,91,152,195}
    host = {127, 0, 0, 1}
    port = 4000

    Router.route(%{
      onward_route: [%TCPAddress{ip: host, port: port}, listener_alias],
      return_route: [address],
      payload: "initiate"
    })
  end
end

defmodule Ockam.Examples.ListenerExample.Lis do
  @moduledoc false

  use Ockam.Worker

  alias Ockam.Message
  alias Ockam.Router

  alias Ockam.Transport.TCPAddress

  require Logger

  @impl true
  def setup(options, state) do
    reply_to = Keyword.fetch!(options, :reply_to)
    register_in_forwarder(state.address)
    {:ok, Map.put(state, :reply_to, reply_to)}
  end

  @impl true
  def handle_message(%{payload: "register"} = message, %{reply_to: reply_to} = state) do
    route = Message.return_route(message)

    Logger.info("Registered as #{inspect(route)}")
    send(reply_to, {:registered, route})

    {:ok, state}
  end

  def handle_message(%{payload: _} = message, state) do
    Logger.info("received a spawn message #{inspect(message)}")

    return_route = Message.return_route(message)
    payload = Message.payload(message)
    spawn_responder(return_route, payload)

    {:ok, state}
  end

  def spawn_responder(return_route, payload) do
    Ockam.Examples.ListenerExample.Responder.create(
      respond_route: return_route,
      respond_msg: payload
    )
  end

  def register_in_forwarder(address) do
    # host = {138,91,152,195}
    host = {127, 0, 0, 1}
    port = 4000
    forwarder_address = [%TCPAddress{ip: host, port: port}, "forwarding_service"]

    Router.route(%{
      onward_route: forwarder_address,
      return_route: [address],
      payload: "register"
    })
  end
end

defmodule Ockam.Examples.ListenerExample.Responder do
  @moduledoc false

  use Ockam.Worker

  alias Ockam.Message
  alias Ockam.Router

  require Logger

  @impl true
  def setup(options, state) do
    Logger.info("Responder starting with #{inspect(options)}")
    respond_route = Keyword.fetch!(options, :respond_route)
    respond_msg = Keyword.fetch!(options, :respond_msg)

    respond = %{
      onward_route: respond_route,
      return_route: [state.address],
      payload: respond_msg
    }

    Router.route(respond)
    {:ok, state}
  end

  @impl true
  def handle_message(%{payload: _} = message, state) do
    Logger.info("Responder got message #{inspect(message)}")
    send_replY(message, state)
    {:ok, state}
  end

  def send_replY(message, state) do
    reply = %{
      onward_route: Message.return_route(message),
      return_route: [state.address],
      payload: Message.payload(message)
    }

    Router.route(reply)
  end
end
