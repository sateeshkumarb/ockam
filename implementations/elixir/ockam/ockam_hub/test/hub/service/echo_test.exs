defmodule Test.Hub.Service.EchoTest do
  use ExUnit.Case

  alias Ockam.Hub.Service.Echo, as: EchoService
  alias Ockam.Router

  test "echo test" do
    {:ok, proxy, proxy_address} =
      Test.Hub.Service.TestEchoProxy.start_link(address: {192, 168, 1, 1})

    {:ok, _echo, _echo_address} = EchoService.start_link(address: {192, 168, 1, 2})

    msg = %{onward_route: [proxy_address], return_route: [], payload: self()}
    r = Router.route(msg)

    receive do
      :ok ->
        assert true
      other ->
        IO.puts("Received: #{inspect(other)}")
        assert false
    after
      5_000 ->
        IO.puts("Nothing was received after 5 seconds")
        assert false
    end
  end
end

defmodule Test.Hub.Service.TestEchoProxy do
  @moduledoc false

  use Ockam.Worker
  alias Ockam.Message
  require Logger

  @echo_address {192, 168, 1, 2}

  @impl true
  def handle_message(message, state) do
    case Message.return_route(message) do
      [@echo_address] -> reply(message, state)
      _other -> request(message, state)
    end
  end

  defp request(message, state) do
    msg = %{
      onward_route: [@echo_address],
      return_route: [state.address],
      payload: Message.payload(message)
    }

    Router.route(msg)

    {:ok, state}
  end

  defp reply(message, state) do
    send(Message.payload(message), :ok)
    {:ok, state}
  end
end
