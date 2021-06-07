defmodule Test.Hub.Service.AliasTest do
  use ExUnit.Case

  alias Ockam.Hub.Service.Alias, as: AliasService
  alias Ockam.Hub.Service.Echo, as: EchoService

  alias Ockam.Router

  test "alias test" do
    {:ok, _worker, worker_address} =
      Test.Hub.Service.AliasTestWorker.start_link(address: {192, 168, 2, 1})

    {:ok, _alias, _alias_address} = AliasService.start_link(address: {192, 168, 2, 2})
    {:ok, _echo, _echo_address} = EchoService.start_link(address: {192, 168, 2, 3})

    msg = %{onward_route: [worker_address], return_route: [], payload: self()}
    Router.route(msg)

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

defmodule Test.Hub.Service.AliasTestWorker do
  @moduledoc false

  use Ockam.Worker

  alias Ockam.Message
  alias Ockam.Router

  @echo_address {192, 168, 2, 3}
  @alias_address {192, 168, 2, 2}

  @impl true
  def handle_message(message, state) do
    case Message.return_route(message) do
      [_address] -> process(message, state)
      [] -> registration(message, state)
      _other -> process(message, state)
    end
  end

  defp registration(message, state) do
    msg = %{
      onward_route: [@alias_address],
      return_route: [state.address],
      payload: %{registration_payload: Message.payload(message), forward_route: @echo_address}
    }

    Router.route(msg)

    new_state = Map.put(state, :status, :registered)
    {:ok, new_state}
  end

  defp process(message, state) when state.status == :registered do
    send(Message.payload(message)[:registration_payload], :ok)
    {:ok, state}
  end
end
