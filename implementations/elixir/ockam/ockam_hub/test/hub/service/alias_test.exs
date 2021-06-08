defmodule Test.Hub.Service.AliasTest do
  use ExUnit.Case

  alias Ockam.Hub.Service.Alias, as: AliasService
  alias Ockam.Hub.Service.Echo, as: EchoService
  alias Ockam.Router
  alias Test.Utils

  test "alias test" do
    {:ok, _worker, worker_address} =
      Test.Hub.Service.AliasTestWorker.start_link(address: "bdf373h28asdf")

    {:ok, _alias, _alias_address} = AliasService.start_link(address: "cdf373h48asdf")
    {:ok, _echo, _echo_address} = EchoService.start_link(address: "ddf373h47asdf")

    msg = %{onward_route: [worker_address], return_route: [], payload: Utils.pid_to_string()}
    Router.route(msg)

    assert_receive(:ok, 5_000)
  end
end

defmodule Test.Hub.Service.AliasTestWorker do
  @moduledoc false

  use Ockam.Worker

  alias Ockam.Message
  alias Ockam.Router
  alias Test.Utils

  @echo_address "ddf373h47asdf"
  @alias_address "cdf373h48asdf"

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
    message
    |> Message.payload()
    |> Map.get(:registration_payload)
    |> Utils.string_to_pid()
    |> send(:ok)

    {:ok, state}
  end
end
