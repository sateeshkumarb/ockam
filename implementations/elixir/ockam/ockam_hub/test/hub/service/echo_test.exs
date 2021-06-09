defmodule Test.Hub.Service.EchoTest do
  use ExUnit.Case

  alias Ockam.Hub.Service.Echo, as: EchoService
  alias Ockam.Router
  alias Test.Utils

  test "echo test" do
    {:ok, _worker, worker_address} =
      Test.Hub.Service.EchoTestWorker.start_link(address: "sdfs73hd8asdf")

    {:ok, _echo, _echo_address} = EchoService.start_link(address: "adfs73h28asdf")

    msg = %{
      onward_route: [worker_address],
      return_route: [],
      payload: Utils.pid_to_string(self())
    }

    Router.route(msg)

    assert_receive(:ok, 5_000)
  end
end

defmodule Test.Hub.Service.EchoTestWorker do
  @moduledoc false

  use Ockam.Worker

  alias Ockam.Message
  alias Test.Utils

  require Logger

  @echo_address "adfs73h28asdf"

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
    message
    |> Message.payload()
    |> Utils.string_to_pid()
    |> send(:ok)

    {:ok, state}
  end
end
