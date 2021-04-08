defmodule Ockam.Hub.Service.Stream.Index do
  @moduledoc false

  use Ockam.Worker

  alias Ockam.Message
  alias Ockam.Router

  require Logger

  @impl true
  def handle_message(%{payload: payload} = message, state) do
    case decode_payload(payload) do
      {:save, %{client_id: client_id, mailbox_name: mailbox_name, index: index}} ->
        save_index({client_id, mailbox_name}, index, state)

      {:get, %{client_id: client_id, mailbox_name: mailbox_name}} ->
        index = get_index({client_id, mailbox_name}, state)
        reply_index(client_id, mailbox_name, index, Message.return_route(message), state)

      {:error, other} ->
        Logger.error("Unexpected message #{inspect(other)}")
        {:ok, state}
    end
  end

  def save_index(id, index, state) do
    indices = Map.get(state, :indices, %{})

    new_indices = Map.update(indices, id, index, fn previous -> max(previous, index) end)

    Map.put(state, :indices, new_indices)
  end

  def get_index(id, state) do
    state
    |> Map.get(:indices, %{})
    |> Map.get(id, 0)
  end

  def reply_index(client_id, mailbox_name, index, return_route, state) do
    Router.route(%{
      onward_route: return_route,
      return_route: [state.address],
      payload:
        :bare.encode(
          %{client_id: client_id, mailbox_name: mailbox_name, index: index},
          reply_scema()
        )
    })
  end

  def decode_payload(payload) do
    save_request = save_request_schema()
    get_request = get_request_schema()

    case :bare.decode(payload, request_schema()) do
      {:ok, {^save_request, data}, ""} -> {:save, data}
      {:ok, {^get_request, data}, ""} -> {:get, data}
      other -> {:error, other}
    end
  end

  def reply_scema() do
    {:struct, [client_id: :string, index: :i64, mailbox_name: :string]}
  end

  def save_request_schema() do
    {:struct, [client_id: :string, mailbox_name: :string, index: :i64]}
  end

  def get_request_schema() do
    {:struct, [client_id: :string, mailbox_name: :string]}
  end

  def request_schema() do
    {:union, [get_request_schema(), save_request_schema()]}
  end
end
