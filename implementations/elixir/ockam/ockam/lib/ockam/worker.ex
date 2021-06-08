defmodule Ockam.Worker do
  @moduledoc false

  @callback setup(options :: Keyword.t(), initial_state :: map()) ::
              {:ok, state :: map()} | {:error, reason :: any()}

  @callback handle_message(message :: any(), state :: map()) ::
              {:ok, state :: map()}
              | {:error, reason :: any()}
              | {:stop, reason :: any(), state :: map()}

  defmacro __using__(_options) do
    quote do
      # use GenServer, makes this module a GenServer.
      #
      # Among other things, it adds the `child_spec/1` function which returns a
      # specification to start this module under a supervisor. When this module is
      # added to a supervisor, the supervisor calls child_spec to figure out the
      # specification that should be used.
      #
      # See the "Child specification" section in the `Supervisor` module for more
      # detailed information.
      #
      # The `@doc` annotation immediately preceding `use GenServer` below
      # is attached to the generated `child_spec/1` function. Since we don't
      # want `child_spec/1` in our Transport module docs, `@doc false` is set here.

      @doc false
      use GenServer

      @behaviour Ockam.Worker

      ## Ignore match errors in handle_info when checking a result of handle_message
      ## handle_message definition may not return {:error, ...} and it shouldn't fail because of that
      @dialyzer {:no_match, handle_info: 2}

      alias Ockam.Node
      alias Ockam.Router
      alias Ockam.Telemetry

      @doc false
      def create(options) when is_list(options) do
        options = Keyword.put_new_lazy(options, :address, &Node.get_random_unregistered_address/0)

        case Node.start_supervised(__MODULE__, options) do
          {:ok, pid, worker} ->
            :sys.get_state(pid)
            {:ok, worker}

          error ->
            error
        end
      end

      @doc false
      def start_link(options) when is_list(options) do
        with {:ok, address} <- get_from_options(:address, options),
             {:ok, pid} <- start(address, options) do
          {:ok, pid, address}
        end
      end

      defp start(address, options) do
        GenServer.start_link(__MODULE__, options, name: {:via, Node.process_registry(), address})
      end

      @doc false
      @impl true
      def init(options) do
        {:ok, options, {:continue, :post_init}}
      end

      @doc false
      @impl true
      def handle_info(message, state) do
        metadata = %{message: message}
        start_time = Telemetry.emit_start_event([__MODULE__, :handle_message], metadata: metadata)

        return_value = handle_message(message, state)

        metadata = Map.put(metadata, :return_value, return_value)
        Telemetry.emit_stop_event([__MODULE__, :handle_message], start_time, metadata: metadata)

        case return_value do
          {:ok, returned_state} ->
            {:noreply, returned_state}

          {:stop, reason, returned_state} ->
            {:stop, reason, returned_state}

          {:error, _reason} ->
            ## TODO: log error
            {:noreply, state}
        end
      end

      @doc false
      @impl true
      def handle_continue(:post_init, options) do
        metadata = %{options: options}
        start_time = Telemetry.emit_start_event([__MODULE__, :init], metadata: metadata)

        with {:ok, address} <- get_from_options(:address, options) do
          return_value = setup(options, %{address: address, module: __MODULE__})

          metadata = Map.put(metadata, :return_value, return_value)
          Telemetry.emit_stop_event([__MODULE__, :init], start_time, metadata: metadata)
          {:ok, state} = return_value
          {:noreply, state}
        end
      end

      @doc false
      def get_from_options(key, options) do
        case Keyword.get(options, key) do
          nil -> {:error, {:option_is_nil, key}}
          value -> {:ok, value}
        end
      end

      @doc false
      def setup(_options, state), do: {:ok, state}

      defoverridable setup: 2
    end
  end
end
