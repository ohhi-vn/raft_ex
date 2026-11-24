defmodule RaftEx.Domain.Conn do
  @moduledoc """
  A connection context for RaftEx plug pipelines, mirroring Plug.Conn semantics.
  """

  @type t :: %__MODULE__{
          type: :command | :rpc | nil,
          request: map(),
          resp: term(),
          assigns: map(),
          halted: boolean(),
          status: term(),
          private: map()
        }

  defstruct type: nil,
            request: %{},
            resp: nil,
            assigns: %{},
            halted: false,
            status: nil,
            private: %{}

  @spec assign(t(), term(), term()) :: t()
  def assign(%__MODULE__{} = conn, key, value) do
    %{conn | assigns: Map.put(conn.assigns, key, value)}
  end

  @spec halt(t()) :: t()
  def halt(%__MODULE__{} = conn) do
    %{conn | halted: true}
  end

  @spec put_resp(t(), term()) :: t()
  def put_resp(%__MODULE__{} = conn, resp) do
    %{conn | resp: resp}
  end

  @spec put_status(t(), term()) :: t()
  def put_status(%__MODULE__{} = conn, status) do
    %{conn | status: status}
  end

  @spec get_assign(t(), term(), term()) :: term()
  def get_assign(%__MODULE__{} = conn, key, default \\ nil) do
    Map.get(conn.assigns, key, default)
  end
end
