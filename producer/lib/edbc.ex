defmodule Edbc do
  use GenServer

  @moduledoc """
  Documentation for `Edbc`.
  """
  def start_link(_) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  def init(_) do
    {:ok, _conn} = Postgrex.Notifications.start_link(name: :notifications, hostname: "10.10.10.168",
      username: "postgres",
      password: "postgres",
      database: "src_db")
    IO.puts("DB initialised...listening now")
      Postgrex.Notifications.listen(:notifications, "new_charge") |> IO.inspect

      {:ok, %{}}
  end

  def handle_info({:notification, _pid, _ref, _channel, payload}, state) do

    IO.inspect(payload)

    decoded_payload = Jason.decode!(payload)


    Producer.publish("charges", :hash, "", decoded_payload)

    {:noreply, state}

  end
end
