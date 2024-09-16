defmodule KafkaProducer.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  defp poolboy_config do
    [
      name: {:local, :worker},
      worker_module: KafkaProducerWorker,
      size: 25,
      max_overflow: 2
    ]
  end

  @impl true
  def start(_type, _args) do
    children = [
      # Starts a worker by calling: KafkaProducer.Worker.start_link(arg)
      # {KafkaProducer.Worker, arg}
      #KafkaProducer,
      #:poolboy.child_spec(:worker, poolboy_config())
      :poolboy.child_spec(:worker, poolboy_config())
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: KafkaProducer.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
