defmodule Consumers.GroupSubscriberV2 do

  @behaviour :brod_group_subscriber_v2

  def init(_arg, _arg2) do
    {:ok, []}
  end

  def handle_message(message, _state) do
    IO.inspect(message, label: "message")
    {:ok, :commit, []}
  end

  def startConsumer do

    group_config = [
      offset_commit_policy: :commit_to_kafka_v2,
      offset_commit_interval_seconds: 5,
      rejoin_delay_seconds: 2,
      reconnect_cool_down_seconds: 10
    ]

    config = %{
      client: :kafka_client,
      group_id: "test-group",
      topics: ["elixir"],
      cb_module: __MODULE__,
      group_config: group_config,
      consumer_config: [begin_offset: :latest]
    }
  {:ok, _pid} = :brod.start_link_group_subscriber_v2(config)
  end



end
