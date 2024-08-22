
defmodule Producer do
  @moduledoc """
  Documentation for `Producer`.
  """

  @doc """
  Publishes a message to the specified Kafka topic.

  ## Parameters

    - `topic` (String): The name of the Kafka topic where the message will be published.
    - `_partition` (atom): Partitioning strategy (not used in this example, but typically you might specify a partition number).
    - `key` (binary or term): The key used to partition the data. The `:hash` option in `:brod.produce_sync/5` will determine the partition based on this key.
    - `message` (binary): The message to be sent to the Kafka topic.

  ## Examples

      iex> Producer.publish("my_topic", :hash, "my_key", "Hello, Kafka!")
      :ok

  This function uses `:brod.produce_sync/5` to send a message synchronously to the Kafka topic. If successful, it returns `:ok`.

  """
def publish(topic, _partition, key, message) do
  :brod.produce_sync(:kafka_client, topic, :hash,key, message)
end
  @doc """
  Hello world.

  ## Examples

      iex> Producer.hello()
      :world

  """

  def hello do
    :world
  end
end
