defmodule ConsumerTest do
  use ExUnit.Case
  doctest Consumer

  test "greets the world" do
    assert Consumer.hello() == :world

  # test "Consumer Test" do
  #   assert Consumer.Consumers.GroupSubscriberV2.startConsumer == {:ok, _PID}
  # end
  end
end
