defmodule KafkaProducerTest do
  use ExUnit.Case
  doctest KafkaProducer

  test "greets the world" do
    assert KafkaProducer.hello() == :world
  end
end
