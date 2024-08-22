defmodule ProducerTest do
  use ExUnit.Case
  doctest Producer

  test "Sample test" do
    assert Producer.hello() == :world
  end

  test "Brod Publish Message to topic" do
    assert Producer.publish(System.get_env("TOPIC"),:partition,"","greet the world")
  end
end
