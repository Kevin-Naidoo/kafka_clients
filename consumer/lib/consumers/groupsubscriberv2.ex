defmodule Consumers.GroupSubscriberV2 do

  alias NimbleCSV.RFC4180, as: CSV

  @behaviour :brod_group_subscriber_v2




  def init(_arg, _arg2) do
    {:ok, []}
  end

  def handle_message(message, _state) do
    #Extract JSON charge record
    message_set = elem(message,4) # gives list of tuples
    #IO.inspect(message_set)
    strip_message(message_set)
    #IO.inspect(list)
   # |> jason_decode()
    #write_to_csv(list)
    {:ok, :commit, []}
  end

  def strip_message(list) do
    Enum.each(list, fn x -> (write_to_csv(Jason.decode(elem(x,3)))) end)
  end

  def jason_decode(list) do
    Enum.map(list, fn x ->(x) end)
  end

  def write_to_csv(jason_decoded) do
    {:ok, x} = jason_decoded
    IO.inspect(x)
    sanitized_row = Enum.map(x, fn
      nil -> "NULL"   # Convert nil values to "NULL"
      value -> to_string(value) # Convert other types to strings
    end)
    File.open(:persistent_term.get(FILE_NAME), [:append], fn file ->
      IO.binwrite(file, CSV.dump_to_iodata([sanitized_row]))
    end)
  end

  def update_file_name do
    timestamp = NaiveDateTime.utc_now() |> NaiveDateTime.to_string() |> String.replace(~r/[:\-]/, "_")

  # Create the file path with the timestamp in the filename
  file_path = "#{timestamp}_output.csv"
  :persistent_term.put(FILE_NAME, file_path)

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
      topics: ["charges"],
      cb_module: __MODULE__,
      group_config: group_config,
      consumer_config: [begin_offset: :latest]
    }
    :persistent_term.put(FILE_NAME, "output.csv")
    update_file_name()
  {:ok, _pid} = :brod.start_link_group_subscriber_v2(config)
  end



end
