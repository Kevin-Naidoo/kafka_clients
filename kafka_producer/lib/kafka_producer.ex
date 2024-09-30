defmodule KafkaProducerWorker do
  use GenServer
  require Logger
 # alias NimbleCSV.RFC4180, as: CSV

  @moduledoc """
  Documentation for `KafkaProducer`.
  """

  #   # Storing configurations as persistent terms


  def start_link(_) do
    GenServer.start_link(__MODULE__, nil)
  end

  @impl true
  def init(init_arg) do
    Logger.info("Starting Kafka Producer....")
    start_terms()
    Logger.info("Load Configs to persistent_terms.....")
    {:ok, init_arg}
  end

  def start_terms do
    :persistent_term.put(CB_ENABLE_CB_CHARGE_SAVE, true)
    :persistent_term.put(KAFKA_CLUSTER_UP, true)
    :persistent_term.put(KAFKA_CLIENT_ID, :kafka_client)
    :persistent_term.put(KAFKA_TOPIC_ID, "charges")
    :persistent_term.put(KAFKA_PARTITION_ORDER, :hash)
    :persistent_term.put(KAFKA_KEY, "")
  end




  @impl true
  def handle_cast({:process_charge, charge}, state) do
    # add logic for when to save this charge?
    save_this_charge = true

    # Get charge map from process charge line 156 - 162
    charge_map = charge

    if :persistent_term.get(CB_ENABLE_CB_CHARGE_SAVE, true) and
         (:persistent_term.get(KAFKA_CLUSTER_UP, false) and
            save_this_charge) do
      # insert logic of retries here
      # Logger.info(
      #   "Publishing using #{:persistent_term.get(KAFKA_CLIENT_ID)} to topic: #{:persistent_term.get(KAFKA_TOPIC_ID)}, partition:#{:persistent_term.get(KAFKA_PARTITION_ORDER)}, key: #{:persistent_term.get(KAFKA_KEY)}, payload: #{charge_map}"
      # )

      :brod.produce_sync(
        :persistent_term.get(KAFKA_CLIENT_ID),
        :persistent_term.get(KAFKA_TOPIC_ID),
        :persistent_term.get(KAFKA_PARTITION_ORDER),
        :persistent_term.get(KAFKA_KEY),
        charge_map
      )
    end

    {:noreply, state}
  end
end
