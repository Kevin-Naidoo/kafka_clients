defmodule KafkaProducer do
  use GenServer
  require Logger

  @moduledoc """
  Documentation for `KafkaProducer`.
  """

  #   # Storing configurations as persistent terms
  :persistent_term.put(CB_ENABLE_CB_CHARGE_SAVE, true)
  :persistent_term.put(KAFKA_CLUSTER_UP, true)
  :persistent_term.put(KAFKA_CLIENT_ID, :kafka_client)
  :persistent_term.put(KAFKA_TOPIC_ID, "charges")
  :persistent_term.put(KAFKA_PARTITION_ORDER, :hash)
  :persistent_term.put(KAFKA_KEY, "")

  def start_link(_arg) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @impl true
  def init(init_arg) do
    Logger.info("Starting Kafka Producer....")
    {:ok, init_arg}
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
      Logger.info(
        "Publishing using #{:persistent_term.get(KAFKA_CLIENT_ID)} to topic: #{:persistent_term.get(KAFKA_TOPIC_ID)}, partition:#{:persistent_term.get(KAFKA_PARTITION_ORDER)}, key: #{:persistent_term.get(KAFKA_KEY)}, payload: #{charge_map}"
      )

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


  def process_charge(charge), do: GenServer.cast(__MODULE__, {:process_charge, charge})
  def charge_payload do
    payload = %{
    id: "binary data",
    allocated_cost: "0.0",
    uncharged: nil,
    allocated_qty: "1770339",
    applies_to: %{
      "balances" => ["uncapped"],
      "service_type" => "mobile_data"
    },
    c_id: "20231120--794024f4-8797-11ee-8696-6cae8b6312a2",
    charged_for: "rainOne Level 9",
    charged_to: %{
      "qty" => 1770339,
      "amount" => 0,
      "balance_name" => "uncapped"
    },
    connection_surcharge: "0",
    cost: "0.0",
    cr_id: "20231120--793fce32-8797-11ee-a3b9-6cae8b6312a2",
    description: nil,
    end_time: "2023-11-20 13:25:11+02",
    item_seq_num: 1,
    meta: %{
      "ratType" => "NR",
      "consumed" => %{
        "6000" => %{
          "cost" => 0,
          "end_time" => "2023-11-20T13:25:11+02:00",
          "quantity" => 1770339,
          "uncharged" => 0,
          "start_time" => "2023-11-20T13:18:59+02:00",
          "allocated_qty" => 0,
          "allocated_cost" => 0,
          "session_status" => 300
        }
      },
      "ipv4_addr" => "10.209.0.62",
      "requestor" => "satest",
      "chargingId" => 1355018262,
      "service_id" => "msisdn-27816699388",
      "resource_id" => "imei-861935050829590",
      "oneTimeEvent" => false,
      "userLocationinfo" => %{
        "nrLocation" => %{
          "tai" => %{
            "nid" => nil,
            "tac" => "07F394",
            "plmnId" => %{"mcc" => "655", "mnc" => "73"}
          },
          "ncgi" => %{
            "nid" => nil,
            "plmnId" => %{"mcc" => "655", "mnc" => "73"},
            "nrCellId" => "0d1414033"
          },
          "ignoreNcgi" => nil,
          "globalGnbId" => nil,
          "geodeticInformation" => nil,
          "ueLocationTimestamp" => "2023-11-20T13:16:50+02:00",
          "geographicalInformation" => nil,
          "ageOfLocationInformation" => nil
        },
        "n3gaLocation" => nil,
        "eutraLocation" => nil
      }
    },
    post_surcharge: "0",
    primary_service: "5G_RAINONE_HOME_L9_PP",
    quantity: "1770339",
    rating_group: 6000,
    requestor_node_id: "vfs_udg_upf_01",
    requestor_service_id: nil,
    rg_meta: %{
      "mode" => "ONLINE",
      "sessionFailover" => "FAILOVER_NOT_SUPPORTED"
    },
    rounding_policy: "1.110000",
    start_time: "2023-11-20 13:18:59+02",
    status: 400,
    sub_id: "imsi-655730000853314",
    tariff_plan: "5G_RAINONE_HOME_L9_PP",
    taxes: %{
      "gl" => "9500A030",
      "name" => "VAT",
      "fixed" => 0,
      "percentage" => 15,
      "tax_amount" => 0.0,
      "taxable_amount" => 0,
      "tax_inclusive_amount" => 0.0
    },
    timestamp: "2023-11-20 13:25:13+02",
    tp_meta: %{
      "model" => "POST-PAID",
      "unit_info" => %{
        "triggers" => [
          %{"triggerType" => "RAT_CHANGE", "triggerCategory" => "IMMEDIATE_REPORT"},
          %{"triggerType" => "QUOTA_THRESHOLD", "triggerCategory" => "IMMEDIATE_REPORT"},
          %{"triggerType" => "QUOTA_EXHAUSTED", "triggerCategory" => "IMMEDIATE_REPORT"},
          %{"triggerType" => "QHT", "triggerCategory" => "IMMEDIATE_REPORT"},
          %{"triggerType" => "VALIDITY_TIME", "triggerCategory" => "IMMEDIATE_REPORT"}
        ],
        "validityTime" => 10800,
        "quotaHoldingTime" => 0,
        "timeQuotaThreshold" => 70,
        "unitQuotaThreshold" => 80
      },
      "final_unit" => %{"finalUnitAction" => "TERMINATE"},
      "quota_calc" => "default",
      "quota_rules" => [],
      "neg_balances" => false,
      "default_quota" => %{"name" => "totalVolume", "quota" => 57982058496},
      "policy_states" => ["High_Speed_30", "High_Speed_60", "High_Speed_Unlimited", "RICA", "Device", "Payment", "PAYMENT_HOLD"],
      "vqt_percentage" => 0.4
    },
    type: 100,
    unit: %{
      "time" => nil,
      "triggers" => [
        %{"timeLimit" => nil, "triggerType" => "FINAL", "volumeLimit" => nil, "volumeLimit64" => nil, "maxNumberOfccc" => nil, "triggerCategory" => "IMMEDIATE_REPORT"}
      ],
      "totalVolume" => 1770339,
      "uplinkVolume" => 167283,
      "downlinkVolume" => 1603056,
      "eventTimeStamp" => nil,
      "triggerTimestamp" => "2023-11-20T13:25:14+02:00",
      "serviceSpecificUnits" => nil
    },
    unit_rate: "0",
    error: nil,
    inserted_at: "2023-11-20 13:25:45+02",
    updated_at: "2023-11-20 13:25:45+02",
    service_id: "17b82eb1-09f8-4236-9dbc-d258bc898198"
  }

  json_payload = Jason.encode!(payload)

end

end
