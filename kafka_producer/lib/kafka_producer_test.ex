defmodule KafkaProducerTest do

  alias NimbleCSV.RFC4180, as: CSV
  require Logger


  # This is where you send them async
  def async_cast_process_charge(charge) do
    Task.async(fn ->
      :poolboy.transaction(
        :worker,
        fn pid ->
          try do
            GenServer.cast(pid, {:process_charge, charge})
            #process_charge(charge_payload())
          catch
            e, r ->
              IO.inspect("poolboy transaction caught error: #{inspect(e)}, #{inspect(r)}")
              :ok
          end
        end
      )
    end)
  end

  # For Testing
  def produce_records(iterations) do
    1..iterations
    |> Enum.each(fn _ -> async_cast_process_charge(charge_payload()) end)

    Logger.info("Done with #{iterations} messages published")
  end

  def process_charge(charge), do: GenServer.cast(__MODULE__, {:process_charge, charge})

  def read_and_produce(file_path) do
    file_path
    |> File.stream!()
    |> CSV.parse_stream(skip_headers: false)
    |> Stream.each(&produce_record/1)
    |> Stream.run()
  end

  defp produce_record(row) do
    # Convert the list (row) directly to JSON
    json_row = Jason.encode!(row)
    # Call publishing fn with JSON charge record
    async_cast_process_charge(row)
  end

  def charge_payload2 do
    payload = [
      "binary data",
      "0.0",
      "NULL",
      "911302350",
      "{\"balances\": [\"uncapped\"], \"service_type\": \"mobile_data\"}",
      "20240122--7e4f6812-b972-11ee-8d87-6cae8b6312a2",
      "Unlimited home 5G Premium",
      "{\"{\\\"qty\\\": 911302350, \\\"amount\\\": 0, \\\"balance_name\\\": \\\"uncapped\\\"}\"}",
      "0",
      "0.0",
      "20240122--7e4ef724-b972-11ee-a6ee-6cae8b6312a2",
      "NULL",
      "2024-01-22 23:55:56+02",
      "1",
      "{\"ratType\": \"NR\", \"ipv4_addr\": \"10.209.0.59\", \"requestor\": \"satest\", \"chargingId\": 375455463, \"service_id\": \"msisdn-27816699304\", \"resource_id\": \"imei-861935050829590\", \"oneTimeEvent\": false, \"userLocationinfo\": {\"nrLocation\": {\"tai\": {\"nid\": null, \"tac\": \"07F394\", \"plmnId\": {\"mcc\": \"655\", \"mnc\": \"73\"}}, \"ncgi\": {\"nid\": null, \"plmnId\": {\"mcc\": \"655\", \"mnc\": \"73\"}, \"nrCellId\": \"0d1414033\"}, \"ignoreNcgi\": null, \"globalGnbId\": null, \"geodeticInformation\": null, \"ueLocationTimestamp\": \"2024-01-20T18:01:52+02:00\", \"geographicalInformation\": null, \"ageOfLocationInformation\": null}, \"n3gaLocation\": null, \"eutraLocation\": null}}",
      "0",
      "5G Home X",
      "911302350",
      "6000",
      "vfs_udg_upf_01",
      "NULL",
      "{\"mode\": \"ONLINE\", \"sessionFailover\": \"FAILOVER_NOT_SUPPORTED\"}",
      "1.110000",
      "2024-01-22 12:06:29+02",
      "400",
      "imsi-655730000598400",
      "5G Home X",
      "{\"gl\": \"9500A030\", \"name\": \"VAT\", \"fixed\": 0, \"percentage\": 15, \"tax_amount\": 0.0, \"taxable_amount\": 0, \"tax_inclusive_amount\": 0.0}",
      "2024-01-23 00:06:29+02",
      "{\"model\": \"POST-PAID\", \"unit_info\": {\"triggers\": [{\"triggerType\": \"QUOTA_THRESHOLD\", \"triggerCategory\": \"IMMEDIATE_REPORT\"}, {\"triggerType\": \"QUOTA_EXHAUSTED\", \"triggerCategory\": \"IMMEDIATE_REPORT\"}, {\"triggerType\": \"QHT\", \"triggerCategory\": \"IMMEDIATE_REPORT\"}, {\"triggerType\": \"VALIDITY_TIME\", \"triggerCategory\": \"IMMEDIATE_REPORT\"}], \"validityTime\": 43200, \"quotaHoldingTime\": 0, \"timeQuotaThreshold\": 70, \"unitQuotaThreshold\": 80}, \"final_unit\": {\"finalUnitAction\": \"TERMINATE\"}, \"quota_calc\": \"default\", \"quota_rules\": [], \"neg_balances\": false, \"default_quota\": {\"name\": \"totalVolume\", \"quota\": 57982058496}, \"policy_states\": [\"High Speed\", \"RICA\", \"Device\", \"Payment\", \"PAYMENT_HOLD\"], \"vqt_percentage\": 0.4}",
      "100",
      "{\"time\": null, \"triggers\": [{\"timeLimit\": null, \"triggerType\": \"VALIDITY_TIME\", \"volumeLimit\": null, \"volumeLimit64\": null, \"maxNumberOfccc\": null, \"triggerCategory\": \"IMMEDIATE_REPORT\"}], \"totalVolume\": 911302350, \"uplinkVolume\": 192325086, \"downlinkVolume\": 718977264, \"eventTimeStamp\": null, \"triggerTimestamp\": \"2024-01-23T00:06:29+02:00\", \"serviceSpecificUnits\": null}",
      "0",
      "NULL",
      "2024-01-23 00:06:43+02",
      "2024-01-23 00:06:43+02",
      "a72e5f19-6245-4d24-8f69-3bf4bf215895"
    ]

    _ = Jason.encode!(payload)
  end

  # Example of one payload
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
        "qty" => 1_770_339,
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
            "quantity" => 1_770_339,
            "uncharged" => 0,
            "start_time" => "2023-11-20T13:18:59+02:00",
            "allocated_qty" => 0,
            "allocated_cost" => 0,
            "session_status" => 300
          }
        },
        "ipv4_addr" => "10.209.0.62",
        "requestor" => "satest",
        "chargingId" => 1_355_018_262,
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
        "default_quota" => %{"name" => "totalVolume", "quota" => 57_982_058_496},
        "policy_states" => [
          "High_Speed_30",
          "High_Speed_60",
          "High_Speed_Unlimited",
          "RICA",
          "Device",
          "Payment",
          "PAYMENT_HOLD"
        ],
        "vqt_percentage" => 0.4
      },
      type: 100,
      unit: %{
        "time" => nil,
        "triggers" => [
          %{
            "timeLimit" => nil,
            "triggerType" => "FINAL",
            "volumeLimit" => nil,
            "volumeLimit64" => nil,
            "maxNumberOfccc" => nil,
            "triggerCategory" => "IMMEDIATE_REPORT"
          }
        ],
        "totalVolume" => 1_770_339,
        "uplinkVolume" => 167_283,
        "downlinkVolume" => 1_603_056,
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

    _json_payload = Jason.encode!(payload)
  end
end
