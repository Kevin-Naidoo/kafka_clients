import Config

config :brod,
  clients: [
    kafka_client: [
      endpoints: ['10.10.10.120': 9092],
      auto_start_producers: true,
      # The following :ssl and :sasl configs are not
      # required when running kafka locally unauthenticated
    ]
  ]
