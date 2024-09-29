# %% New and simpler config
from typing import Optional
from pydantic_settings import BaseSettings

class Config(BaseSettings):
    """
    Configuration settings for the trade_to_ohlc service

    Attributes:
        kafka_broker_address (str): The address of the Kafka broker.
        kafka_input_topic (str): The name of the Kafka topic where the trade data is read from.
        kafka_output_topic (str): The name of the Kafka topic where the OHLC data is written to.
        ohlc_window_seconds (int): The window size in seconds for OHLC aggregation.
        kafka_consumer_group (str): The group in which the output kafka topic is held
    """
# TODO: Hard code in the variables and print statements. Ensure kafka groups also work and are populated. Ensure timestamps work for historical

# For historical:

    kafka_broker_address: Optional[str] = 'localhost:19092'
    kafka_input_topic_name: str = 'trade_historical'
    kafka_output_topic_name: str = 'ohlc_historical'
    kafka_consumer_group: str = 'trade_to_ohlc_historical_group'
    ohlc_windows_seconds: int = '60'

# # For live

    # kafka_broker_address: Optional[str] = 'localhost:19092'
    # kafka_input_topic_name: str = 'trade'
    # kafka_output_topic_name: str = 'ohlc'
    # kafka_consumer_group: str = 'trade_to_ohlc_live'
    # ohlc_windows_seconds: int = '60'
    
config_trade_to_ohlc = Config()
print(f"\n--- Final Configuration ---")
print(f"Kafka Broker Address: {config_trade_to_ohlc.kafka_broker_address}")
print(f"Kafka Input Topic: {config_trade_to_ohlc.kafka_input_topic_name}")
print(f"Kafka Output Topic: {config_trade_to_ohlc.kafka_output_topic_name}")
print(f"Kafka Consumer Group: {config_trade_to_ohlc.kafka_consumer_group}")
print(f"OHLC Window Seconds: {config_trade_to_ohlc.ohlc_windows_seconds}")


# %% Old
# from dotenv import load_dotenv, find_dotenv
# from pydantic_settings import BaseSettings

# # Determine which env file to load based on whether the app is running in Docker
# env_path = '.env.docker' if os.getenv('RUNNING_IN_DOCKER') else '.env'
# load_dotenv(find_dotenv(env_path))

# # Print statements to debug environment variables loading
# print(f"Using environment file: {env_path}")
# print(f"KAFKA_BROKER_ADDRESS: {os.getenv('KAFKA_BROKER_ADDRESS')}")
# print(f"KAFKA_INPUT_TOPIC: {os.getenv('KAFKA_INPUT_TOPIC')}")
# print(f"KAFKA_OUTPUT_TOPIC: {os.getenv('KAFKA_OUTPUT_TOPIC')}")
# print(f"KAFKA_CONSUMER_GROUP: {os.getenv('KAFKA_CONSUMER_GROUP')}")

# # Old
# # class Config(BaseSettings):
# #     product_id: str = 'BTC/EUR'
# #     kafka_broker_address: str = os.getenv('KAFKA_BROKER_ADDRESS', 'default_broker_address')
# #     kafka_input_topic_name: str = 'trade'
# #     kafka_output_topic_name: str = 'ohlc'
# #     ohlc_windows_seconds: int = 10 # Units: seconds
    
# #     # int(os.getenv('OHLC_WINDOW_SECONDS', '10'))  # Default to 10s if not set - add back into trade_to_ohlc


# #     # Print to debug initialized settings
# #     def __post_init__(self):
# #         print(f"Configured product_id: {self.product_id}")
# #         print(f"Configured kafka_broker_address: {self.kafka_broker_address}")
# #         print(f"Configured kafka_topic_name: {self.kafka_input_topic_name}")
# #         print(f"Configured ohlc_windows_seconds: {self.ohlc_windows_seconds}") #- add back into trade_to_ohlc


# #New:
# class Config(BaseSettings):
#     kafka_broker_address: str = os.getenv('KAFKA_BROKER_ADDRESS')
    
#     # Determine the mode (live or historical) and set Kafka topics accordingly
#     live_or_historical: str = os.getenv('LIVE_OR_HISTORICAL', 'live')

#     # Set the correct input/output topics based on live or historical mode
#     kafka_input_topic_name: str = (
#         'trade_historical' if live_or_historical == 'historical' else 'trade'
#     )
#     kafka_output_topic_name: str = (
#         'ohlc_historical' if live_or_historical == 'historical' else 'ohlc'
#     )
#     kafka_consumer_group: str = (
#         'trade_to_ohlc_historical_consumer_group'
#         if live_or_historical == 'historical'
#         else 'trade_to_ohlc_consumer_group'
#     )
    
#     # Set OHLC window directly in the config, not from the environment
#     ohlc_windows_seconds: int = 60  # Set to 60 seconds as a default value

#     # Debug initialization
#     def __post_init__(self):
#         print(f"\nRunning in {self.live_or_historical.upper()} mode")
#         print(f"Configured kafka_broker_address: {self.kafka_broker_address}")
#         print(f"Configured kafka_input_topic_name: {self.kafka_input_topic_name}")
#         print(f"Configured kafka_output_topic_name: {self.kafka_output_topic_name}")
#         print(f"Configured kafka_consumer_group: {self.kafka_consumer_group}")
#         print(f"Configured ohlc_windows_seconds: {self.ohlc_windows_seconds}")

# # Creating a configuration instance
# config_trade_to_ohlc = Config()

# # Debugging: Print out the configuration to verify values
# print(f"\n--- Final Configuration ---")
# print(f"Kafka Broker Address: {config_trade_to_ohlc.kafka_broker_address}")
# print(f"Kafka Input Topic: {config_trade_to_ohlc.kafka_input_topic_name}")
# print(f"Kafka Output Topic: {config_trade_to_ohlc.kafka_output_topic_name}")
# print(f"Kafka Consumer Group: {config_trade_to_ohlc.kafka_consumer_group}")
# print(f"OHLC Window Seconds: {config_trade_to_ohlc.ohlc_windows_seconds}")