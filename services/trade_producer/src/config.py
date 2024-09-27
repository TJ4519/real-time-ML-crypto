# %% Final config set-up
# Note: All vars are defined in the makefiles, .env and .envdocker are depricated
from typing import List, Optional
from pydantic_settings import BaseSettings
from pydantic import field_validator
from dotenv import load_dotenv
load_dotenv


class Config(BaseSettings):
    kafka_broker_address: Optional[str] = "localhost:19092"  # remove for prod
    kafka_topic_name: str = "trade_historical" # remove for prod 'trade_historical' for historical
    product_ids: List[str] = ["BTC/USD", "ETH/USD"] # remove for prod
    live_or_historical: str = "historical" # remove for prod 
    last_n_days: Optional[int] = 1 # remove for prod
    cache_dir_historical_data: Optional[str] = None # remove for prod

    @field_validator('live_or_historical')
    @classmethod
    def validate_live_or_historical(cls, value):
        assert value in {'live', 'historical'}, f'Invalid value for live_or_historical: {value}'
        return value

config_kraken_to_trade = Config()

# Final config check
print(f"Port address for Kafka Broker: {config_kraken_to_trade.kafka_broker_address}")
print(f"Product IDs: {config_kraken_to_trade.product_ids}")
print(f"Trades written into : {config_kraken_to_trade.kafka_topic_name} Kafka Topic")
if config_kraken_to_trade.live_or_historical == 'historical':
    print(f"Historical Data for {config_kraken_to_trade.last_n_days} days")



































# %% Old set of configs
# import os
# from dotenv import load_dotenv, find_dotenv
# from pydantic_settings import BaseSettings
# from typing import List,Optional
# # Determine which env file to load based on whether the app is running in Docker
# env_path = '.env.docker' if os.getenv('RUNNING_IN_DOCKER') else '.env'
# load_dotenv(find_dotenv(env_path))

# # Print statements to debug environment variables loading

# print("Using environment file:", env_path)
# print("KAFKA_BROKER_ADDRESS:", os.getenv('KAFKA_BROKER_ADDRESS'))

# # Old
# # class Config(BaseSettings):
# #     product_id: str = 'BTC/USD'
# #     #product_ids: List[str] = ['BTC/USD', 'ETH/USD']  # List of product IDs
# #     kafka_broker_address: str = os.getenv('KAFKA_BROKER_ADDRESS', 'default_broker_address')
# #     kafka_topic_name: str = 'trade'


# #     # Validate using pydantic
# #     live_or_historical:str = 'live'
# #     last_n_days: int = 3 # The int is refering to days
# #     # ohlc_windows_seconds: int = int(os.getenv('OHLC_WINDOW_SECONDS', '10'))  # Default to 10s if not set - add back into trade_to_ohlc

# #     # Print to debug initialized settings
# #     def __post_init__(self):
# #         print(f"Configured product_id: {self.product_id}")
# #         print(f"Configured kafka_broker_address: {self.kafka_broker_address}")
# #         print(f"Configured kafka_topic_name: {self.kafka_topic_name}")
# #         # print(f"Configured ohlc_windows_seconds: {self.ohlc_windows_seconds}") - add back into trade_to_ohlc

# #New
# # Utility function to convert comma-separated string to a list of strings
# def parse_product_ids(product_ids: str) -> List[str]:
#     return product_ids.split(',')

# class Config(BaseSettings):
#     product_ids: List[str] = parse_product_ids(os.getenv('PRODUCT_IDS', 'BTC/USD,ETH/EUR,ETH/USD'))
#     kafka_broker_address: str = os.getenv('KAFKA_BROKER_ADDRESS')
    
#     # Determine the Kafka topic based on live or historical mode
#     live_or_historical: str = os.getenv('LIVE_OR_HISTORICAL', 'historical')

#     # Set the correct topic based on live or historical
#     kafka_topic_name: str = 'trade_historical' if live_or_historical == 'historical' else 'trade'
    
#     # Only relevant for historical mode
#     last_n_days: Optional [int] = int(os.getenv('LAST_N_DAYS', 3)) if live_or_historical == 'historical' else None

#     # Debug initialization
#     def __post_init__(self):
#         print(f"\nRunning in {self.live_or_historical.upper()} mode")

#         # Print statements for live mode
#         if self.live_or_historical == 'live':
#             print(f"Configured product_id: {self.product_ids}")
#             print(f"Kafka Broker Address: {self.kafka_broker_address}")
#             print(f"Kafka Topic Name: {self.kafka_topic_name}")
        
#         # Print statements for historical mode
#         elif self.live_or_historical == 'historical':
#             print(f"Configured product_id: {self.product_ids}")
#             print(f"Kafka Broker Address: {self.kafka_broker_address}")
#             print(f"Kafka Topic Name: {self.kafka_topic_name}")
#             print(f"Fetching last {self.last_n_days} days of trades")

        

# # Creating a configuration instance
# config_kraken_to_trade = Config()

# #Extra debug print to verify configuration at runtime
# print(f"\n--- Final Configuration ---")
# print(f"product_ids: {config_kraken_to_trade.product_ids}")
# print(f"Kafka Broker Address: {config_kraken_to_trade.kafka_broker_address}")
# print(f"live_or_historical: {config_kraken_to_trade.live_or_historical}")
# print(f"Kafka Topic: {config_kraken_to_trade.kafka_topic_name}")
# if config_kraken_to_trade.live_or_historical == 'historical':
#     print(f"Last N Days: {config_kraken_to_trade.last_n_days}")