import os
from dotenv import load_dotenv, find_dotenv
from pydantic_settings import BaseSettings

# Determine which env file to load based on whether the app is running in Docker
env_path = '.env.docker' if os.getenv('RUNNING_IN_DOCKER') else '.env'
load_dotenv(find_dotenv(env_path))

# Print statements to debug environment variables loading
# print("Using environment file:", env_path)
# print("KAFKA_BROKER_ADDRESS:", os.getenv('KAFKA_BROKER_ADDRESS'))
# print("OHLC_WINDOW_SECONDS:", os.getenv('OHLC_WINDOW_SECONDS')) - add back into trade_to_ohlc

class Config(BaseSettings):
    product_id: str = 'BTC/EUR'
    kafka_broker_address: str = os.getenv('KAFKA_BROKER_ADDRESS', 'default_broker_address')
    kafka_input_topic_name: str = 'trade'
    kafka_output_topic_name: str = 'ohlc'
    ohlc_windows_seconds: int = 10 # Units: seconds
    
    # int(os.getenv('OHLC_WINDOW_SECONDS', '10'))  # Default to 10s if not set - add back into trade_to_ohlc


    # Print to debug initialized settings
    def __post_init__(self):
        print(f"Configured product_id: {self.product_id}")
        print(f"Configured kafka_broker_address: {self.kafka_broker_address}")
        print(f"Configured kafka_topic_name: {self.kafka_input_topic_name}")
        print(f"Configured ohlc_windows_seconds: {self.ohlc_windows_seconds}") #- add back into trade_to_ohlc

# Creating a configuration instance
config_trade_to_ohlc = Config()

# Debugging: Print out the configuration to verify values
print(f"Configured product_id: {config_trade_to_ohlc.product_id}")
print(f"Configured kafka_broker_address: {config_trade_to_ohlc.kafka_broker_address}")
print(f"Configured kafka_topic_name: {config_trade_to_ohlc.kafka_input_topic_name}")
print(f"Configured kafka_topic_name: {config_trade_to_ohlc.kafka_output_topic_name}")
print(f"Configured ohlc_windows_seconds: {config_trade_to_ohlc.ohlc_windows_seconds}")
