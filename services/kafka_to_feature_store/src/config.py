import os
from dotenv import load_dotenv, find_dotenv
from pydantic_settings import BaseSettings

# load_dotenv(find_dotenv)


# from pydantic_settings import BaseSettings

# class Config(BaseSettings):
#     # product_id:
#     # kafka_broker_address:
#     # kafka_topic_name:
#     # ohlc_window_seconds: 
#     hopsworks_project_name : str = os.environ['HOPSWORKS_PROJECT_NAME']
#     hopsworks_api_key : str = os.environ['HOPSWORKS_API_KEY']

# config = Config()

# %% 

# Determine which env file to load based on whether the app is running in Docker
env_path = '.env.docker' if os.getenv('RUNNING_IN_DOCKER') else '.env'
load_dotenv(find_dotenv(env_path))


class Config(BaseSettings):
    kafka_broker_address: str = os.getenv('KAFKA_BROKER_ADDRESS', 'default_broker_address')
    kafka_topic_name: str = 'ohlc'
    hopsworks_project_name: str = os.getenv('HOPSWORKS_PROJECT_NAME')
    hopsworks_api_key: str = os.getenv('HOPSWORKS_API_KEY')
    feature_group_name: str = 'ohlc_feature_group'
    feature_store_version: int = 1  # Default version

    # Print to debug initialized settings
    def __post_init__(self):
        print(f"Configured kafka_broker_address: {self.kafka_broker_address}")
        print(f"Configured kafka_topic_name: {self.kafka_topic_name}")
        print(f"Configured hopsworks_project_name: {self.hopsworks_project_name}")
        print(f"Configured hopsworks_api_key: {self.hopsworks_api_key}")
        print(f"Configured feature_group_name: {self.feature_group_name}")
        print(f"Configured feature_store_version: {self.feature_store_version}")

# Creating a configuration instance
config_kafka_to_hops = Config()

# Print statements to debug environment variables loading
print("Using environment file:", env_path)
print("KAFKA_BROKER_ADDRESS:", config_kafka_to_hops.kafka_broker_address)
print("HOPSWORKS_API_KEY:", config_kafka_to_hops.hopsworks_api_key)
print("HOPSWORKS_PROJECT_NAME:", config_kafka_to_hops.hopsworks_project_name)
print("feature_group_name:", config_kafka_to_hops.feature_group_name)