import os
from dotenv import load_dotenv, find_dotenv
from pydantic_settings import BaseSettings
from pydantic import field_validator
from typing import Optional
from dotenv import load_dotenv, find_dotenv  # Import dotenv functions #NOTE:REMOVE FOR PROD JUST HERE FOR MANUAL LOADING OF VARS

load_dotenv(find_dotenv())  # Ensure this is called to load .env

class Config(BaseSettings):

    #FOR Docker:
    # kafka_broker_address: Optional[str] = None
    # kafka_topic_name: str 
    # kafka_consumer_group: str 
    # feature_group_name: str 
    # feature_group_version: int

    # # by default we want our `kafka_to_feature_store` service to run in live mode
    # live_or_historical: str = 'live'

    # # buffer size to store messages before writing to the feature store
    # buffer_size: int

    # # force save to feature store every n seconds
    # save_every_n_sec: int = 600

    # # whether to create a new consumer group or not
    # create_new_consumer_group: bool = False
    
    # # required to authenticate with Hopsworks API
    # hopsworks_project_name: str
    # hopsworks_api_key: str

    # # historical
    kafka_broker_address: Optional[str] = 'localhost:19092'
    kafka_topic_name: str = 'ohlc_historical'
    kafka_consumer_group: str = 'trade_to_ohlc_historical_group'
    feature_group_name: str = 'ohlc_feature_group_v5' #from v4
    feature_group_version: int = 1

    # by default we want our `kafka_to_feature_store` service to run in live mode
    live_or_historical: str = 'historical'

    # buffer size to store messages before writing to the feature store
    buffer_size: int = 200_000

    # force save to feature store every n seconds
    save_every_n_sec: int = 30

    # whether to create a new consumer group or not
    create_new_consumer_group: bool = False
    
    # required to authenticate with Hopsworks API
    hopsworks_project_name: str = os.getenv('HOPSWORKS_PROJECT_NAME')
    hopsworks_api_key: str = os.getenv('HOPSWORKS_API_KEY')


    ######################################################

    # live
    # kafka_broker_address: Optional[str] = 'localhost:19092'
    # kafka_topic_name: str = 'ohlc'
    # kafka_consumer_group: str = 'trade_to_ohlc_live'
    # # feature_group_name: str = 'ohlc_feature_group' 
    # feature_group_name: str ='ohlc_feature_group_v3'
    # feature_group_version: int = 1 #changed from 4

    # # by default we want our `kafka_to_feature_store` service to run in live mode
    # live_or_historical: str = 'live'

    # # buffer size to store messages before writing to the feature store
    # buffer_size: int = 20

    # # force save to feature store every n seconds
    # save_every_n_sec: int = 30

    # # whether to create a new consumer group or not, default to false--flip to true if some ingestion failure to hopsworks causes not  all the data in the topics to land into hopsowkr
    # create_new_consumer_group: bool = False
    
    # # required to authenticate with Hopsworks API
    # hopsworks_project_name: str = os.getenv('HOPSWORKS_PROJECT_NAME')

    # hopsworks_api_key: str = os.getenv('HOPSWORKS_API_KEY')

    @field_validator('live_or_historical')
    @classmethod
    def validate_live_or_historical(cls, value):
        assert value in {
            'live',
            'historical',
        }, f'Invalid value for live_or_historical: {value}'
        return value


config_kafka_to_hops = Config()

print("KAFKA_BROKER_ADDRESS:", config_kafka_to_hops.kafka_broker_address)
print("HOPSWORKS_PROJECT_NAME:", config_kafka_to_hops.hopsworks_project_name)
print("feature_group_name:", config_kafka_to_hops.feature_group_name)
print("feature_group_name:", config_kafka_to_hops.live_or_historical)


























# # load_dotenv(find_dotenv)


# # from pydantic_settings import BaseSettings

# # class Config(BaseSettings):
# #     # product_id:
# #     # kafka_broker_address:
# #     # kafka_topic_name:
# #     # ohlc_window_seconds: 
# #     hopsworks_project_name : str = os.environ['HOPSWORKS_PROJECT_NAME']
# #     hopsworks_api_key : str = os.environ['HOPSWORKS_API_KEY']

# # config = Config()

# # %% 

# # Determine which env file to load based on whether the app is running in Docker
# env_path = '.env.docker' if os.getenv('RUNNING_IN_DOCKER') else '.env'
# load_dotenv(find_dotenv(env_path))


# class Config(BaseSettings):
#     kafka_broker_address: str = os.getenv('KAFKA_BROKER_ADDRESS', 'default_broker_address')
#     kafka_topic_name: str = 'ohlc'
#     hopsworks_project_name: str = os.getenv('HOPSWORKS_PROJECT_NAME')

#     live_or_historical:str = 'historical'
#     hopsworks_api_key: str = os.getenv('HOPSWORKS_API_KEY')
#     feature_group_name: str = 'ohlc_feature_group'
#     feature_store_version: int = 1  # Default version

#     buffer_size: int = 150000
#     save_every_n_sec: int = 30 
#     create_new_consumer_group: bool = True

#     # Print to debug initialized settings
#     def __post_init__(self):
#         print(f"Configured kafka_broker_address: {self.kafka_broker_address}")
#         print(f"Configured kafka_topic_name: {self.kafka_topic_name}")
#         print(f"Configured hopsworks_project_name: {self.hopsworks_project_name}")
#         print(f"Configured hopsworks_api_key: {self.hopsworks_api_key}")
#         print(f"Configured feature_group_name: {self.feature_group_name}")
#         print(f"Configured feature_store_version: {self.feature_store_version}")

# # Creating a configuration instance
# config_kafka_to_hops = Config()

# # Print statements to debug environment variables loading
# print("Using environment file:", env_path)
# print("KAFKA_BROKER_ADDRESS:", config_kafka_to_hops.kafka_broker_address)
# print("HOPSWORKS_API_KEY:", config_kafka_to_hops.hopsworks_api_key)
# print("HOPSWORKS_PROJECT_NAME:", config_kafka_to_hops.hopsworks_project_name)
# print("feature_group_name:", config_kafka_to_hops.feature_group_name)
# print("feature_group_name:", config_kafka_to_hops.live_or_historical)