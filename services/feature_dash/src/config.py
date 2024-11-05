from pydantic_settings import BaseSettings
from dotenv import load_dotenv
from typing import Optional
# # %%
# Comment out for prod
import os #comment out for prod as one loads from Makefiles
from pathlib import Path #comment out for prod as one loads from Makefiles

# Load the .env file from the kafka_to_feature_store microservice
dotenv_path = Path('../kafka_to_feature_store/.env')
load_dotenv(dotenv_path=dotenv_path)

# # %%
class Config(BaseSettings):
    
#     product_id: str = ["BTC/USD"]
    
#     # feature group our feature view reads data from
#     feature_group_name: str = 'ohlc_feature_group'
#     feature_group_version: int = 1

#     # feature view name and version
#     # feature_view_name: str = 
#     # feature_view_version: int = 

#     # required to authenticate with Hopsworks API
#     hopsworks_project_name: str = os.getenv('HOPSWORKS_PROJECT_NAME')
#     hopsworks_api_key: str = os.getenv('HOPSWORKS_API_KEY')

# config_feature_dash = Config()


    # historical
    kafka_broker_address: Optional[str] = 'localhost:19092'
    kafka_topic_name: str = 'ohlc_historical'
    kafka_consumer_group: str = 'trade_to_ohlc_historical_group'
    feature_group_name: str = 'ohlc_feature_group'
    feature_group_version: int = 2

    # by default we want our `kafka_to_feature_store` service to run in live mode
    live_or_historical: str = 'historical'

    # buffer size to store messages before writing to the feature store
    buffer_size: int = 150000

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
    # feature_group_name: str = 'ohlc_feature_group'
    # feature_group_version: int = 2

    # # by default we want our `kafka_to_feature_store` service to run in live mode
    # live_or_historical: str = 'historical'

    # # buffer size to store messages before writing to the feature store
    # buffer_size: int = 150000

    # # force save to feature store every n seconds
    # save_every_n_sec: int = 30

    # # whether to create a new consumer group or not
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