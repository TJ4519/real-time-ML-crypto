# Defining the function that takes int ohlc_candle_sticks dict and writes into hopsworks feature storet tables
import hopsworks
from config import config_kafka_to_hops

import pandas as pd

def push_data_to_feature_store(
        feature_group_name: str,
        feature_group_version: int,
        feature_data: dict,

)-> None:
    
    """
    Write the data of the incoming features, which are the ohlc candle sticks, into the feature store

    Args:

    feature_group_name (str) = Name of the featutre group  
    feature_store_version (int) = The version number 
    feature_data (dict) = The dict which includes the ohlc candle sticks
    
    """

    # Instantiate a connection and get the project feature store handler
    project = hopsworks.login(
        project = config.hopsworks_project_name,

        api_key_value= config.hopsworks_api_key,
    )

    feature_store = project.get_feature_store()

    # Create a new feature group to start inserting feature values.
    ohlc_feature_group = feature_store.get_or_create_feature_group(
        name= feature_group_name,
        version = feature_group_version,
        description = 'Open High Low Close candle sticks streamed and transformed from kraken websocketapi',
        primary_key = ["product_id","timestamp"],
        event_time = 'timestamp',
        online_enabled=True, #for real time predctions, the features are stored in the online store for inference 
    )

    # breakpoint()

    # Hopsworks feature stores don't use dicts, but are compatiable with pandas dataframes. Input data as a list of dictionaries not just dictionary - ValueError otherwise
    data = pd.DataFrame([feature_data])
    
    ohlc_feature_group.insert(data)





    
