# Defining the function that takes int ohlc_candle_sticks dict and writes into hopsworks feature storet tables

from typing import List
import pandas as pd
import hopsworks
from config import config_kafka_to_hops


def push_data_to_feature_store(
        feature_group_name: str,
        feature_group_version: int,
        data: List[dict],
        online_or_offline: str,

)-> None:
    
    """
    Write the data of the incoming features, which are the ohlc candle sticks, into the feature store tagging them with the corresponding {feature_group_name} and {feature_group_version}

    Args:

    feature_group_name (str) : Name of the featutre group  
    feature_store_version (int) : The version number 
    data (List[dict]) : The List of dicts, where each dict includes the ohlc candle sticks for a specific currency pair in the product_ids list
    online_or_offline (str) =  the string setting that dictates whether the offline or the online feature group store is used 

    
    """

    # Instantiate a connection and get the project feature store handler
    project = hopsworks.login(
        project = config_kafka_to_hops.hopsworks_project_name,

        api_key_value= config_kafka_to_hops.hopsworks_api_key,
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

    # Transform the data, which is a list of dicts into a dataframe. Why? Because Hopsworks feature stores don't use dicts, but are compatiable with pandas dataframes. Input data as a list of dictionaries not just dictionary - ValueError otherwise
    data = pd.DataFrame([data])
    
    # Write the data to the feature group
    ohlc_feature_group.insert(
        data,
        write_options=
        {
            "start_offline_materialization": True if online_or_offline == "offline" else False  
        }
        
    )





    
