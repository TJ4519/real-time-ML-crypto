from quixstreams import Application
from loguru import logger 
import json
import pandas as pd
import hopsworks

# import hopsworks_features
from config import config_kafka_to_hops


# Changed the python path C:\Users\tejas\OneDrive\Desktop\real-time-ml-trial3>set PYTHONPATH=C:\Users\tejas\OneDrive\Desktop\real-time-ml-trial3\services\kafka_to_feature_store
# Consequently, the imports work here without root directory as it does with src.config within trade_producer

#%%  Function for hopsworks

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

    # Hopsworks feature stores don't use dicts, but are compatiable with pandas dataframes. Input data as a list of dictionaries not just dictionary - ValueError otherwise
    data = pd.DataFrame([feature_data])
    
    ohlc_feature_group.insert(data)



#%% Function that uses the hopsworks_features function above-This needs to be out back into src.hopsworks_features, but testing it here to ensure it runs as I keep getting import errors

def kafka_to_feature_store(
        kafka_topic: str,
        kafka_broker_address: str,
        feature_group_name: str,
        feature_store_version: int,

) -> None:
    """

    This func sets up the consumer of data. This reads ohlc data and writes to the hopsworks feature store as defined by the 
    parameters inside the arguments feature_group_name and feature_store_version
    
    ARGS:
    kafka_topic (str): The topic it reads from 
    kafka_borker_address(str): Kafka broker address
    feature_group_name(str): Name of the featutre group
    feature_store_version (int): The version number 

    Return:
    None

    """

    app = Application(
        broker_address=kafka_broker_address,
        consumer_group="kafka_to_feature_store",
    )

    # input_topic = app.topic(name=kaka_input_topic, value_serializer='json')

    #Write data as a consumer
    with app.get_consumer() as consumer:
        consumer.subscribe(topics=[kafka_topic]) # topics are expected in list format so put kafka_topic argumet insdie []

        while True:
            msg = consumer.poll(1)
            
            if msg is None:
                continue
            
            elif msg.error():
            
                logger.error(('Kafka error;', msg.error()))
                
                # raise exception
                continue
            
            # If the message is not and and it' not erronious then the candle sticks are coming through
            else:

                # First step is parse the candle sticks into a dictionary with json.loads
                
                ohlc_candle_sticks = json.loads(msg.value().decode('utf-8'))

                push_data_to_feature_store(
                    feature_group_name=feature_group_name,
                    feature_group_version=feature_store_version,
                    feature_data= ohlc_candle_sticks,
                )


            
            value = msg.value()


            consumer.store_offsets(message=msg)

if __name__== '__main__':
    kafka_to_feature_store(
        kafka_topic=config_kafka_to_hops.kafka_topic_name,
        kafka_broker_address=config_kafka_to_hops.kafka_broker_address,
        feature_group_name=config_kafka_to_hops.feature_group_name,
        feature_store_version=config_kafka_to_hops.feature_store_version,
    )





        

    


