from quixstreams import Application
from loguru import logger 
import json
import pandas as pd
import hopsworks

# import hopsworks_features
from hopsworks_features import push_data_to_feature_store
from config import config_kafka_to_hops


# Changed the python path C:\Users\tejas\OneDrive\Desktop\real-time-ml-trial3>set PYTHONPATH=C:\Users\tejas\OneDrive\Desktop\real-time-ml-trial3\services\kafka_to_feature_store
# Consequently, the imports work here without root directory as it does with src.config within trade_producer


# %% The func for current timestamp
def get_current_utc_sec() -> int:
    """
    Returns the current UTC time expressed in seconds since the epoch.
    
    """
    from datetime import datetime, timezone
    return int(datetime.now(timezone.utc).timestamp())

#%% Function that uses the hopsworks_features function above-This needs to be out back into src.hopsworks_features, but testing it here to ensure it runs as I keep getting import errors
from typing import Optional
def kafka_to_feature_store(
        kafka_topic: str,
        kafka_broker_address: str,
        feature_group_name: str,
        feature_store_version: int,
        buffer_size: int,
        live_or_historical: str, 
        save_every_n_sec: int,
        create_new_consumer_group: bool,


) -> None:
    """

    This func sets up the consumer of data. This reads ohlc data and writes to the hopsworks feature store as defined by the 
    parameters inside the arguments feature_group_name and feature_store_version
    
    kafka_topic (str): The Kafka topic from which the data is read i.e this is the input topic, which ought to be the ohlc
    kafka_broker_address (str): The address of the Kafka broker 
    kafka_consumer_group (str): The Kafka consumer group we use for reading messages
    feature_group_name (str): The name of the feature group to write to
    feature_group_version (int): The version of the feature group
    buffer_size (int): The number of messages to read from Kafka before writing to the feature store
    live_or_historical (str): Whether we are saving live data to the Feature or historical data.Livde data goes to the online feature store, whilst historical data goes to the offline feature store
    save_every_n_sec (int): In the event where data streaming is rate limited, this defines the maximum number of seconds to wait before writing the data to the feature store. Additional conditional check along with buffer_size
    create_new_consumer_group (bool): Whether to create a new consumer group or not

    Return:
    None

    """


    # New consumer group generation logic
    if create_new_consumer_group:
        # generate a unique consumer group name using uuid
        import uuid
        kafka_consumer_group = 'ohlc_historical_consumer_group_' + str(uuid.uuid4())
        logger.debug(f'New consumer group name: {kafka_consumer_group}')
    
    # Instantiate Kafka
    app = Application(
        broker_address=kafka_broker_address,
        consumer_group="kafka_to_feature_store",
        auto_offset_reset="earliest" if live_or_historical == 'historical' else "latest",
    )
    

    topic = app.topic(name=kafka_topic, value_serializer='json')
    
    # Establish the UTC time and save it
    last_saved_to_feature_store_ts = get_current_utc_sec()
    
    
    buffer = []

    

    # Write data as a consumer using a polling loop
    with app.get_consumer() as consumer:
        consumer.subscribe(topics=[kafka_topic]) # topics are expected in list format so put kafka_topic argumet insdie []

        while True:
            msg = consumer.poll(1)
            sec_since_last_saved = ( get_current_utc_sec() - last_saved_to_feature_store_ts)

            # cheeky bug check to see if there is any error
            if (msg is not None) and msg.error():
                logger.error('Kafka error', msg.error())
                continue

            # If there is no error streaming in, but the websocket api is rate limited, but the time since the last message has arrived still hasn't exceeded the customisable save_every_n_sec window....then perhaps no cause for concern and wait it out with a debugger msg
            elif (msg is None) and (sec_since_last_saved < save_every_n_sec):
                logger.debug('No new messages streaming in from the input topic of ohlc')
                logger.debug(f'Latest instance of saving to feature store was as {sec_since_last_saved} seconds ago (limit={save_every_n_sec})')
                continue
            
            # If there isn't an error, and we're streaming messages within the alloted so called buffering time window, then obs write the incoming data in batches
            else:
                # below creates the candle sticks objects that is serialised in nature, and is written into a buffer/batch
                if msg is not None:
                    ohlc_candle_sticks = json.loads(msg.value().decode('utf-8'))
                    buffer.append(ohlc_candle_sticks)
                    logger.debug(f'Message {ohlc_candle_sticks} added to buffer. Buffer size={len(buffer)}')
                # Once the serialised ohlc data, which is appended into the buffer exceeds the buffer size or the customisable time window for no error + no messages as per save_every_n_sec, then push to feature store
                if (len(buffer) >= buffer_size) or (sec_since_last_saved >= save_every_n_sec):
                    if len(buffer) > 0:
                        
                        try:
                        
                            push_data_to_feature_store(
                                
                                # logic for writing into offline or online
                                online_or_offline='online' if live_or_historical == 'live' else 'offline',
                                
                                
                                feature_group_name=feature_group_name,
                                feature_group_version=feature_store_version,
                                data=buffer,
                                
                            )
                            
                            logger.debug('Data pushed to the feature store')
                        
                        except Exception as e:
                            
                            logger.error(f'Failed to push data to the feature store: {e}')
                            continue

                        # Clear buffer after writing to feature store
                        buffer = []
                        last_saved_to_feature_store_ts = get_current_utc_sec()

            

if __name__== '__main__':
    
    logger.debug(config_kafka_to_hops.model_dump())

    try:
    
        kafka_to_feature_store(
        kafka_topic=config_kafka_to_hops.kafka_topic_name,
        kafka_broker_address=config_kafka_to_hops.kafka_broker_address,
        feature_group_name=config_kafka_to_hops.feature_group_name,
        feature_store_version=config_kafka_to_hops.feature_store_version,
        live_or_historical = config_kafka_to_hops.live_or_historical,

        buffer_size= config_kafka_to_hops.buffer_size,
        save_every_n_sec= config_kafka_to_hops.save_every_n_sec,
        create_new_consumer_group= config_kafka_to_hops.create_new_consumer_group,
    )
    
    except KeyboardInterrupt:
        logger.info('Exiting neatly!')





        

    


