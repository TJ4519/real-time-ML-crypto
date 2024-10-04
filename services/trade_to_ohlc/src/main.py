
from quixstreams import Application
from datetime import timedelta
from loguru import logger
from typing import Any, List, Optional, Tuple

from config import config_trade_to_ohlc #from src.config import config_trade_to_ohlc doesn't work like it does for main.trade_producer. Root library might be in this service

def custom_ts_extractor(
        value: Any,
        headers: Optional[List[Tuple[str, bytes]]],
        timestamp: float,
        timestamp_type,  
                            ) -> int:
    """
    By default, Quix Streams uses Kafka message timestamps to determine the time of the event. Consequently, 
    For historical tumbling windows, there was a bug; assigning processing time where there should be event times.
    
        
    This functionm taken from Quixstreams for streaming data into aggregated windows example extracts
    
    instead of Kafka timestamp. the timestamp from the message payload itself and not the kafka timestamp. 
    Link: https://quix.io/docs/quix-streams/windowing.html#updating-window-definitions

    Need to use the `timestamp_ms` field from the message payload
    
    Args:

    Return:
     value(int): timestamp

    """
    return value['timestamp_ms'] #in milliseconds
    #return value.timestamp_ms


def trade_to_ohlc(
        kafka_input_topic: str,
        kafka_output_topic: str,
        kafka_broker_address: str,
        kafka_consumer_group: str,
        ohlc_windows_seconds: int,
)-> None:
    """
    Takes the stream of trading information from the input kafka topic address, slices the data into a window measured in seconds, to create candles for open, close, high and low
    The "candlestick" data is stored in another output for feature storage

    NOTE:
    The Quixstreams library might expect timestamps to be in a consistent format, and the aggregation function might not be handling the different formats properly. Specifically:

    ISO 8601 format: This format is a string representation of a date and time.
    Unix timestamp: This is an integer representation of seconds (or milliseconds, depending on implementation) since January 1, 1970 (the Unix epoch).



    args:
        kaka_input_topic: str : Kafka topic where the stream of trade data is being stored from websocket
        kaka_output_topic: str: Kafka topic where the open,high,low,close data is stored as part of feature pipeline
        kaka_broker_address: str: Kafka broker address..duh
        window_seconds: the time interval over which the data streams is sliced to determine candle sticks
    return:
        none

    """
    # Note on adding consumer group:
    # By specifying consumer_grpup, which is a parameter in kafka topcics, scaling can be achieved. Negating the conumer_group, prevents the consumers of kafka to scale ]
    # horizontally in the event that the producer may log data into the topics at a higher throughput-which might be the case if one switches to another api
    
    app = Application(
        broker_address=kafka_broker_address,
        consumer_group=kafka_consumer_group,
        auto_offset_reset= 'earliest', 
        #could pick 'latest' which tells the application to retrieve the latest messages in the input topic.
    )


    # Define input and output topics
    input_topic = app.topic( name=kafka_input_topic, 
                            value_serializer='json',
                            timestamp_extractor= custom_ts_extractor, #The timestamp is being extracted from the timestamp_ms key in the trade dict and not the default kafka value
                            )
    output_topic = app.topic(name=kafka_output_topic, value_serializer='json')

    # Most important part of the data prep. Here, the incoming kafka data is turned into a dataframe; a streamingdataframe from the serialised input and apply transformations
    sdf = app.dataframe(topic=input_topic)

    # breakpoint()
    # Incoming stream transformed. quixstreams shows how to use tumbling windows for streaming dataframe transformations see link for windows and aggreagations in the quixstreams doc:
    # https://quix.io/docs/quix-streams/windowing.html#updating-window-definitions

    
    # Function that established the logic for transforming data when initialising tumbling window
    # TODO Make this more readable, could I use 
    def _init_ohlc_candle(trade:dict): 

        #breakpoint()
        """
        Initialise the Open-High-Low-Close candle, assigin values from to each key taken from the first values of the first trade which is the input dict.

        """
        logger.debug(f"Received value for OHLC initialization: {trade}")
        
        # timestamp = value.get("timestamp_ms") or value.get("time")
        # if not timestamp:
        #     logger.warning(f"Missing timestamp in value: {value}")
        #     return None
        return {
            # #"timestamp" : value["timestamp_ms"],
            # "timestamp" : timestamp,
            "open": trade["price"],
            "high": trade["price"],
            "low": trade["price"],
            "close": trade["price"],
            "product_id" : trade["product_id"],
        }
        
        
    
    # Function to update the ohlc candle with a simple bubble-compare sort to update the max and min for high and low
    def _update_ohlc_candle(ohlc_candle: dict, trade: dict) -> dict:
        #breakpoint()
        """
        Basically a bubble sort. Compare subsequent trade, i.e. the most recent trade to the one before, and adjust highs and lows.

        Args:
            ohlc_candle (dict) : The current ohlc candle
            trade (dict) : New incoming trade in the sequence.
        Return:
            dict: the updated OHLC candle
        """
        return {
            "open": ohlc_candle["open"],
            "high": max(ohlc_candle["high"], trade["price"]),
            "low": min(ohlc_candle["high"], trade["price"]),
            "close": trade["price"],
            "product_id" : trade["product_id"],           
        }
   
    
    # %% Apply transformations and make candles
    
    #For monitoring:
    # sdf = sdf.tumbling_window(duration_ms =timedelta(seconds=ohlc_windows_seconds))
    # sdf = sdf.reduce(reducer=_update_ohlc_candle, initializer=_init_ohlc_candle).current()
    
    # For running in normal non debug mode
    sdf = (
        sdf.tumbling_window(duration_ms=timedelta(seconds=ohlc_windows_seconds))
        .reduce(reducer=_update_ohlc_candle, initializer=_init_ohlc_candle)
        .final()
        )


    # Make candels by assigning values from the data streamed in by from the input topic:
   
    # {
    #     'timestamp': 1717667930000, # end of the window
    #     'open': 63535.98,
    #     'high': 63537.11,
    #     'low': 63535.98,
    #     'close': 63537.11,
    #     'product_id': 'BTC/USD',
    # }
    #breakpoint()
    sdf['open'] = sdf['value']['open']
    sdf['high'] = sdf['value']['high']
    sdf['low'] = sdf['value']['low']
    sdf['close'] = sdf['value']['close']
    sdf['product_id'] = sdf['value']['product_id']
    sdf['timestamp'] = sdf['end']

    # Final step of the processing data frame after assigning values, is to return a dict as the sdf
    sdf = sdf [['timestamp',
                'product_id', 
                'open', 
                'high', 
                'low', 
                'close',
                ]]

    
    sdf = sdf.update(logger.info)
    #Write sdf to output topic
    sdf = sdf.to_topic(output_topic)


    # kick-off the streaming application
    app.run(sdf)

if __name__ == '__main__':

    # app.clear_state()  #USE TO CLEAR CACHE IF TOPIC INCASE TOPIC STATE IS GIVING ERRORS

    trade_to_ohlc(

        kafka_input_topic = config_trade_to_ohlc.kafka_input_topic_name ,
        kafka_output_topic = config_trade_to_ohlc.kafka_output_topic_name,
        kafka_broker_address = config_trade_to_ohlc.kafka_broker_address ,
        kafka_consumer_group=config_trade_to_ohlc.kafka_consumer_group,
        ohlc_windows_seconds = config_trade_to_ohlc.ohlc_windows_seconds,

    )
    


    


