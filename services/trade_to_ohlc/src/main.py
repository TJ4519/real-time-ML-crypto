
from quixstreams import Application
from datetime import timedelta
from loguru import logger

#TODO fix config params
#from src.config import config



def trade_to_ohlc(
        kaka_input_topic: str,
        kaka_output_topic: str,
        kaka_broker_address: str,
        ohlc_windows_seconds: int,
)-> None:
    """
    Takes the stream of trading information from the input kafka topic address, slices the data into a window measured in seconds, to create candles for open, close, high and low
    The "candlestick" data is stored in another output for feature storage



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
        broker_address=kaka_broker_address,
        consumer_group="trade_to_ohlc",
        auto_offset_reset= 'earliest', 
        #could pick 'latest' which tells the application to retrieve the latest messages in the input topic.
    )


    # Define input and output topics
    input_topic = app.topic(name=kaka_input_topic, value_serializer='json')
    output_topic = app.topic(name=kaka_output_topic, value_serializer='json')

    # streamingdatafarmes sdf 
    sdf = app.dataframe(topic=input_topic)


    # Incoming stream transformed. quixstreams shows how to use tumbling windows for streaming dataframe transformations see link for windows and aggreagations in the quixstreams doc:
    # https://quix.io/docs/quix-streams/windowing.html#updating-window-definitions

    
    # Function that established the logic for transforming data when initialising tumbling window
    def _init_ohlc_candle(value: dict) -> dict:
        """
        Initialise the Open-High-Low-Close candle from the first trade which is the input dict.

        Once initialised, the update mechanism needs to also be applied

        """

        return {
            "timestamp" : value["timestamp"],
            "open": value["price"],
            "high": value["price"],
            "low": value["price"],
            "close": value["price"],
            "product_id" : value["product_id"],
        }
    
    
    
    
    # Function to update the ohlc candle with a simple bubble-compare sort to update the max and min for high and low
    def _update_ohlc_candle(ohlc_candle: dict, trade: dict) -> dict:
        """
        Basically a bubble sort. Compare the most recent trade to the one before, and adjust highs and lows.

        Args:
            ohlc_candle : dict : The current ohlc candle
            trade : dict : New incoming trade
        Return:
            dict: the updated OHLC candle
        """
        return {
            "timestamp": ohlc_candle["timestamp"],
            "open": ohlc_candle["open"],
            "high": max(ohlc_candle["high"], trade["price"]),
            "low": min(ohlc_candle["high"], trade["price"]),
            "close": trade["price"],
            "product_id" : trade["product_id"],
            #
        }
    ##############################################################################################
    
    
    # apply transformations and make candles
    sdf = sdf.tumbling_window(duration_ms =timedelta(seconds=ohlc_windows_seconds))
    sdf = sdf.reduce(reducer=_update_ohlc_candle, initializer=_init_ohlc_candle).current()



    sdf['open'] = sdf['value']['open']
    sdf['high'] = sdf['value']['high']
    sdf['low'] = sdf['value']['low']
    sdf['close'] = sdf['value']['close']
    sdf['product_id'] = sdf['value']['product_id']

    # add timestamp key
    sdf['timestamp'] = sdf['end']


    sdf = sdf [['timestamp',
                'product_id', 
                'open', 
                'high', 
                'low', 
                'close',
                ]]


    sdf = sdf.update(logger.info)
    
    sdf = sdf.to_topic(output_topic)


    # kick-off the streaming application
    app.run(sdf)

if __name__ == '__main__':

    from src.config import config

    trade_to_ohlc(

        kaka_input_topic = 'trade' ,
        kaka_output_topic = 'ohlc',
        kaka_broker_address = 'localhost:19092' , #local external port, 
        ohlc_windows_seconds = 2,
        # kaka_input_topic = config.kafka_input_topic ,
        # kaka_output_topic = config.kafka_output_topic,
        # kaka_broker_address = config.kafka_broker_address ,
        # ohlc_windows_seconds = config.ohlc_windows_seconds,

    )
    
    


