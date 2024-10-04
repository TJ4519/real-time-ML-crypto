from typing import Dict, List

from loguru import logger
from quixstreams import Application

from src.config import config_kraken_to_trade

from src.kraken_api.websocket import KrakenWebsocketTradeAPI
from src.kraken_api.restapi import KrakenRestAPI
from src.kraken_api.restapi import KrakenRestAPIMultipleProducts
from src.kraken_api.trade import Trade
from typing import Optional
import json
# import sys
# import os

# print("sys.path:", sys.path)  # This will print out all the paths Python is using
# print("Working Directory:", os.getcwd())  # This will print the current working directory

# breakpoint()
def produce_trades(kaka_broker_address: str, 
                   kaka_topic_name: str, 
                   product_ids: List[str], #Updated from a single element str variable of product_id which is passed from the config.py. product_id: str
                   live_or_historical: str,
                   last_n_days: str,
                   cache_dir: Optional[str],
                   ) -> None:
    """
    Reads trades from the Kraken APIs and saves them into a Kafka topic
    Note: live_or_historical can be the live or historical. 

    Args:
        kaka_broker_address (str): The address of the Kafka broker.
        kaka_topic_name (str): The name of the Kafka topic.
        product_ids List(str): currency pair or pairs
        live_or_historical(str) : Number of days in the past the trading data for the currency pairs that you want

    Returns:
        Live trades
    """
    #First validate is running trade_producr live or history
    assert live_or_historical in {"live", "historical"}, f"Invalid value for live or historical: {live_or_historical}" #Notes: Assert is for testing, for production just use if else try
    
    app = Application(broker_address=kaka_broker_address)

    # the topic where we will save the trades
    topic = app.topic(name=kaka_topic_name, value_serializer='json')

    logger.info(f'Creating the api to fetch data for {product_ids}')

    # Create an instance of the Kraken API for websocket or restapi depending on the settings chosen in live_or_historical 
    if live_or_historical == 'live':
        kraken_api = KrakenWebsocketTradeAPI(product_ids=product_ids) #Updared websocket class to handle a list of strings aka product_ids instead of a single str currency  as product_id
    else: 
        # TODO Add back the multiple product ID fix using using KrakenRestAPIMultipleProducts
        
        # kraken_api = KrakenRestAPIMultipleProducts(product_ids=[product_id],
        #                            last_n_days = last_n_days,
        #                            )
        kraken_api = KrakenRestAPIMultipleProducts(product_ids=product_ids,
                                   last_n_days = last_n_days,
                                   cache_dir= cache_dir,
                                   )


    

    logger.info('Creating the producer...')

# %%
    # Create a Producer instance - updated for both live and history using the is_done method-which is a method installed in both websocket and restapi classes
    # and uses the hidden _is_done condition to stop when the last timestamp hits the to_ms mark at which point the break is hit insde the while true 
    with app.get_producer() as producer:
        while True:
            #breakpoint()
            # An if statement to allow breaks in the while True for historical data fetching which does need to switch from on to off once the history is fetched
            if kraken_api.is_done():
                logger.info('Done fetching')
                break
            
            
            # Get the trades from the Kraken API class with typed hints
                       
            trades: List[Trade]  = kraken_api.get_trades()
            
            # breakpoint()
            for trade in trades:
                # Kafka transmits messages as machine readable byte string, so prepare input to be serialised for Topic transmission
                message = topic.serialize(
                    key=trade.product_id, 
                    value = trade.model_dump(), #model_dump() from pydantic converts objects to dicts; for serialising 
                    )
                
                producer.produce(
                    topic=topic.name, 
                    value=message.value,
                    #value = json.dumps(test_message), 
                    #key="BTC/USD",
                    key=message.key)
                
                logger.debug(f'{trade.model_dump()}')
                

                #logger.info(f'Message sent to first kafka topic: {trade}')



if __name__ == '__main__':
    #produce_trades()


    produce_trades(kaka_broker_address=config_kraken_to_trade.kafka_broker_address,
                   kaka_topic_name=config_kraken_to_trade.kafka_topic_name,
                   product_ids = config_kraken_to_trade.product_ids, #changed to handle multiple product_ids List[str] from a previou single string product_id passed to the config

                   # parameters for historical backfilling using restapi 
                   live_or_historical=config_kraken_to_trade.live_or_historical,
                   last_n_days = config_kraken_to_trade.last_n_days,
                   cache_dir=config_kraken_to_trade.cache_dir,
                   )