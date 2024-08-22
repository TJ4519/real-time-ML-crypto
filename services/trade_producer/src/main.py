from typing import Dict, List

from loguru import logger
from quixstreams import Application

from src.config import config_kraken_to_trade

from src.kraken_api.websocket import KrakenWebsocketTradeAPI
from src.kraken_api.restapi import KrakenRestAPI




def produce_trades(kaka_broker_address: str, 
                   kaka_topic_name: str, 
                   product_id: str,
                   live_or_historical: str,
                   last_n_days: str,
                   ) -> None:
    """
    Reads trades from the Kraken APIs and saves them into a Kafka topic
    Note: live_or_historical can be the live or historical. 

    Args:
        kaka_broker_address (str): The address of the Kafka broker.
        kaka_topic_name (str): The name of the Kafka topic.
        product_id (str): currency pair or pairs
        live_or_historical(str) : Number of days in the past the trading data for the currency pairs that you want

    Returns:
        Live trades
    """
    #First validate is running trade_producr live or history
    assert live_or_historical in {"live", "historical"}, f"Invalid value for live or historical: {live_or_historical}"
    
    app = Application(broker_address=kaka_broker_address)

    # the topic where we will save the trades
    topic = app.topic(name=kaka_topic_name, value_serializer='json')

    logger.info(f'Creating the api to fetch data for {product_id}')

    # Create an instance of the Kraken API for websocket or restapi depending on the settings chosen in live_or_historical 
    if live_or_historical == 'live':
        kraken_api = KrakenWebsocketTradeAPI(product_id=product_id)
    else: 
        # Compute timestamps in milliseconds
        import time
        to_ms = int(time.time()*1000) # Init (milliseconds) the current point in time when asking to retrieve backwards looking data 
        from_ms = to_ms - last_n_days*24*60*60*1000 # the earliest point in time depending on how many days back you want. Days var converted into milliseconds ofc
        
        kraken_api = KrakenRestAPI(product_ids=[product_id],from_ms=from_ms, to_ms=to_ms)


    

    logger.info('Creating the producer...')


    # %% Former Producer Instance with if and else loops based on live_or_historical without using the class method with is_done(self.)
    # with app.get_producer() as producer:
    #     if live_or_historical == 'live':
    #         while True:
    #             trades: List[Dict] = kraken_api.get_trades()

    #             for trade in trades:
    #                 message = topic.serialize(key=trade['product_id'], value=trade)
    #                 producer.produce(topic=topic.name, value=message.value, key=message.key)
    #                 logger.info(f'Message sent to Kafka topic: {trade}')

    #             from time import sleep
    #             sleep(0.5)
    #     else:
    #         while True:
    #             trades: List[Dict] = kraken_api.get_trades()

    #             if not trades:  # If no more trades are returned, we're done
    #                 logger.info("All historical data has been processed.")
    #                 break

    #             for trade in trades:
    #                 message = topic.serialize(key=trade['product_id'], value=trade)
    #                 producer.produce(topic=topic.name, value=message.value, key=message.key)
    #                 logger.info(f'Message sent to Kafka topic: {trade}')

    #             # Adjust sleep duration or remove it entirely for historical data if not needed
    #             from time import sleep
    #             sleep(0.5)
# %%
    # Create a Producer instance - updated for both live and history using the is_done method-which is a method installed in both websocket and restapi classes
    # and uses the hidden _is_done condition to stop when the last timestamp hits the to_ms mark at which point the break is hit insde the while true 
    with app.get_producer() as producer:
        while True:
            
            # An if statement to allow breaks in the while True for historical data fetching which does need to switch from on to off once the history is fetched
            if kraken_api.is_done():
                logger.info('Done fetching')
                break
            
            
            # Get the trades from the Kraken API class with typed hints
            trades: List[Dict] = kraken_api.get_trades()
            # breakpoint()
            for trade in trades:
                # Serialize an event using the defined Topic
                message = topic.serialize(key=trade['product_id'], value=trade)

                # Produce a message into the Kafka topic
                producer.produce(topic=topic.name, value=message.value, key=message.key)

                logger.info(f'Message sent to first kafka topic: {trade}')

            from time import sleep

            #sleep(0.5)


if __name__ == '__main__':
    #produce_trades()


    produce_trades(kaka_broker_address=config_kraken_to_trade.kafka_broker_address,
                   kaka_topic_name=config_kraken_to_trade.kafka_topic_name,
                   product_id = config_kraken_to_trade.product_id,

                   # parameters for historical backfilling using restapi 
                   live_or_historical=config_kraken_to_trade.live_or_historical,
                   last_n_days = config_kraken_to_trade.last_n_days,
                   )