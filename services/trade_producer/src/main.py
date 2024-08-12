from typing import Dict, List

from loguru import logger
from quixstreams import Application

from src.kraken_api import KrakenWebsocketTradeAPI
from src.config import config


def produce_trades(kaka_broker_address: str, 
                   kaka_topic_name: str, 
                   product_id: str
                   ) -> None:
    """
    Reads trades from the Kraken websocket API and saves them into a Kafka topic

    Args:
        kaka_broker_address (str): The address of the Kafka broker.
        kaka_topic_name (str): The name of the Kafka topic.
        product_id (str): currency pair

    Returns:
        Live trades
    """
    app = Application(broker_address=kaka_broker_address)

    # the topic where we will save the trades
    topic = app.topic(name=kaka_topic_name, value_serializer='json')

    # Create an instance of the Kraken API
    kraken_api = KrakenWebsocketTradeAPI(product_id=product_id)

    logger.info('Creating the producer...')

    # Create a Producer instance
    with app.get_producer() as producer:
        while True:
            # Get the trades from the Kraken API class with typed hints
            trades: List[Dict] = kraken_api.get_trades()

            for trade in trades:
                # Serialize an event using the defined Topic
                message = topic.serialize(key=trade['product_id'], value=trade)

                # Produce a message into the Kafka topic
                producer.produce(topic=topic.name, value=message.value, key=message.key)

                logger.info(f'Message sent to first kafka topic: {trade}')

            from time import sleep

            sleep(0.5)


if __name__ == '__main__':
    #produce_trades()


    produce_trades(kaka_broker_address=config.kafka_broker_address,
                   #'localhost:19092',
                   #'redpanda-0:9092',
                   kaka_topic_name=config.kafka_topic_name,
                   product_id = config.product_id,
                   )