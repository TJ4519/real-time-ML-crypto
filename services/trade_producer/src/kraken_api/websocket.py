import json
from typing import Dict, List

from loguru import logger
from websocket import create_connection
from datetime import datetime,timezone
from src.kraken_api.trade import Trade #The pydantic class for creating the unique trade objects, instead of dictionaries

class KrakenWebsocketTradeAPI:
    URL = 'wss://ws.kraken.com/v2'

    def __init__(self, product_ids: List[str]):
        self.product_ids = product_ids

        self.ws = create_connection(self.URL)
        logger.info('Connecting to the URL: Success')

        self._likensubscribe(product_ids)

    def _likensubscribe(self, product_ids: List[str]):
        """
        Function:
        Estbalish connection to kraken's api, then subscirbe

        Args:
        product_id : str - The trading pair currency for which the subscription is established

        """

        # subscribing by sending a message through the kraken websocket api. Message pulled from kraken api documentation
        logger.info(
            f'Now....attempting to subscribe to trades for the following pair(s) of currency pairs: {product_ids} via websocket'
        )
        msg = {
            'method': 'subscribe',
            'params': {'channel': 'trade', 'symbol': product_ids, 'snapshot': False},
        }

        # Sending the message, convert the dictionary in msg first to a string type using json.dumps
        self.ws.send(json.dumps(msg))

        logger.info('Success, message subscription live')

    def get_trades(self) -> List[Trade]:
        """
        Modded function/method that:

        Args:None

        Returns: A "Trade" object, instead of a list of dictionaries...the "Trade" class is defined using pydantic
        
        """
        # while True:
            
        message = (self.ws.recv())  # Recieved message from API is string convert into dict using json dumps
            
            
        message_data = json.loads(message) #string converted to dicts, the values are accessed and stored as trade objects


            # prepare trades dict by initialising empty
        trades = []

            # from the message_data, ignore connection, heartbeat, and other non trade messages that don't contain market data

        if (
                message_data.get('channel') == 'trade'
                and message_data.get('type') == 'update'
                and 'qty' in message_data.get('data', [{}])[0]
            ):  
                # Ensure data is present and qty is in the first item
                # Append each trade item to the trades list
                
            for trade in message_data['data']:
                   
                    # New conversion using static method
                timestamp_ms = self.to_ms(trade['timestamp'])

                trades.append(
                        # Old dict way of doing business
                        # {
                        #     'product_id': self.product_id,
                        #     'price': trade['price'],
                        #     'volume': trade['qty'],
                        #     'timestamp': trade['timestamp'],
                        # }
                        Trade (
                            product_id = trade['symbol'],
                            price=trade['price'],
                            volume=trade['qty'],
                            timestamp_ms=timestamp_ms,

                        )
                     
                    )

            # breakpoint()

        return trades
    #Cheeky 
    def is_done(self) -> bool:
        """
        is_done flag is never flipped since its live websocket retrieval of data from the api unlike the restapi
        """
        return False
    
    @staticmethod  # Used for timestamp conversion in the websocket class when accessing the vars in the trade class
    def to_ms(timestamp: str) -> int:
        """
        A function that transforms the websocket timestamps that appear as ISO 8601 format string like '2024-06-17T09:36:39.467866Z'
        into UNIX timestamps 

        Args:
            timestamp (str): A timestamp expressed as a string.

        Returns:
            int: A timestamp expressed in milliseconds.
        """
        # parse a string like this '2024-06-17T09:36:39.467866Z'
        # into a datetime object assuming UTC timezone
        # and then transform this datetime object into Unix timestamp
        # expressed in milliseconds
        
        timestamp = datetime.fromisoformat(timestamp[:-1]).replace(tzinfo=timezone.utc)
        return int(timestamp.timestamp() * 1000)
