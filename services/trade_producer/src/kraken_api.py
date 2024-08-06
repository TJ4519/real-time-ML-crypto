import json
from typing import Dict, List

from loguru import logger
from websocket import create_connection


class KrakenWebsocketTradeAPI:
    URL = 'wss://ws.kraken.com/v2'

    def __init__(self, product_id: str):
        self.product_id = product_id

        self.ws = create_connection(self.URL)
        logger.info('Connecting to the URL: Success')

        self._likensubscribe(product_id)

    # hidden method to establish connection and subscribe to the message que
    def _likensubscribe(self, product_id: str):
        """
        Function:
        Estbalish connection to kraken's api, then subscirbe

        Args:
        product_id : str - The trading pair currency for which the subscription is established

        """

        # subscribing by sending a message through the kraken websocket api. Message pulled from kraken api documentation
        logger.info(
            f'Now....attempting subscribing to trades for the following pair {product_id} via messasing to websocket'
        )
        msg = {
            'method': 'subscribe',
            'params': {'channel': 'trade', 'symbol': [product_id], 'snapshot': False},
        }

        # Sending the message, convert the dictionary in msg first to a string type using json.dumps
        self.ws.send(json.dumps(msg))

        logger.info('Success, message subscription live')

    def get_trades(self) -> List[Dict]:
        while True:
            message = (
                self.ws.recv()
            )  # Recieved message from API is string convert into dict using json dumps
            message_data = json.loads(message)

            # prepare trades dict by initialising empty
            trades = []

            # from the message_data, ignore connection, heartbeat, and other non trade messages that don't contain market data

            if (
                message_data.get('channel') == 'trade'
                and message_data.get('type') == 'update'
                and 'qty' in message_data.get('data', [{}])[0]
            ):  # Ensure data is present and qty is in the first item
                # Append each trade item to the trades list
                for trade in message_data['data']:
                    trades.append(
                        {
                            'product_id': self.product_id,
                            'price': trade['price'],
                            'volume': trade['qty'],
                            'timestamp': trade['timestamp'],
                        }
                    )

            # breakpoint()

            return trades
