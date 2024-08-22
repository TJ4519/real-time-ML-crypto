# A seperate file to create a restapi class for historical backfilling of the model instead of the live trade via websocket
import json
import requests
from typing import List,Dict
from loguru import logger
#from src.config import config_kraken_to_trade



# Convert WebSocket API product_id to REST API product_id
# product_id_websocket = config_kraken_to_trade.product_id
# product_id_rest = websocket_to_rest_map.get(product_id_websocket)

class KrakenRestAPI:
    
    # Documentation here https://docs.kraken.com/api/docs/rest-api/get-recent-trades
    # the url needs to be modded for currency pair and timestamp from where you want to return history from 
    # "https://api.kraken.com/0/public/Trades?pair=XXBTZEUR&since=1616663618". This is taken directly frmo the documentation page

    # URL : "https://api.kraken.com/0/public/Trades" # based URL 

    URL = "https://api.kraken.com/0/public/Trades?pair={product_id}&since={since_seconds}" 
    # NOTE: In kraken, for some reason, the since time is given in seconds (not consitent with milliseconds) AND as seen below, the last timestamp is nanosenconds (also needs to be consistent with milliseconds)
    # NOTE: # The REST API's GET method, specifically for public/Trades, can accept currency pair symbols in the form of BTC/USD, BTCUSD, and in the ISO 4217-A3 format, such as XBT/USD. 
    # On this page @ https://docs.kraken.com/api/docs/rest-api/get-recent-trades
    # I've played around with the "send api request" and can confirm that the URL can be fed currency pairs in both formats like BTC/USD, BTCUSD and it GETs the correct pair. 
    # The BTCUSD does get translated to XBTUSD format inside the data dict. But..BTC-USD format doesn't return any data...interesting. 

    # This mapping is no longer needed since BTC/USD type format, which is also used by websocketapi, can be used for restapi

    # websocket_to_rest_map = {
    # "BTC/USD": "XXBTZUSD",
    # "ETH/USD": "XETHZUSD",
    # "LTC/USD": "XLTCZUSD",
    # "BTC/EUR": "XXBTZEUR",
    # # Add other mappings as needed 
    # }



    # Initialise 
    def __init__( 
                self, 
                # TODO come back and fix this to include multiple product_ids
                product_ids: List[str],
                from_ms:int ,
                to_ms:int )-> None:
        """
        Initialise this class with the possibility of multiple currency pairs which can be specified in the product_ids
        and the to and from backwards looking time intervals 

        Args:
        product_ids (List[str]): A list of product IDS aka currency pairs (BTC/USD,ETH/USD ETC.) for which historic data is fetched
        from_ms (int) : Starting timestamp converted to milliseconds. This is the earliest timestamp
        to_ms(int) : End timestamp in milliseconds also. This is the latest timestamp
        
        Returns:
            none
        """
        
        if isinstance(product_ids, str):
            product_ids = [product_ids]  # Convert single string to a list


        #self.product_ids = 'BTC/USD'
        
        
        self.product_ids = product_ids 
        self.from_ms = from_ms
        self.to_ms = to_ms
        
        # At the point of initialisation the hidden _is_done variable is False, which allows the trades to be produced inside the while true statement in main.py. But in the get_trades() method
        # This variable, initialised as False, will be flipped when the last timestamp hits the from_ms, and then become true 
        
        self._is_done = False # to be flipped and flopped
        
        logger.info(f"Initialized KrakenRestAPI with product_ids: {self.product_ids}, from_ms: {self.from_ms}, to_ms: {self.to_ms}")

     
    def get_trades(self)-> List[Dict]:
        """
        Backwards looking data from kraken is retrieved in batches of max. 1000 trades

        Data struct is in dict form

        Args:
            None

        Returns:
            List[Dict] : A list of dicts, each dict is of a trade containing info like : {'product_id': 'BTC/EUR', 'price': 54255.9, 'volume': 0.00189445, 'timestamp': '2024-08-18T16:08:24.768249Z'}
        
        """
         
        

        url = "https://api.kraken.com/0/public/Trades"

        payload = {}
        headers = {'Accept': 'application/json'}

        # NOTE: from_ms needs to be consistent in units with since_seconds that is used by kraken, kraken expects seconds as the "since" timestamp. Currently since_ms is milliseconds

        since_time_in_seconds = self.from_ms // 1000 
        url = self.URL.format(product_id=self.product_ids[0], since_seconds=since_time_in_seconds)

        response = requests.request("GET", url, headers=headers, data=payload) 
        
        # The data returns a dict with two keys error and results which contains a list of list containing trade results
        # The trade results are 
        # : Array of trade entries [<price>, <volume>, <time>, <buy/sell>, <market/limit>, <miscellaneous>, <trade_id>]
        
        #Parse 
        data = json.loads(response.text)



        # TODO
        # cheecky error check to raise exception if the GET method results in an errr from the server
        # if data['error'] is not None:
        #     raise Exception(data['error'])
        
        if data.get('error'):
            raise Exception (data['e'])
        #    raise Exception(f"Kraken API error data['error'])
        
        
        # Generate the trades
        trades = []
        
        for trade in data["result"][self.product_ids[0]]:
            trades.append({
                'product_id': self.product_ids[0],
                'price' : float(trade[0]),
                'volume': float(trade[1]),
                'time' : int(trade[2]),
                
            })
        
        logger.debug(f"Received {len(trades)} trades for {self.product_ids[0]}")

        # breakpoint()
        # flip the switch 

        # NOTE: The last timestamp is in nanoseconds given by KrakenAPI.....making a comparision with to_ms (which is in milliseconds), units need to be converted
        
        last_ts_in_ns = int(data['result']['last']) #data[result][last] conatains a str like '1724255081389396723' (see https://www.epochconverter.com/) which needs to be parsed as an int first 
        last_ts = last_ts_in_ns // 1_000_000 # convert nano to milliseconds
        if last_ts >= self.to_ms:
            self._is_done = True
        
        logger.debug(f'The total amount of trades recieved for this window of time = {len(trades)} ')
        logger.debug(f'The timestamp of the latest trade in this backwards looking window is : {last_ts}')
        
        return trades
        
    
    # a boolean to allow breaks
    def is_done(self) -> bool:
        return self._is_done
    # breakpoint()






