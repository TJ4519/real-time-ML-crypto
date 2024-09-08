# A seperate file to create a restapi class for historical backfilling of the model instead of the live trade via websocket
import json
import requests
from typing import List,Dict,Tuple
from loguru import logger
import datetime
#from src.config import config_kraken_to_trade


class KrakenRestAPI:
    
    # Documentation here https://docs.kraken.com/api/docs/rest-api/get-recent-trades
    # the url needs to be modded for currency pair and timestamp from where you want to return history from 
    # "https://api.kraken.com/0/public/Trades?pair=XXBTZEUR&since=1616663618". This is taken directly from the documentation page

    # URL : "https://api.kraken.com/0/public/Trades" # based URL 

    URL = "https://api.kraken.com/0/public/Trades?pair={product_id}&since={since_seconds}" 
    
    # NOTE: In kraken, for some reason, the since time is given in seconds (not consitent with milliseconds) AND as seen below, the last timestamp is nanosenconds (also needs to be consistent with milliseconds)
    # NOTE: # The REST API's GET method, specifically for public/Trades, can accept currency pair symbols in the form of BTC/USD, BTCUSD, and in the ISO 4217-A3 format, such as XBT/USD. 
    # On this page @ https://docs.kraken.com/api/docs/rest-api/get-recent-trades

    # %%
    def __init__( 
                self, 
                # TODO come back and fix this to include multiple product_ids
                product_ids: List[str],
                last_n_days: int,
                
                )-> None:
        """
        Initialise this class with the possibility of multiple currency pairs which can be specified in the product_ids
        and the to and from backwards looking time intervals 

        Args:
        product_ids (List[str]): A list of product IDS aka currency pairs (BTC/USD,ETH/USD ETC.) for which historic data is fetched
        
        last_n_days (int): Pass the total number of days for which you want the trades. from and to timestamps are calculated within the class
        
        Returns:
            none
        """
        
        if isinstance(product_ids, str):
            product_ids = [product_ids]  # Convert single string to a list
        
        
        # Instatiate variables. from and to_ms are calculated using a static method called _init_from_ms_and_from_ms

        self.product_ids = product_ids 
        #self.from_ms,self.to_ms = self._init_from_ms_and_from_ms(last_n_days) #this should work
        self.to_ms, self.from_ms = self._init_from_ms_and_from_ms(last_n_days)
        self.last_trade_ms = self.to_ms 
        #self.last_trade_ms = self.from_ms
        
            
        self._is_done = False # to be flipped and flopped
        
        logger.info(f"Initialized KrakenRestAPI with product_ids: {self.product_ids}, from_ms: {self.from_ms}, to_ms: {self.to_ms}")


    # %% Compute the from_ms and to_ms
    #@staticmethod
    def _init_from_ms_and_from_ms(self,last_n_days:int)-> Tuple [int,int]:
        """
        Returns from_ms and to_ms timestamps for the backwards looking data

        Args:
            last_n_days(int): The number of days in the past for which the data is needed
        Returns:
            Tuple[int,int]: tuple of from_ms and to_ms timsetamps
        """

        import time
        from datetime import datetime,timezone

        today_date = datetime.now(timezone.utc).replace(hour=0,minute=0,second=0,microsecond=0)
        #today_date = datetime.now(timezone.utc)
        to_ms = int(today_date.timestamp()*1000) # Init (milliseconds) the current point in time when asking to retrieve backwards looking data 
        from_ms = to_ms - last_n_days*24*60*60*1000 # the earliest point in time depending on how many days back you want. Days var converted into milliseconds ofc
            
        return from_ms, to_ms
    
    # %%
    def get_trades(self)-> List[Dict]:
        """
        Backwards looking data from kraken is retrieved in batches of max. 1000 trades

        Args:
            None

        Returns:
            List[Dict] : A list of dicts, each dict is of a trade containing info like : {'product_id': 'BTC/EUR', 'price': 54255.9, 'volume': 0.00189445, 'timestamp': '2024-08-18T16:08:24.768249Z'}
        
        """
         
        

        url = "https://api.kraken.com/0/public/Trades"

        payload = {}
        headers = {'Accept': 'application/json'}

        # NOTE: from_ms needs to be consistent in seconds units that is used by kraken URL for GET

        since_time_in_seconds = self.last_trade_ms // 1000 # convert the earliest timsetamp into seconds (last_trade_ms, derived from self.from_ms upon initialisation)
        url = self.URL.format(product_id=self.product_ids[0], since_seconds=since_time_in_seconds)

        response = requests.request("GET", url, headers=headers, data=payload) 
        
        # The data returns a dict with two keys error and results which contains a list of list containing trade results
        # The trade results are 
        # : Array of trade entries [<price>, <volume>, <time>, <buy/sell>, <market/limit>, <miscellaneous>, <trade_id>]
        
        #Parse 
        data = json.loads(response.text)
        
        if data.get('error'):
            raise Exception (f"Kraken API error data['error'])")
                
        # Generate the trades
        trades = []
        for trade in data["result"][self.product_ids[0]]:
            trades.append({
                'product_id': self.product_ids[0],
                'price' : float(trade[0]),
                'volume': float(trade[1]),
                'timestamp' : int(trade[2]),
                
            })

        #Debugger to see trades before the filtering mechanism using from_ms
        logger.debug(f"Total amount of trades = {len(trades)}")
        logger.debug(f"Trades before filtering: {trades}")
        breakpoint()
        
        
        # Apply Filtering
        # Selective filter on trades to be only between the from and to milliseconds timestamps
        trades = [trade for trade in trades if trade['timestamp'] <= self.from_ms//1000]
        
        
        logger.debug(f"Received {len(trades)} trades for {self.product_ids[0]}")

        # breakpoint()
        

        # NOTE: The last timestamp is in nanoseconds given by KrakenAPI.....making a comparision with to_ms (which is in milliseconds), units need to be converted
        
        last_ts_in_ns = int(data['result']['last'])  
        self.last_trade_ms = last_ts_in_ns // 1_000_000 # convert the kraken's timsetamp, given in nanoseconds, into milliseconds
        self._is_done = self.last_trade_ms >= self.to_ms
        
        logger.debug(f'The total amount of trades recieved for this window of time = {len(trades)} ')
        logger.debug(f'The timestamp of the latest trade in this backwards looking window is : {last_ts_in_ns}')
        # breakpoint()
        return trades
        
    # %%
    def is_done(self) -> bool:
        return self._is_done
    






