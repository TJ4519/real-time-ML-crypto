# A seperate file to create a restapi class for historical backfilling of the model instead of the live trade via websocket
import json
import requests
from pathlib import Path
from typing import List,Dict,Tuple, Optional
from loguru import logger

from time import sleep

from datetime import datetime,timezone

from src.kraken_api.trade import Trade
from concurrent.futures import ThreadPoolExecutor

import pandas as pd #used by caching class to store trades in df

class KrakenRestAPIMultipleProducts:
    """
        For each of the elements of the product_ids aka currency pair list, this class deploys an instance of the KrakenRestAPI class via paralellism.
    """
    
    def __init__(self, 
                 product_ids: List[str], 
                 last_n_days: int,
                 n_threads: Optional[int] = 1 ,
                 cache_dir: Optional[str] = None,
                 ) -> None:
        self.n_threads = n_threads

        #Instanitate a KrakenRestApi class for each element in product_ids 
        self.product_ids = product_ids
        self.kraken_apis = [
            KrakenRestAPI(product_id=product_id, last_n_days=last_n_days) for product_id in product_ids
            ]
        

    def get_trades(self) -> List[Trade]: 
        """
        Bring back the collection of trades in a dictionary containing each of the currency pair object classes
        that take after the KrakenRestAPI class.

        Args:
            None

        Returns:
            List[Trade]: A list of dicts  where each dict has the currency pair, as initialised by the dictates of the self.product_ids.
        """

        # For sequential calls and trade generation 
        if self.n_threads == 1:
            trades = []
            for kraken_api in self.kraken_apis:
                if kraken_api.is_done():
                    continue
                else:
                    trades.extend(kraken_api.get_trades())
        
        
        #For concurrent(parlellish) API calls using multiple product_ids
        else:
            with ThreadPoolExecutor(max_workers=self.n_threads) as executor:
                trades = list(executor.map(self.get_trades_for_one_pairclass, self.kraken_apis))
                
                # Flatten the list of lists into a single list
                trades = [trade for sublist in trades for trade in sublist]
     
     # Add back in if the concurrent block is to be removed
        # for kraken_api in self.kraken_apis:
        #     # this is_done() is quite useful don't omit
        #     if kraken_api.is_done():
        #         continue
        #     trades.extend(kraken_api.get_trades())
        
        return trades
    
    def get_trades_for_one_pairclass(self, kraken_api: 'KrakenRestAPI') -> List[Dict]:
        """
        The single most critical and indentifiable function, that is to be executed in each paralel thread to get trades for the self. product_id assigned to the thread 
        
        Invokes the get_trades() method for each class returning trades in a List[Dict] until is_done is flipped from false to true.
        
        Returns:
            List[Dict]: List of trades from one Kraken API instance.
        """
        if not kraken_api.is_done():
            return kraken_api.get_trades()
        return []

    def is_done(self) -> bool:
        """
        Check if all KrakenRestAPI instances are done fetching trades.

        Returns:
            bool: True if all instances are done, False otherwise.
        """
        # for kraken_api in self.kraken_apis:
        #     if not kraken_api.is_done():
        #         return False

        # return True
        
        return all(kraken_api.is_done() for kraken_api in self.kraken_apis)

class KrakenRestAPI:
    
    # Documentation here https://docs.kraken.com/api/docs/rest-api/get-recent-trades
    # the url needs to be modded for currency pair and timestamp from where you want to return history from 
    # "https://api.kraken.com/0/public/Trades?pair=XXBTZEUR&since=1616663618". This is taken directly from the documentation page

    # URL : "https://api.kraken.com/0/public/Trades" # based URL 

    URL = "https://api.kraken.com/0/public/Trades?pair={product_id}&since={since_seconds}" 
    
    # NOTE: In kraken, for some reason, the since time is given in seconds (not consitent with milliseconds) AND as seen below, the last timestamp is nanosenconds (also needs to be consistent with milliseconds)
    # NOTE: # The REST API's GET method, specifically for public/Trades, can accept currency pair symbols in the form of BTC/USD, BTCUSD, and in the ISO 4217-A3 format, such as XBT/USD. 
    # On this page @ https://docs.kraken.com/api/docs/rest-api/get-recent-trades

    def __init__( 
                self, 
                product_id: str,
                last_n_days: int,
                cache_dir: Optional[int]=None,
                )-> None:
        """
        Initialise this class with the possibility of multiple currency pairs which can be specified in the product_ids
        and the to and from backwards looking time intervals 

        Args:
        product_ids (str): A single product ID from the set of initialised currency pairs in via the looping over self.product_ids.
        last_n_days (int): The total number of days for which you want the trades. from and to timestamps are calculated within the class
        cache_dir (Optional(str)): A directory to cache the trade data in compressed parquet format to help speed things when re-running (after breakage or manual stopping) the trade_producer over the same product_ids and time window 
        
        Returns:
            none
        """
               
        # Instatiate variables. from and to_ms are calculated using a static method called _init_from_ms_and_from_ms

        self.product_id = product_id
        self.from_ms,self.to_ms = self._init_from_ms_and_from_ms(last_n_days) 
        
        # The _is_done() variable is initialised as false
        self.last_trade_ms = self.from_ms 

        self._is_done = False # to be flipped and flopped
        
        logger.info(f"Initialized KrakenRestAPI with product_ids: {self.product_id}, from_ms: {self.from_ms}, to_ms: {self.to_ms}")
        logger.debug(f'Initializing KrakenRestAPI: The furthest point in time from_ms={ts_to_date(self.from_ms)}, towards to_ms={ts_to_date(self.to_ms)}')
        
        
        self.use_cache = False
        if cache_dir is not None:
            self.cache = CachedTradeData(cache_dir)
            self.use_cache = True

    # %% Compute the from_ms and to_ms
    @staticmethod
    def _init_from_ms_and_from_ms(last_n_days:int)-> Tuple [int,int]:
        """
        Returns from_ms and to_ms timestamps for the backwards looking data. 
        #NOTE: the timewindow, as specified by the last_n_days, is computed from the presnet day midnight to the to_ms midnight. If re-run is made today, it will bind the timewindow to the midnight of the day.

        Args:
            last_n_days(int): The number of days in the past for which the data is needed
        Returns:
            Tuple[int,int]: tuple of from_ms and to_ms timsetamps
        """

        today_date = datetime.now(timezone.utc).replace(hour=0,minute=0,second=0,microsecond=0)
       
        #today_date = datetime.now(timezone.utc)
        to_ms = int(today_date.timestamp()*1000) # Init (milliseconds) the current point in time when asking to retrieve backwards looking data 
        from_ms = to_ms - last_n_days*24*60*60*1000 # the earliest point in time depending on how many days back you want. Days var converted into milliseconds ofc
            
        return from_ms, to_ms
    

    
    # %%
    def get_trades(self)-> List[Trade]:
        """
        #NOTE: max 1000 trades can be gotten for each currency per api request
        
        This method fetches trades, as trade objects, for a single product_id and....

        1. Caches the trades into compressed parquet to avoid repeat requests when re-running over the same timewindow in case of errors (or manually stopping)
        2. Handle errors in case restapi chucks a rate limiting thing
        3. Filter the trades within a specific time window (bounded by from_ms and to_ms).

        Args:
            None

        Returns:
            List[Trade] : A list of Trade objects, each dict object is of a trade containing info like : {'product_id': 'BTC/EUR', 'price': 54255.9, 'volume': 0.00189445, 'timestamp': '2024-08-18T16:08:24.768249Z'}
        
        """
         
        
        # Step 1 - The URL constructor
        url = "https://api.kraken.com/0/public/Trades"

        payload = {}
        headers = {'Accept': 'application/json'}

        # NOTE: from_ms needs to be consistent in seconds units that is used by kraken URL for GET

        #since_time_in_seconds = self.last_trade_ms // 1000 # convert the earliest timsetamp into seconds (last_trade_ms, derived from self.from_ms upon initialisation)
        
        since_ns = self.last_trade_ms * 1_000_000

        since_time_in_seconds = self.last_trade_ms // 1000 # convert the earliest timsetamp into seconds (last_trade_ms, derived from self.from_ms upon initialisation)
        url = self.URL.format(product_id=self.product_id, since_seconds=since_time_in_seconds)
        logger.debug(f"{url=}")

        # Step 2 - Check if the url hasn't already been cached with data to avoid redundant calls to api
        if self.use_cache and self.cache.has(url):
            trades = self.cache.read(url)
            # since_ns = self.last_trade_ms *1_000_000
            logger.debug(f"Loaded a  number of trades that were cached. A total of {len(trades)} trades retrieved for {self.product_id}, since={ns_to_date(since_ns)}")

        # Step 3 - If no cache, then proceed with the url and GET request to the servers
        else:
            response = requests.request("GET", url, headers=headers, data=payload) 

            # NOTE The data returns a dict with two keys error and results which contains a list of list containing trade results
            # NOTE Array of trade entries [<price>, <volume>, <time>, <buy/sell>, <market/limit>, <miscellaneous>, <trade_id>]
        
            # Parse the incoming data from the GET request, which is JSON str, into an interatble dict
            data = json.loads(response.text)
            
            
            # Error handling incase api is rate-limited
            if ('error' in data) and ('EGeneral:Too many requests' in data['error']):
                # slow down the rate at which we are making requests to the Kraken API
                logger.info('Too many requests. Sleeping for 30 seconds')
                sleep(30) # Billions must wait in line

            # Generate the trades- NOTE: using a list comprehension trick instead instead of doing 
            trades = [
            Trade(
                    product_id=self.product_id,
                    price=float(trade[0]),
                    volume=float(trade[1]),
                    timestamp_ms=int(trade[2]*1000), # Converting the timestamp (in seconds) to milliseconds
                )

                for trade in data["result"][self.product_id]
                
                ]                
            
            # Total trades in batch
            logger.debug(f"Total amount of trades = {len(trades)}")       
            # Log the number of trades and the timestamps in the current batch
            logger.debug(f"Fetched {len(trades)} trades for {self.product_id}, last trade timestamp_ms={trades[-1].timestamp_ms}")
            

            # Step 4 - Cache them shits
            if self.use_cache:
                self.cache.write(url,trades)
                logger.debug(
                    f'Cached {self.product_id}, since={ns_to_date(since_ns)}'
                )

            # Sleep for a moment before next fresh url
            sleep(1)

        # # NOTE: This is a fix to solve a sitiation where, on occaisons the last trade in a single batch is the same as the first batch, which is throwing it for an infite loop.
        # In the occaison that last trade and the first trade are the same, 
        if trades[-1].timestamp_ms == self.last_trade_ms:
            # If the timestamps are the same, increment the `last_trade_ms` by 1 millisecond to move forward.
            self.last_trade_ms = trades[-1].timestamp_ms + 1

        # else: 
        # NOTE: Otherwise, under normal expected conditions, use the value for timestamp_ms trades[-1].timestamp_ms as `self.last_trade_ms`....
        # But..the last_ts_in_ns = int(data['result']['last']) 
        #    self.last_trade_ms = trades[-1].timestamp_ms
        
        # breakpoint()

        # Apply Filtering
        trades = [trade for trade in trades if trade.timestamp_ms <= self.to_ms]
        logger.debug(f"Received {len(trades)} trades for {self.product_id}")      

        # NOTE: The last timestamp is in nanoseconds given by KrakenAPI.....making a comparision with to_ms (which is in milliseconds), units need to be converted.
        # Example  using BTC/USD from Kraken documnention:
            #             {
            #   "error": [],
            #   "result": {
            #     "BTC/USD": [
            #       [
            #         "122.00000",
            #         "0.10000000",
            #         1381095256.0907948,
            #         "s",
            #         "l",
            #         "",
            #         1
            #       ],
            #       [
            #         "123.61000",
            #         "0.10000000",
            #         1381179031.0033572,
            #         "s",
            #         "l",
            #         "",
            #         2
            #       ],
            # [ 
            #     "220.36266",
            #     "0.11850777",
            #     1383581942.7930002, <--------lastish timestamp in milliseconds
            #     "b",
            #     "l",
            #     "",
            #     1000
            # ]
            # 
            # ],
            # "last": "1383581942793000173" <-the same trades[-1].timestamp_ms above, but under key of "last" and in nanoseconds 
            #   }
            # }
        last_ts_in_ns = int(data['result']['last']) 
        self.last_trade_ms = last_ts_in_ns // 1_000_000 # convert the kraken's timsetamp, given in nanoseconds, into milliseconds

    
    
        logger.debug(f'The total amount of trades recieved for this window of time = {len(trades)} ')
        logger.debug(f'The timestamp of the EARLIEST (SMALLEST EPOCH) trade in this backwards looking window is : {last_ts_in_ns}')
    
        return trades
        
    # %%
    def is_done(self) -> bool:       
        return self.last_trade_ms >= self.to_ms
        

class CachedTradeData:
     
        """
            Class to handle the caching of trade data fetched from the Kraken REST API.
        """

    # Innit bruv a path for cache dir
        def __init__(self, cache_dir: str) -> None:
            self.cache_dir = Path(cache_dir)

            if not self.cache_dir.exists():
                # create the cache directory if it does not exist
                self.cache_dir.mkdir(parents=True)


        def read(self, url: str) -> List[Trade]:
            """
            Reads from the cache the trade data for the given url
            """
            file_path = self._get_file_path(url)

            if file_path.exists():
                # read the data from the parquet file
                

                data = pd.read_parquet(file_path)
                # transform the data to a list of Trade objects
                return [Trade(**trade) for trade in data.to_dict(orient='records')]

            return []

        def write(self, url: str, trades: List[Trade]) -> None:
            """
            Save trades into a compressed parquet file in the cache directory
            """
            
            if not trades:
                return

            data = pd.DataFrame([trade.model_dump() for trade in trades])

            # write the DataFrame to a parquet file
            file_path = self._get_file_path(url)
            data.to_parquet(file_path)

        def has(self, url: str) -> bool:
            """
            Returns True if the cache has the trade data for the given url, False otherwise.
            """
            file_path = self._get_file_path(url)
            return file_path.exists()

        def _get_file_path(self, url: str) -> str:
            """
            Returns the file path where the trade data for the given url is (or will be) stored.
            """
            # use the given url to generate a unique file name in a deterministic way
            import hashlib

            url_hash = hashlib.md5(url.encode()).hexdigest()
            return self.cache_dir / f'{url_hash}.parquet'
            # return self.cache_dir / f'{product_id.replace ("/","-")}_{from_ms}.parquet'

def ts_to_date(ts: int) -> str:
    """
    Transform a timestamp in Unix milliseconds to a human-readable date

    Args:
        ts (int): A timestamp in Unix milliseconds

    Returns:
        str: A human-readable date in the format '%Y-%m-%d %H:%M:%S'
    """

    return datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime(
        '%Y-%m-%d %H:%M:%S'
    )


def ns_to_date(ns: int) -> str:
    """
    Transform a timestamp in Unix nanoseconds format to a human-readable date

    Args:
        ns (int): A timestamp in Unix nanoseconds

    Returns:
        str: A human-readable date in the format '%Y-%m-%d %H:%M:%S'
    """

    return datetime.fromtimestamp(ns / 1_000_000_000, tz=timezone.utc).strftime(
        '%Y-%m-%d %H:%M:%S'
    )
        