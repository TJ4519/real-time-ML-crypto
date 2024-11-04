import hopsworks
from hsfs.feature_group import FeatureGroup
import pandas as pd
from loguru import logger
# from dotenv import load_dotenv
import os

# import data from another service folder
csv_file_path = r'C:\Users\tejas\OneDrive\Desktop\real-time-ml-trial3\services\kafka_to_feature_store\src\ohlc_data.csv'
data = pd.read_csv(csv_file_path)

#Create a copy
ohlc_data = data.copy(deep=True)


# Make the OHLC data more readable 
print(ohlc_data.head())
ohlc_data['datetime']= pd.to_datetime(ohlc_data['timestamp'],unit='ms')


# TODO Impute 'missing' candle sticks when there are no trades

def interpolate_missing_candles(ohlc_data: pd.DataFrame = ohlc_data ,
                                ohlc_windows_seconds:int = 60, # from the config
                                ) -> pd.DataFrame:
    """
    Uses pandas re-indexing to read the closing price from the ohlc candle of the last 'available' candlestick if no trades have been registered after the batch of time: ohlc_window_seconds 
    has elapsed. Forward filling technique, but also creating a flag of the rows popluated by this forward fill to engineer a feature column later 

    Args:
        ohlc_data (pd.DataFrame): The OHLC data retrieved from feature group or external csv
        ohlc_window_sec (int): The size of the window in seconds.

    Returns:
        pd.DataFrame: The OHLC data with the missing candles interpolated.
    """

    # Define the index column
    ohlc_data.set_index('timestamp', inplace=True)
    
    # Range of timestamp values within within interpolation is to be done
   
    from_ms = ohlc_data.index.min() #not to be confused with from_ms and to_ms of trade_producer -the from and to are defining the range of values
    to_ms = ohlc_data.index.max()
    labels = range(from_ms, to_ms, ohlc_windows_seconds * 1000)
    
    # reindex the dataframe to add missing rows
    ohlc_data = ohlc_data.reindex(labels)

    # Flag forward-filled rows :NOTE: Used later for engineering the time-since-last-trades column
    ohlc_data['is_forward_filled'] = ohlc_data['close'].isnull()

    # interpolate missing values using forward fill for close prices
    ohlc_data['close'].ffill(inplace=True)
    # if ohlc_data['open] is null use the ohlc_data['close'] value
    ohlc_data['open'].fillna(ohlc_data['close'], inplace=True)
    ohlc_data['high'].fillna(ohlc_data['close'], inplace=True)
    ohlc_data['low'].fillna(ohlc_data['close'], inplace=True)
    ohlc_data['product_id'].ffill(inplace=True)

    # reset the index
    ohlc_data.reset_index(inplace=True)

    ohlc_data['datetime'] = pd.to_datetime(ohlc_data['timestamp'], unit='ms')

    return ohlc_data



# Create taget feature column
#NOTE: Assuming that training and subsequent inference is interested in predicting %change movement over some ohlc_window_seconds - NOT REGRESSION BTW

def create_target_metric(
    ohlc_data: pd.DataFrame,
    ohlc_windows_seconds: int,
    prediction_window_sec: int,
) -> pd.DataFrame:
    """
    Create a target feature for each row, where the target feature value is the %change in it's future. (Row1 close price - Row(n) close price)/row1 

    %change is tied to how many bathces of time, which are greater or equal to the ohlc_window_seconds in the future. 

    Args:
        ohlc_data (pd.DataFrame): The OHLC data.
        ohlc_window_seconds (int): The size of the ohlc window in seconds.
        discretization_thresholds (list): The thresholds to discretize the close price.
        prediction_window_sec (int): The size of the prediction window in seconds.

    Returns:
        pd.DataFrame: The OHLC data with the target feature
    """

    #Basic check that ensures the prediction_window_sec is a multiple of ohlc_window_sec. prediction_window_sec/ohlc_windows_seconds needs to be an integer

    assert (
        prediction_window_sec % ohlc_windows_seconds == 0
    ), 'prediction_window_sec must be a multiple of ohlc_window_sec'

    # Integer corresponding to the number of ohlc candle sticks in the future to compare to the original row
    n_candles_into_future = prediction_window_sec // ohlc_windows_seconds

    # create a new column with the percentage change in the close price n_candles_into_future
    ohlc_data['close_pct_change'] = ohlc_data['close'].pct_change(n_candles_into_future)

    # shift the target column by n_candles_into_future to have the target for the current candle
    ohlc_data['target'] = ohlc_data['close_pct_change'].shift(-n_candles_into_future)

    # drop the close_pct_change column which is temporary
    ohlc_data.drop(columns=['close_pct_change'], inplace=True)

    #the last row won't have any future values to refer to in the data so remove the nan
    ohlc_data.dropna(subset=['target'], inplace=True)

    return ohlc_data