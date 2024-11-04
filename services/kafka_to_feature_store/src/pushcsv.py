from config import config_kafka_to_hops
import hopsworks
from hsfs.feature_group import FeatureGroup
import pandas as pd
from loguru import logger
# from dotenv import load_dotenv
import os

# load_dotenv

#%% Read from file and write to dataframe
def connect_to_hopsworks():
    project = hopsworks.login(
        project=os.getenv("HOPSWORKS_PROJECT_NAME"),
        api_key_value=os.getenv("HOPSWORKS_API_KEY")
    )
    return project

def get_feature_group( 
        project, 
        feature_group_name, 
        feature_group_version, 
        online_enabled=True 
        ):
    
    feature_store = project.get_feature_store()
    feature_group = feature_store.get_or_create_feature_group(
        name=feature_group_name,
        version=feature_group_version,
        #description='OHLC data feature group',
        primary_key=['product_id', 'timestamp'],
        event_time='timestamp',
        online_enabled=online_enabled  # Set True for online, False for offline
    )
    return feature_group

def load_csv_data(csv_file_path):
    return pd.read_csv(csv_file_path)

def push_one_row_to_feature_group(feature_group, data):
    # Extract the first row as a DataFrame
    first_row = data.head(1)

    # # Explicitly cast product_id to string
    # first_row['product_id'] = first_row['product_id'].astype(str)

    # Debugging - check the row before inserting
    print("First row to insert (after casting product_id to string):\n", first_row)
    
    # Insert the first row into the feature store
    feature_group.insert(
        first_row,
        write_options={"start_offline_materialization": False}  # Change to False for online
    )
    print("Pushed one row to the feature store.")

def main():
    # File path for the CSV
    csv_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'ohlc_data.csv')
    
    # Step 1: Connect to Hopsworks
    project = connect_to_hopsworks()
    
    # Step 2: Get the feature group (online or offline)
    feature_group_name = 'ohlc_feature_group_v4'  # Adjust this as needed
    feature_group_version = 1  # Adjust this as needed
    feature_group = get_feature_group(project, feature_group_name, feature_group_version, online_enabled=True)  # Change to False for offline store
    
    # Step 3: Load the CSV data
    data = load_csv_data(csv_file_path)
    
    # Step 4: Push one row to the feature store
    push_one_row_to_feature_group(feature_group, data)
    
if __name__ == '__main__':
    main()

# %%
# Define the OhlcDataWriter class, similar to your tutor's code
# class OhlcDataWriter:
    # """
    # A class to help us write OHLC data from a pandas DataFrame to the feature store.
    
    # The Hopsworks credentials are read from environment variables:
    # - HOPSWORKS_PROJECT_NAME
    # - HOPSWORKS_API_KEY
    # """
    # def __init__(
    #         self, hopsworks_project_name: str, 
    #         hopsworks_api_key: str, 
    #         feature_group_name: str, 
    #         feature_group_version: int):
        

    #     self.feature_group_name = feature_group_name
    #     self.feature_group_version = feature_group_version
    #     self.hopsworks_project_name = hopsworks_project_name
    #     self.hopsworks_api_key = hopsworks_api_key
        
    #     # breakpoint()

    # def write_from_csv(self, csv_file_path: str, rows_to_ingest: int = None):

    #     feature_group = self._get_feature_group()

    #      # Read the data from the CSV file
    #     data = pd.read_csv(csv_file_path)
        
    #     # If rows_to_ingest is specified, take only the first `rows_to_ingest` rows
    #     if rows_to_ingest is not None:
    #         data = data.head(rows_to_ingest)
    #         logger.info(f"Ingesting {rows_to_ingest} row(s) from the CSV file.")

    #     # Insert the DataFrame into the feature group
    #     feature_group.insert(
    #         data,
    #         write_options={
    #             'start_offline_materialization': True
    #         },
    #     )
    #     logger.info(f"Data from {csv_file_path} has been successfully ingested.")

    # def _get_feature_group(self) -> FeatureGroup:
    #     """
    #     Returns (and possibly creates) the feature group we will be writing to.
    #     """
    #     # Authenticate with Hopsworks API
    #     project = hopsworks.login(
    #         project=self.hopsworks_project_name,
    #         api_key_value=self.hopsworks_api_key,
    #     )

    #     # Get the feature store
    #     feature_store = project.get_feature_store()

    #     # Create or retrieve the feature group
    #     feature_group = feature_store.get_or_create_feature_group(
    #         name=self.feature_group_name,
    #         version=self.feature_group_version,
    #         description='OHLC data coming from Kraken CSV ingestion',
    #         primary_key=['product_id', 'timestamp'],
    #         event_time='timestamp',
    #         online_enabled=True,  # Set to True for online feature store usage
    #     )

    #     return feature_group

# def main():
#     # File path for the CSV file (hardcoded or passed as argument)
#     base_dir = os.path.dirname(os.path.abspath(__file__))
#     csv_file_path = os.path.join(base_dir, 'ohlc_data.csv')
#     print(f"Using CSV Path: {csv_file_path}")
    
#     # Initialize the data writer using your config object
#     writer = OhlcDataWriter(
#         hopsworks_project_name=config_kafka_to_hops.hopsworks_project_name,
#         hopsworks_api_key=config_kafka_to_hops.hopsworks_api_key,
#         feature_group_name=config_kafka_to_hops.feature_group_name,
#         feature_group_version=config_kafka_to_hops.feature_group_version,
#     )
    
#     # Write only one row to the feature store for testing
#     writer.write_from_csv(csv_file_path, rows_to_ingest=None)
#     logger.debug(f'OHLC data (one row) from CSV was saved to {config_kafka_to_hops.feature_group_name}-{config_kafka_to_hops.feature_group_version}')

# if __name__ == '__main__':
#     main()