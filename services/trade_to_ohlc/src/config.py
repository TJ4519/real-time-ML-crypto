import os
# read from env for local broker address--not hard coding in the config
from dotenv import load_dotenv, find_dotenv

# Load the environment variables from the .env file
load_dotenv(find_dotenv())


from pydantic_settings import BaseSettings

# Defining a configuration class to handle settings more efficiently
class Config(BaseSettings):
    kafka_broker_address: str = os.environ['KAFKA_BROKER_ADDRESS']
    kafka_input_topic: str = 'trade'
    kafka_output_topic: str = 'ohlc'
    ohlc_windows_seconds: int = os.environ['OHLC_WINDOWS_SECONDS']

# Creating a configuration instance
config = Config()