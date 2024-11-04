# main.py (Modified for Manual Ingestion Test)

from quixstreams import Application
from loguru import logger 
import json
import pandas as pd
from typing import Optional
from hopsworks_features import push_data_to_feature_store
from config import config_kafka_to_hops

def get_current_utc_sec() -> int:
    """
    Returns the current UTC time expressed in seconds since the epoch.
    """
    from datetime import datetime, timezone
    return int(datetime.now(timezone.utc).timestamp())

# Define your manual test trades here
test_trades = [
    {'timestamp': 6928000000000, 'product_id': 'BTC/USD', 'open': 56000.0, 'high': 56200.0, 'low': 55900.0, 'close': 56100.0},
    {'timestamp': 6928006000000, 'product_id': 'ETH/USD', 'open': 3500.0, 'high': 3550.0, 'low': 3490.0, 'close': 3520.0},
]

def kafka_to_feature_store(
        kafka_topic: str,
        kafka_broker_address: str,
        feature_group_name: str,
        feature_store_version: int,
        buffer_size: Optional[int]=1,
        live_or_historical: Optional[str]='historical', 
        save_every_n_sec: Optional[int] = 600,
        create_new_consumer_group: Optional[bool]=False,
) -> None:
    """
    Function that simulates consuming JSON-formatted trade data and writing it to the feature store.
    """
    buffer = []

    for trade_data in test_trades:  # No need for json.loads(), as test_trades are dictionaries
        # Append the data to the buffer
        buffer.append(trade_data)
        logger.debug(f'Appended trade: {trade_data} to buffer. Buffer size={len(buffer)}')

        # If buffer is full or save interval is reached, push data to feature store
        if len(buffer) >= buffer_size:
            try:
                push_data_to_feature_store(
                    feature_group_name=feature_group_name,
                    feature_group_version=feature_store_version,
                    data=buffer,
                    online_or_offline='offline' if live_or_historical == 'historical' else 'online'
                )
                logger.debug('Data pushed to the feature store')
                
                # Clear the buffer after successful push
                buffer = []

            except Exception as e:
                logger.error(f'Failed to push data to the feature store: {e}')

# Only execute the ingestion if running the script directly
if __name__ == '__main__':
    kafka_to_feature_store(
        kafka_topic=config_kafka_to_hops.kafka_topic_name,
        kafka_broker_address=config_kafka_to_hops.kafka_broker_address,
        feature_group_name=config_kafka_to_hops.feature_group_name,
        feature_store_version=config_kafka_to_hops.feature_group_version,
        buffer_size=config_kafka_to_hops.buffer_size,
        live_or_historical=config_kafka_to_hops.live_or_historical,
        save_every_n_sec=config_kafka_to_hops.save_every_n_sec,
        create_new_consumer_group=config_kafka_to_hops.create_new_consumer_group,
    )
