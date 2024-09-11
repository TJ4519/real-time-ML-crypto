## Servless machine learning pipeline for real-time classification and short-term momentum trading for currency pairs

![ML Pipeline plan]![alt text](image-1.png)
Fig 1. Schema of pipeline components. Data Source is a websocket or restapi from Kraken. Feature pipeline includes three microservices (trade_producer, trade_to_ohlc, kafka_to_feature_store). 

## Current Status


### Feature Pipeline
- **Trade Producer Microservice**: Runs locally and within Docker containers
- **Trade to OHLC Conversion**: Runs locally and within Docker containers
- **Trade to OHLC Conversion**: Runs locally and within Docker containers 
- **Note**: Whilst all three services are production ready, desired changes to restapi.py file still needed to accomodate multiple currency pairs (list)

### Data Management
- **OHLC Data Storage**: The process to store OHLC data in the Hopsworks Feature Store needs to be addressed and API 