from quixstreams import Application
#TODO
# from src.config import config 

def kafka_to_feature_store(
        kafka_topic: str,
        kafka_borker_address: str,
        feature_group_name: str,
        feature_store_version: int,

) -> None:
    """

    This func sets up the consumer of data. This reads ohlc data and writes to the hopsworks feature store as defined by the 
    parameters inside the arguments feature_group_name and feature_store_version
    
    ARGS:
    kafka_topic (str): The topic it reads from 
    kafka_borker_address(str): Kafka broker address
    feature_group_name(str): Name of the featutre group
    feature_store_version (int): The version number 

    Return:
    None

    """

    app = Application(
        broker_address=kafka_borker_address,
        consumer_group="kafka_to_feature_store",
    )

    # input_topic = app.topic(name=kaka_input_topic, value_serializer='json')

    #Write data as a consumer
    with app.get_consumer() as consumer:
        consumer.subscribe(topics=kafka_topic)

        while True:
            msg = consumer.poll(1)

            breakpoint()
            
            if msg is None:
                continue
            elif msg.error():
                print('Kafka error;', msg.error())
                continue


            value = msg.value()


            consumer.store_offsets(message=msg)

if __name__== '__main__':
    kafka_to_feature_store(
        kafka_topic='ohlc',
        kafka_borker_address='localhost:9092',
        feature_group_name='ohlc_feature_group',
        feature_store_version=1,
    )





        

    


