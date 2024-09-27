from pydantic import BaseModel


class Trade(BaseModel):
    product_id: str # Each trade object has one product_id, the product_ids is a config variable to be passed as multiple strings
    price:float
    timestamp_ms: int #converted from timsetamp to timestamp_ms (ms for microsenconds). This is done to standardise the timestamps recieved from websocket and restapi. 
    volume: float
