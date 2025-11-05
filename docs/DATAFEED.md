# Data Feed

## Price volume

### Initialize connector

```python
from common.data_feed.factory import DataFeedFactory

config = {
    "host": "",
    "port": "",
    "username": "",
    "password": "",
    "database_name": "",
}
schema = ""
price_volume_data_feed = DataFeedFactory.PriceVolumeDataFeed(config, schema)
```

### Get ohlcv

```python
price_df = price_volume_data_feed.query_ohlcv(
        symbol=["ACB"],
        resolution="day",
        start_date=datetime.datetime.utcnow().date(),
)
```
