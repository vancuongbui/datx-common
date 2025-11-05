# Install

```
pip install --upgrade --no-deps --force-reinstall git+https://common-read:e7jrbtsJzDQzsikDiVRe@git.datx.com.vn/rnd-group/common.git
```

# Logging

Setup logging

```python
from common.logging import setup_logging

setup_logging()
```

# Database Connector

## Initialize connector to an database

```python
from common.database_connector import factory
connector = factory.get_connector('postgres', host='10.0.255.2', port='32423', username='postgres', password='*****', database_name='social')
```

## Read data from sql query

```python
sql = '''
Select * from ticker_temp_index;
'''
df = connector.query_by_sql(sql, chunk=False)
```

## Create a new table using SQLAlchemy Schema

https://docs.sqlalchemy.org/en/20/core/metadata.html

```python
from sqlalchemy import Column, String, Integer, DateTime
table = 'sentiment_snapshot_index'
columns = [
    Column("symbol",String(10),primary_key=True),
    Column("sentiment_score",Integer),
]
connector.create_table(table, columns)
```

## Upsert dataframe to a table in database

### The given table in database already has primary key

```python
df = pd.DataFrame()
table = 'sentiment_snapshot_index'
connector.upsert(df, table, keys=['symbol'], primary_key=True)
```

### The given table in database doesn't have primary key

```python
df = pd.DataFrame()
table = 'sentiment_snapshot_index'
connector.upsert(df, table, keys=['symbol'], primary_key=False)
```

### Generate updated_at column

```python
df = pd.DataFrame()
table = 'sentiment_snapshot_index'
connector.upsert(df, table, keys=['symbol'], include_updated_at=True)
```

## S3 Connector

```python
from common.s3_connector import Boto3Connector
connector = Boto3Connector(url="https://aws.s3", access_key="abcd", secret_key="efgh")
```

### Get last file from an S3 folder

```python
connector.get_last_file_from_s3_folder(bucket="finnpro", prefix="mess_get_news_vi")

```

### Get list of files from an S3 folder

```python
connector.get_list_file_from_s3_folder(bucket="fiinpro", prefix="mess_get_news_vi")

```

### Create a Parquet file from a dataframe

```python
connector.create_df_parquet_file(df=df, bucket="fiinpro", parquet_path="mess_get_news_en/mess_get_news_en_2017:02:02_2017_03_02.parquet")

```

### Read a parquet file to a dataframe

```python
connector.read_df_parquet_file(bucket="fiinpro", parquet_path="mess_get_news_en/mess_get_news_en_2017:02:02_2017_03_02.parquet")

```
