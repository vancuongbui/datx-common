import logging
from typing import List, Union

import numpy as np
import pandas as pd
from sqlalchemy import Column, MetaData, Table, create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)


class DatabaseConnector:
    def __init__(
        self,
        host=None,
        port=None,
        username=None,
        password=None,
        database_name=None,
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database_name = database_name
        self.uri = self.create_uri()
        self.engine = self.init_engine(self.uri)

    def create_uri(self) -> str:
        raise NotImplementedError

    def init_engine(self, uri):
        engine = create_engine(uri, connect_args={'connect_timeout': 3600})
        return engine

    def query_by_sql(self, sql: str, chunk=False, chunk_size=5000) -> Union[pd.DataFrame, List[pd.DataFrame]]:
        """Query Dataframe from database using SQL query

        Args:
            sql (str): SQL Query

        Returns:
            Union[pd.DataFrame, List[pd.DataFrame]]: result, return list if retrieval in chunk
        """
        with self.engine.connect() as conn:
            df = pd.read_sql(text(sql), conn, chunksize=chunk_size if chunk else None)
        return df

    def upsert(
        self,
        df: pd.DataFrame,
        table: str,
        keys: List[str],
        primary_key=False,
        include_updated_at=False,
        schema: str = None,
        auto_create_table=True,
    ):
        df = df.replace({np.nan: None})
        if include_updated_at:
            df["updated_at"] = pd.Timestamp.utcnow()
        if primary_key:
            self.upsert_with_primary_keys(df, table, keys, schema=schema, auto_create_table=auto_create_table)
        else:
            self.upsert_with_non_primary_keys(df, table, keys, schema=schema, auto_create_table=auto_create_table)

    def insert(self, df: pd.DataFrame, table: str, schema=None, if_exists: str = "append"):
        df.to_sql(table, if_exists=if_exists, index=False, con=self.engine, schema=schema)

    def upsert_with_non_primary_keys(
        self,
        df: pd.DataFrame,
        table: str,
        keys: List[str],
        schema=None,
        auto_create_table=False,
    ):
        from .mysql_database_connector import MysqlDatabaseConnector
        table_existed = self.check_table_exists(table, schema)
        if schema:
            table_name = f"{schema}.{table}"
        else:
            table_name = table

        if not table_existed:
            if auto_create_table:
                df.to_sql(
                    table,
                    schema=schema,
                    if_exists="append",
                    index=False,
                    con=self.engine,
                )
                return
            else:
                raise AssertionError(f"The table name {table} does not exists, create it first.")

        current_db_records = self.query_by_sql(f"Select count(1) as num_records from {table_name}")
        current_db_records = current_db_records["num_records"].values[0]
        if current_db_records > 20000:
            raise AssertionError("Cant upsert with non primary keys in large table (>20000 rows)")
        current_db_df = self.query_by_sql(f"Select * from {table_name}")
        db_key_values = current_db_df[keys].drop_duplicates().values.tolist()
        update_rows = []
        insert_rows = []
        for _, row in df.iterrows():
            row_key_values = [row[k] if not pd.isna(row[k]) else None for k in keys]
            if row_key_values in db_key_values:
                update_rows.append(row)
            else:
                insert_rows.append(row)
        logger.info(f"{len(update_rows)=}")
        for row in update_rows:
            sql = f"""UPDATE {table_name}
"""
            where_condition = []
            set_stm = []
            for col in row.index:
                if pd.isna(row[col]):
                    if col in keys:
                        if isinstance(self, MysqlDatabaseConnector):
                            where_condition.append(f""" `{col}` is null """)
                        else:
                            where_condition.append(f""" "{col}" is null """)
                    else:
                        if isinstance(self, MysqlDatabaseConnector):
                            set_stm.append(f""" `{col}`=null """)
                        else:
                            set_stm.append(f""""{col}"=null """)
                else:
                    if col in keys:
                        if isinstance(self, MysqlDatabaseConnector):
                            where_condition.append(f""" `{col}`='{row[col]}' """)
                        else:
                            where_condition.append(f""""{col}"='{row[col]}' """)
                    else:
                        if isinstance(self, MysqlDatabaseConnector):
                            set_stm.append(f""" `{col}`='{row[col]}' """)
                        else:
                            set_stm.append(f""""{col}"='{row[col]}' """)
            sql += " SET " + ", ".join(set_stm)
            sql += " WHERE " + " AND ".join(where_condition)
            self.execute_sql(sql)
        logger.info(f"{len(insert_rows)=}")
        if len(insert_rows):
            pd.DataFrame(insert_rows).to_sql(table, if_exists="append", index=False, con=self.engine, schema=schema)

    def upsert_with_primary_keys(
        self,
        df: pd.DataFrame,
        table: str,
        keys: List[str],
        check: bool = True,
        schema: str = None,
        auto_create_table=True,
    ):
        table_existed = self.check_table_exists(table, schema)
        # If table is not exists in database, dump data directly
        if not table_existed:
            logger.info("Table is not existed yet")
            if auto_create_table:
                logger.info("Create new table")
                df.to_sql(
                    table,
                    schema=schema,
                    if_exists="append",
                    index=False,
                    con=self.engine,
                )
                return
            else:
                raise AssertionError(f"The table name {table} does not exists, create it first.")
        if check:
            # Check whether table and columns exists in database or not:
            self.upsert_check_database(table, keys, schema=schema)
            # Check key unique
            self.upsert_check_df(df, keys)

        conn = self.engine.connect()
        trans = conn.begin()
        try:
            stmt = self.gen_upsert_statement(df, table, keys, schema)
            conn.execute(stmt)
            trans.commit()
        except SQLAlchemyError as e:
            trans.rollback()
            logger.error(repr(e))
            logger.error(type(e))
            logger.error(str(e.orig))
            raise e
        except Exception as error:
            trans.rollback()
            logger.error(repr(error))
            raise error
        finally:
            conn.close()

    def gen_upsert_statement(self, df, table, keys, schema):
        raise NotImplementedError

    def upsert_check_database(self, table_name, columns: List[str], schema: str = None) -> None:
        """Check whether given table and columns exists or not
        Args:
            table_name (str): name of table need to check
            columns (List[str]): list of column names
        """
        try:
            sql = f"""Select "{'","'.join(columns)}"
from {table_name if not schema else schema + '.' + table_name}
limit 1;
            """
            self.query_by_sql(sql)
        except Exception as error:
            raise error

    def upsert_check_df(self, df: pd.DataFrame, keys: List[str]) -> None:
        """Check whether values in keys columns of df are unique or not

        Args:
            df (pd.DataFrame): _description_
            keys (List[str]): _description_
        """
        num_duplicates = df.duplicated(subset=keys).sum()
        if num_duplicates:
            raise ValueError("Key Columns in given DataFrame are not unique!")

    def upsert_check_columns(self, table, df):
        print("Checking columns..")
        df_columns = df.columns.tolist()
        table_columns = self._retrieve_table_columns(table)
        if table_columns is None:
            # Table not exists
            return
        print(f"{df_columns=}")
        print(f"{table_columns=}")

        if len(set(df_columns).union(set(table_columns))) != len(set(df_columns).intersection(set(table_columns))):
            raise ValueError("Columns in DataFrame are not corresponding to database table's columns")

    def _retrieve_table_columns(self, table_name):
        try:
            table_df = self.query_by_sql(f"select * from {table_name} limit 1")
            return table_df.columns.tolist()
        except Exception as error:
            logger.error(repr(error))
            # Table not exists
            return

    def check_table_exists(self, table_name, schema):
        return inspect(self.engine).has_table(table_name, schema)

    def gen_table_instance_from_database(self, table_name, schema=None):
        metadata = MetaData(schema=schema)
        inspector = inspect(self.engine)
        table_columns = inspector.get_columns(table_name, schema)
        table = Table(
            table_name,
            metadata,
            *(Column(column["name"], column["type"]) for column in table_columns),
            extend_existing=True,  # Extend the existing schema
            schema=schema,
        )
        return table

    def execute_sql(self, sql: str):
        with self.engine.connect() as conn:
            conn.execute(text(sql))
            conn.commit()

    def get_primary_keys(self, table, schema):
        return [k.name for k in inspect(self.gen_table_instance_from_database(table, schema)).primary_key]

    def get_unique_constraint(self, table):
        records = inspect(self.engine).get_unique_constraints(table)
        result = []
        for r in records:
            result.append(sorted(r["column_names"]))
        return

    def create_table(self, table, columns: List[Column] = [], schema: str = None):
        metadata = MetaData(schema=schema)
        table = Table(table, metadata, *columns, schema=schema)
        metadata.create_all(bind=self.engine, checkfirst=True)

    def execute_transaction(self, statements: list[str], autocommit=False):
        if not autocommit:
            with self.engine.begin() as conn:
                for statement in statements:
                    conn.execute(text(statement))
        else:
            with self.engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                for statement in statements:
                    conn.execute(text(statement))
