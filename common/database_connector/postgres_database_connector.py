from urllib.parse import quote

from sqlalchemy.dialects.postgresql import insert

from .database_connector import DatabaseConnector


class PostgresDatabaseConnector(DatabaseConnector):
    def __init__(self, host, port, username, password, database_name):
        super().__init__(host, port, username, password, database_name)
        if self.port is None:
            self.port = 5432

    def create_uri(self) -> str:
        uri = (
            f"postgresql+psycopg2://{self.username}:{quote(self.password)}@{self.host}:{self.port}/{self.database_name}"
        )
        return uri

    def gen_upsert_statement(self, df, table, keys, schema):
        orm_table = self.gen_table_instance_from_database(table, schema)
        stmt = insert(orm_table).values(df.to_dict("records"))
        stmt = stmt.on_conflict_do_update(
            index_elements=keys,
            set_={
                col: stmt.excluded[col] for col in df.columns.difference(keys).tolist()
            },
        )
        return stmt