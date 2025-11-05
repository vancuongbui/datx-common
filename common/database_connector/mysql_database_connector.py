from urllib.parse import quote

from sqlalchemy.dialects.mysql import insert

from .database_connector import DatabaseConnector


class MysqlDatabaseConnector(DatabaseConnector):
    def __init__(self, host, port, username, password, database_name):
        super().__init__(host, port, username, password, database_name)
        if self.port is None:
            self.port = 3306

    def create_uri(self) -> str:
        uri = f"mysql+mysqlconnector://{self.username}:{quote(self.password)}@{self.host}:{self.port}/{self.database_name}"
        return uri

    def gen_upsert_statement(self, df, table, keys, schema=None):
        orm_table = self.gen_table_instance_from_database(table, schema)
        data = df.to_dict("records")
        stmt = insert(orm_table).values(data)
        update_dict = {x.name: x for x in stmt.inserted if x.name not in keys and x.name in df.columns}
        stmt = stmt.on_duplicate_key_update(update_dict)
        return stmt
