from typing import List, Literal
from .database_connector import DatabaseConnector
from .mysql_database_connector import MysqlDatabaseConnector
from .postgres_database_connector import PostgresDatabaseConnector


class DatabaseConnectorFactory(object):
    def __init__(self):
        self.creators = {}

    def get_connector(
        self,
        database_type: Literal["postgres", "mysql", "timescale"],
        host: str,
        port: int,
        username: str,
        password: str,
        database_name: str,
        *args,
        **kwargs,
    ) -> DatabaseConnector:
        creator = self.creators.get(database_type)
        if not creator:
            raise AssertionError(f"Database {database_type} is not supported")
        return creator(host, port, username, password, database_name, *args, **kwargs)

    def register_connector(
        self,
        database_type: Literal["postgres", "mysql", "timescale"],
        creator: DatabaseConnector,
    ) -> None:
        self.creators[database_type] = creator

    @property
    def supported_types(self) -> List[str]:
        return self.creators.keys()


database_connector_factory = DatabaseConnectorFactory()
database_connector_factory.register_connector("postgres", PostgresDatabaseConnector)
database_connector_factory.register_connector("timescale", PostgresDatabaseConnector)
database_connector_factory.register_connector("mysql", MysqlDatabaseConnector)
