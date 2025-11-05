from common.data_feed.price_volume_data_feed import AsyncPriceVolumeDataFeed, PriceVolumeDataFeed
from common.data_feed.timescale.timescale_data_feed import AsyncTimeScaleDataFeed, TimeScaleDataFeed


class DataFeedFactory:
    @classmethod
    def PriceVolumeDataFeed(cls, db_config: dict, schema: str = "public") -> PriceVolumeDataFeed:
        """
        Creates a new instance of PriceVolumeDataFeed using the provided database configuration.

        Args:
            db_config (dict): A dictionary containing the configuration details for the database. Contains:
                - host: The host of the database.
                - port: The port of the database.
                - username: The username of the database.
                - password: The password of the database.
                - database_name: The name of the database.
                - schema: The schema that contains the price volumne tables.

        Returns:
            PriceVolumeDataFeed: A new instance of PriceVolumeDataFeed.
        """
        return TimeScaleDataFeed(db_config, schema)

    @classmethod
    def AsyncPriceVolumeDataFeed(cls, db_config: dict, schema: str = "public") -> AsyncPriceVolumeDataFeed:
        """
        Creates a new instance of AsyncPriceVolumeDataFeed using the provided database configuration.
        NOTE: You must close the data feed by calling `self.close()` after using it.

        Args:
            db_config (dict): A dictionary containing the configuration details for the database. Contains:
                - host: The host of the database.
                - port: The port of the database.
                - username: The username of the database.
                - password: The password of the database.
                - database_name: The name of the database.
                - schema: The schema that contains the price volumne tables.

        Returns:
            AsyncPriceVolumeDataFeed: A new instance of AsyncPriceVolumeDataFeed.
        """
        return AsyncTimeScaleDataFeed(db_config, schema)
