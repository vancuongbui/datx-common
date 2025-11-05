import logging
import boto3
import pandas as pd
from common.logging import setup_logging
from io import BytesIO
from botocore.client import BaseClient

setup_logging()


class Boto3Connector:
    def __init__(self, url: str, access_key: str, secret_key: str) -> None:
        self.url = url
        self.access_key = access_key
        self.secret_key = secret_key
        self.client: BaseClient = None
        self.initialize_cient()

    def initialize_cient(self) -> BaseClient:
        self.client = boto3.client(
            "s3",
            endpoint_url=self.url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            use_ssl=True,
        )

    def get_last_file_from_s3_folder(self, bucket: str, prefix: str) -> str | None:
        """
        Get lastest file from a s3 folder
        bucket: name of bucket
        prefix: prefix of the path to folder
        """
        try:
            response = self.client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            response_content = response.get("Contents", None)

            if response_content is None:
                return None

            file_name_list = []
            for obj in response_content:
                file_name = obj["Key"].split("/")[-1]

                if file_name.endswith("parquet") and len(file_name) == 16 and file_name[:8].isdigit():
                    file_name_list.append(file_name)

            file_name_list.sort(reverse=True)

            if len(file_name_list):
                return file_name_list[0]
            else:
                return None
        except Exception as e:
            logging.error(repr(e))
            return None

    def get_list_file_from_s3_folder(
        self,
        bucket: str,
        prefix: str,
        file_format_parquet=True,
        name_len=-1,
        is_digit=False,
    ) -> list:
        """
        Get a list of files from a s3 folder
        bucket: name of bucket
        prefix: prefix of the path to folder
        file_format_parquet: True if you want to get only parquet files, default is True
        name_len: length of file name, default is -1
        is_digit: True if you want to get only file name that is digit, default is False
        """
        try:
            paginator = self.client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

            file_name_list = []
            for page in pages:
                # response_content = page.get("Contents", None)
                for obj in page["Contents"]:
                    file_name = obj["Key"].split("/")[-1]
                    if file_format_parquet:
                        if not file_name.endswith("parquet"):
                            continue

                    if name_len != -1:
                        if len(file_name) != name_len:
                            continue

                    if is_digit:
                        if len(file_name.split(".")) != 2 or not file_name.split(".")[0].isdigit():
                            continue

                    file_name_list.append(file_name)
            return file_name_list
        except Exception as e:
            logging.warning(repr(e))
            return []

    def create_df_parquet_file(self, df: pd.DataFrame, bucket: str, parquet_path: str) -> None:
        """
        Create parquet file
        df: dataframe
        bucket: name of bucket
        parquet_path: path to parquet file
        """
        buffer = BytesIO()
        df.to_parquet(buffer)
        buffer.seek(0)
        self.client.upload_fileobj(buffer, bucket, parquet_path)

    def read_df_parquet_file(self, bucket: str, parquet_path: str) -> pd.DataFrame:
        """
        Read a parquet file, output is a dataframe
        bucket: name of bucket
        parquet_path: path to parquet file
        """
        try:
            buffer = BytesIO()
            self.client.download_fileobj(bucket, parquet_path, buffer)
            df = pd.read_parquet(buffer)
            return df
        except Exception as e:
            logging.error(f"read_df_parquet_file from {parquet_path} false", repr(e))
            return pd.DataFrame()

    def read_df_excel_file(self, bucket: str, excel_path: str, sheet_name: str = None) -> pd.DataFrame:
        """
        Read an excel file, output is a dataframe
        bucket: name of bucket
        excel_path: path to excel file
        """
        try:
            buffer = BytesIO()
            self.client.download_fileobj(bucket, excel_path, buffer)
            df = pd.read_excel(io=buffer, sheet_name=sheet_name)
            return df
        except Exception as e:
            logging.error(f"read_df_excel_file from {excel_path} false", repr(e))
            return pd.DataFrame()
