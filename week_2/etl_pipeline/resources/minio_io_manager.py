import os
from contextlib import contextmanager
from datetime import datetime
from typing import Union

import pandas as pd
from dagster import IOManager, OutputContext, InputContext
from minio import Minio

@contextmanager
def connect_minio(config):
    """Establish a connection with MinIO."""
    client = Minio(
        endpoint=config.get("endpoint_url"),
        access_key=config.get("aws_access_key_id"),
        secret_key=config.get("aws_secret_access_key"),
        secure=False,
    )
    
    try:
        yield client
    except Exception as e:
        raise e


class MinIOIOManager(IOManager):
    def __init__(self, config):
        self.config = config

    def _get_path(self, context: Union[InputContext, OutputContext]):
        """Generate object storage path and temporary file path."""
        layer, schema, table = context.asset_key.path
        key = "/".join([layer, schema, table.replace(f"{layer}_", "")])
        tmp_file_path = "/tmp/file-{}.parquet".format(datetime.today().strftime("%Y%m%d%H%M%S"))
        return f"{key}.pq", tmp_file_path

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        """Save a pandas DataFrame to MinIO."""
        key_name, tmp_file_path = self._get_path(context)

        try:
            bucket_name = self.config.get("bucket_name")

            # Convert DataFrame to Parquet and save temporarily
            obj.to_parquet(tmp_file_path)

            # Upload to MinIO
            with connect_minio(self.config) as client:
                found = client.bucket_exists(bucket_name)
                if not found:
                    client.make_bucket(bucket_name)

                client.fput_object(
                    bucket_name=bucket_name,
                    object_name=key_name,
                    file_path=tmp_file_path,
                )

            # Clean up temporary file
            os.remove(tmp_file_path)

        except Exception as e:
            if os.path.exists(tmp_file_path):
                os.remove(tmp_file_path)
            raise e

    def load_input(self, context: InputContext) -> pd.DataFrame:
        """Load a pandas DataFrame from MinIO."""
        key_name, tmp_file_path = self._get_path(context)

        try:
            bucket_name = self.config.get("bucket_name")

            with connect_minio(self.config) as client:
                found = client.bucket_exists(bucket_name)
                if not found:
                    raise ValueError(f"Bucket {bucket_name} does not exist.")
                client.fget_object(
                    bucket_name=bucket_name,
                    object_name=key_name,
                    file_path=tmp_file_path,
                )

            # Load Parquet file into a DataFrame
            pd_data = pd.read_parquet(tmp_file_path)

            # Clean up temporary file after loading
            os.remove(tmp_file_path)

            return pd_data

        except Exception as e:
            if os.path.exists(tmp_file_path):
                os.remove(tmp_file_path)
            raise e
