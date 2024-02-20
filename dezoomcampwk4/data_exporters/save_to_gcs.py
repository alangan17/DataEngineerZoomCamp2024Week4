from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data_to_google_cloud_storage(data, **kwargs) -> None:
    if isinstance(data, list):
        data = DataFrame(data)

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    bucket_name = kwargs.get('l_gcs_bucket_name')
    object_key = data.iloc[0]['chunk_output_filename']

    print(f"Attempt to save data to {bucket_name}/{object_key}")

    GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile)).export(
        data,
        bucket_name,
        object_key,
    )