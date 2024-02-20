if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import requests

class DownloadError(Exception):
    pass

# Function to download a Parquet file from a given URL
def download_file(url, local_path, filename, filename_ext):
    print(f"{url = }")
    print(f"{local_path = }")

    try:
        response = requests.get(url, stream=True)
        print(f"{response.status_code=}")
        if response.status_code == 200:
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print("File downloaded successfully.")

            return {
                "url": url,
                "local_path": local_path,
                "filename": filename,
                "filename_ext": filename_ext,
                "status_code": int(response.status_code)
            }
        elif response.status_code == 404:
            raise DownloadError("Error 404: The requested URL was not found on this server.")
        else:
            raise DownloadError(f"Failed to download the file. Status code: {response.status_code}")
    except Exception as e:
        raise DownloadError(f"An error occurred: {e}")



@data_loader
def load_data(*args, **kwargs):
    """
    Get variables then trigger file download action
    """

    # URL of the Parquet file
    base_url = kwargs.get('src_base_url')
    data_type = kwargs.get('src_data_type')
    data_year = kwargs.get('src_data_year')
    data_month = kwargs.get('src_data_month')
    data_ext = kwargs.get('src_data_ext')

    filename = f"{data_type}_{data_year}-{data_month}"

    download_file_meta = download_file(
        url = f"{base_url}/{data_type}_{data_year}-{data_month}.{data_ext}",
        local_path = f"/home/src/data/{filename}.{data_ext}",
        filename = filename,
        filename_ext = data_ext
    )

    return download_file_meta


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

@test
def test_status_code(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output.get('status_code') == 200, 'Status code must be 200'
