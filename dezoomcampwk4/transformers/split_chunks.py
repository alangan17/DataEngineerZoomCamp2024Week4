if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import duckdb
import pandas as pd

def read_chunks(
    filename, filename_ext, file_path, chunk_size, schema_name
):
    """Read the file, get total rows, read by chunks"""

    conn = duckdb.connect(database=':memory:', read_only=False)

    # Determine the total number of rows in the Parquet file
    total_rows_query = (f"""
        SELECT COUNT(*) AS total 
        FROM read_parquet('{file_path}')
        """)
    total_rows_result = conn.execute(total_rows_query).fetchone()
    total_rows = total_rows_result[0]
    print(f"Total rows: {total_rows}")

    chunk_list = []

    # Process the Parquet file in chunks
    for offset in range(0, total_rows, chunk_size):
    # Chnage total_rows for unit test
    # for offset in range(0, 2100000, chunk_size):
        
        # chunk_df = conn.execute(chunk_query).fetchdf()
        # Process the chunk
        #print(f"Processing chunk: {offset // chunk_size + 1}, Rows: {len(chunk_df)}")

        chunk_query = (f"""
            FROM read_parquet('{file_path}') 
            LIMIT {chunk_size} 
            OFFSET {offset}
            """)

        chunk_seq = offset // chunk_size + 1
        chunk = {
            "chunk_seq": chunk_seq,
            "chunk_size": chunk_size,
            "chunk_query": chunk_query,
            "chunk_output_filename": f"{filename}_{chunk_seq}.{filename_ext}"
        }

        chunk_list.append(chunk)

    return chunk_list
        
def enforce_schema(df, schema_name):
    fill_values = {
        "VendorID": 0,
        "passenger_count": 0,
        "PULocationID": 0,
        "DOLocationID": 0,
        "payment_type": -999,
    }

    schemas = {
        "yello_taxi_schema": {
            "VendorID": "int64",
            "tpep_pickup_datetime": "datetime64[ns]",
            "tpep_dropoff_datetime": "datetime64[ns]",
            "passenger_count": "int64",
            "trip_distance": "float64",
            "RatecodeID": "float64",
            "store_and_fwd_flag": "str",
            "PULocationID": "int64",
            "DOLocationID": "int64",
            "payment_type": "int64",
            "fare_amount": "float64",
            "extra": "float64",
            "mta_tax": "float64",
            "tip_amount": "float64",
            "tolls_amount": "float64",
            "improvement_surcharge": "float64",
            "total_amount": "float64",
            "congestion_surcharge": "float64",
            "airport_fee": "float64"  # Note: Given as 0 non-null, indicating it may often be empty
        }
    }

    schema = schemas.get(schema_name)
    if schema is None:
        print(f"Schema '{schema_name}' not found.")
    print(f"Using schema '{schema_name}':")

    # Fill missing values according to the defined fill values
    for column, fill_value in fill_values.items():
        if column in df.columns:
            df[column] = df[column].fillna(fill_value)

    # Convert the DataFrame to conform to the adjusted schema
    for column, dtype in schema.items():
        if column in df.columns:  # Check if the column exists in the DataFrame
            # Use the pandas nullable integer and string types where appropriate
            df[column] = df[column].astype(dtype)
        else:
            # Handle missing columns by adding them with appropriate missing values
            df[column] = pd.NA
            df[column] = df[column].astype(dtype)
    
    return df

@transformer
def transform(data, *args, **kwargs):
    # Read the file into a DuckDB
    filename = data.get('filename')
    filename_ext = data.get('filename_ext')
    file_path = data.get('local_path')
    chunk_size = int(kwargs.get('t_chunk_size'))
    schema_name = kwargs.get('t_schema_name')

    chunk_list = read_chunks(
        filename = filename,
        filename_ext = filename_ext,
        file_path = file_path,
        chunk_size = chunk_size,
        schema_name = schema_name
    )

    return pd.DataFrame(chunk_list)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
