if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import duckdb

@transformer
def transform(data, *args, **kwargs):
    print(data)

    # Get chunk dataframe
    conn = duckdb.connect(database=':memory:', read_only=False)

    # Determine the total number of rows in the Parquet file
    df = total_rows_result = conn.execute(data.get('chunk_query')).df()

    print("Schema before changing:")
    print(df.info())

    schema_name = kwargs.get('t_schema_name')

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
    
    print("Schema after changing:")
    print(df.info())

    df['chunk_output_filename'] = data.get('chunk_output_filename')

    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
