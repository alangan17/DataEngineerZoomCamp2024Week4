if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from pandas import DataFrame

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import polars as pl
import os

# Function to fill null values for a column
def fill_null(column, fill_value):
    if pa.types.is_floating(column.type) or pa.types.is_integer(column.type):
        return pc.fill_null(column, pa.scalar(fill_value, type=column.type))
    elif pa.types.is_string(column.type):
        return pc.fill_null(column, pa.scalar(fill_value))
    # Add more conditions as necessary for other types
    return column

def pandas_manipulation(df: DataFrame):
    # Fill missing values according to the defined fill values
    print(f"Attemping fill null values...")
    for column, fill_value in fill_values.items():
        if column in df.columns:
            try:
                df[column] = df[column].fillna(fill_value)
            except OutOfBoundsDatetime as e:
                print(f"{column} encountered exception: {e}, forcing to be null value")
                df[column] = pd.to_datetime(date_col_to_force, errors = 'coerce')
            except Exception as e:
                print(f"{column} encountered exception: {e}")
                raise e

    # Convert the DataFrame to conform to the adjusted schema
    print(f"Attemping changing column data type...")
    for column, dtype in schema.items():
        if column in df.columns:  # Check if the column exists in the DataFrame
            # Use the pandas nullable integer and string types where appropriate
            try:
                df[column] = df[column].astype(dtype)
            except OutOfBoundsDatetime as e:
                print(f"{column} encountered exception: {e}, forcing to be null value")
                df[column] = pd.to_datetime(date_col_to_force, errors = 'coerce')
            except Exception as e:
                print(f"{column} encountered exception: {e}")
                raise e
        else:
            print(f"{column} is not in the schema")
            df[column] = df[column]
    
    print("Schema after changing:")
    print(df.info())

    df['chunk_output_filename'] = data.get('chunk_output_filename')

    return df

@transformer
def transform(data, *args, **kwargs):
    print(data)

    # Get chunk dataframe
    conn = duckdb.connect(database=':memory:', read_only=False)
    try:
        # df = conn.execute(data.get('chunk_query')).df()
        df = conn.execute(data.get('chunk_query')).arrow()
    except Exception as e:
        print(f"encountered exception: {e}")
        raise e



    print("Schema before changing:")
    #print(df.info())
    print(df.schema)
    print("\n")

    schema_name = kwargs.get('t_schema_name')


    fill_values = {
        "VendorID": 0,
        "passenger_count": 0,
        "PULocationID": 0,
        "DOLocationID": 0,
        "payment_type": -999,
        "ehail_fee": 0,
    }

    schemas = {
        "yellow_taxi_schema": {
            "VendorID": "int64",
            "tpep_pickup_datetime": "datetime64[us]",
            "tpep_dropoff_datetime": "datetime64[us]",
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
        },

        "green_taxi_schema": {
            "VendorID": "int64",
            "lpep_pickup_datetime": "datetime64[us]",
            "lpep_dropoff_datetime": "datetime64[us]",
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
            "ehail_fee": "float64",
            "improvement_surcharge": "float64",
            "total_amount": "float64",
            "congestion_surcharge": "float64",
        },

        "fhv_taxi_schema": {
            "dispatching_base_num": "str",
            "pickup_datetime": "datetime64[us]",
            "dropOff_datetime": "datetime64[us]",
            "PUlocationID": "int64",
            "DOlocationID": "int64",
            "SR_Flag": "int64",
            "Affiliated_base_number": "str",
        }
    }

    schema = schemas.get(schema_name)
    assert schema is not None, f"Schema '{schema_name}' not found."


    print(f"Using {schema_name}: {schema}\n")
    # Apply fill values
    for column_name, fill_value in fill_values.items():
        if column_name in df.column_names:
            column_index = df.column_names.index(column_name)
            column = df.column(column_index)
            filled_column = fill_null(column, fill_value)
            df = df.set_column(column_index, df.field(column_name), filled_column)
    
    # Construct the target schema
    fields = [pa.field(name, pa.from_numpy_dtype(dtype)) for name, dtype in schema.items()]
    target_schema = pa.schema(fields)

    # Cast the table to the new schema
    try:
        df = df.cast(target_schema)
    except pa.ArrowInvalid as e:
        print(f"Encountered exception during casting: {e}")
        raise e

    print("Schema after changing:")
    print(df.schema)

    #df['chunk_output_filename'] = data.get('chunk_output_filename')
    df = df.append_column('chunk_output_filename', pa.array([data.get('chunk_output_filename')] * len(df), pa.string()))

    print(type(df))

    output_path = f"/home/src/data/chunk_temp/{data.get('chunk_output_filename')}"
    # pq.write_table(df, output_path)

    # Pyarrow write to gcs
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = kwargs.get('l_gcp_key_path')
    bucket_name = kwargs['l_gcs_bucket_name']
    project_id = kwargs['l_gcp_project_id']
    root_path = f"{bucket_name}/{data.get('chunk_output_filename')}"

    gcs = pa.fs.GcsFileSystem()

    pq.write_table(
        df, 
        where = root_path,
        filesystem=gcs
    )

    data['gcp_project_id'] = project_id
    data['gcs_bucket_name'] = bucket_name
    data['gcs_path'] = root_path
    data['row_count'] = int(len(df))

    return DataFrame([data])


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
