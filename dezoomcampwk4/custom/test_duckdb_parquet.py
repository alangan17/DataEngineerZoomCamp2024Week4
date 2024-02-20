if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import duckdb

@custom
def transform_custom(*args, **kwargs):
    """
    args: The output from any upstream parent blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    df = duckdb.sql(f"""
        SELECT SUM(passenger_count) AS passenger_count
        -- FROM read_parquet('/home/src/data/test/*.parquet') --11030483 12015913
        FROM read_parquet('/home/src/data/yellow_tripdata_2019-02.parquet')
    """).df()

    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'