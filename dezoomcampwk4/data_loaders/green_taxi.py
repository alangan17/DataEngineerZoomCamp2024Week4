from mage_ai.orchestration.triggers.api import trigger_pipeline
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def trigger(*args, **kwargs):
    """
    Trigger another pipeline to run.

    Documentation: https://docs.mage.ai/orchestration/triggers/trigger-pipeline
    """
    for yyyy in range(2019, 2021):
        for mm in range(1, 13):
            print(f"{yyyy}-{mm:02d}")

            trigger_pipeline(
                'api_to_gcs',        # Required: enter the UUID of the pipeline to trigger
                variables={
                    "l_gcp_key_path": "/home/src/keys/gcp_sa.json",
                    "l_gcp_project_id": "dez2024-413305",
                    "l_gcs_bucket_name": "dez2024-wk-nytaxi-mage-green",
                    "src_base_url": "https://d37ci6vzurychx.cloudfront.net/trip-data",
                    "src_data_ext": "parquet",
                    "src_data_month": f"{mm:02d}",
                    "src_data_type": "green_tripdata",
                    "src_data_year": yyyy,
                    "t_chunk_size": 1000000,
                    "t_schema_name": "green_taxi_schema",
                },           # Optional: runtime variables for the pipeline
                check_status=True,     # Optional: poll and check the status of the triggered pipeline
                error_on_failure=True, # Optional: if triggered pipeline fails, raise an exception
                poll_interval=15,       # Optional: check the status of triggered pipeline every N seconds
                poll_timeout=None,      # Optional: raise an exception after N seconds
                verbose=True,           # Optional: print status of triggered pipeline run
            )
