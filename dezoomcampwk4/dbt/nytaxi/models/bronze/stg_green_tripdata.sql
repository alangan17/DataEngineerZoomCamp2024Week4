{{ config(materializaed="view") }}

-- How to set `model_schema`
-- 1. Copy the schema from BigQuery (copy as JSON)
-- 2. Change the column data `type`
-- 3. Add `rename` if you need to rename the column name
-- 4. Add the CTE step `change_data_type`
{% if execute %}
    {% set model_schema = [
        {
            "name": "VendorID",
            "rename": "vendor_id",
            "mode": "",
            "type": "INTEGER",
            "description": null,
            "fields": [],
        },
        {
            "name": "RatecodeID",
            "rename": "ratecode_id",
            "mode": "",
            "type": "FLOAT64",
            "description": null,
            "fields": [],
        },
        {
            "name": "PULocationID",
            "rename": "pickup_location_id",
            "mode": "",
            "type": "INTEGER",
            "description": null,
            "fields": [],
        },
        {
            "name": "DOLocationID",
            "rename": "dropoff_location_id",
            "mode": "",
            "type": "INTEGER",
            "description": null,
            "fields": [],
        },
        {
            "name": "lpep_pickup_datetime",
            "rename": "pickup_datetime",
            "mode": "",
            "type": "TIMESTAMP",
            "description": null,
            "fields": [],
        },
        {
            "name": "lpep_dropoff_datetime",
            "rename": "dropoff_datetime",
            "mode": "",
            "type": "TIMESTAMP",
            "description": null,
            "fields": [],
        },
        {
            "name": "store_and_fwd_flag",
            "mode": "",
            "type": "STRING",
            "description": null,
            "fields": [],
        },
        {
            "name": "passenger_count",
            "mode": "",
            "type": "INTEGER",
            "description": null,
            "fields": [],
        },
        {
            "name": "trip_distance",
            "mode": "",
            "type": "NUMERIC",
            "description": null,
            "fields": [],
        },
        {
            "name": "trip_type",
            "mode": "",
            "type": "FLOAT64",
            "description": null,
            "fields": [],
        },
        {
            "name": "fare_amount",
            "mode": "",
            "type": "FLOAT64",
            "description": null,
            "fields": [],
        },
        {
            "name": "extra",
            "mode": "",
            "type": "FLOAT64",
            "description": null,
            "fields": [],
        },
        {
            "name": "mta_tax",
            "mode": "",
            "type": "FLOAT64",
            "description": null,
            "fields": [],
        },
        {
            "name": "tip_amount",
            "mode": "",
            "type": "FLOAT64",
            "description": null,
            "fields": [],
        },
        {
            "name": "tolls_amount",
            "mode": "",
            "type": "FLOAT64",
            "description": null,
            "fields": [],
        },
        {
            "name": "improvement_surcharge",
            "mode": "",
            "type": "FLOAT64",
            "description": null,
            "fields": [],
        },
        {
            "name": "total_amount",
            "mode": "",
            "type": "FLOAT64",
            "description": null,
            "fields": [],
        },
        {
            "name": "payment_type",
            "mode": "",
            "type": "FLOAT64",
            "description": null,
            "fields": [],
        },
        {
            "name": "congestion_surcharge",
            "mode": "",
            "type": "FLOAT64",
            "description": null,
            "fields": [],
        },
    ] %}
{% endif %}

with
    base as (
        select {{ dbt_utils.star(from=source("bronze", "green_tripdata")) }}
        from {{ source("bronze", "green_tripdata") }}
        where VendorID is not null
        {% if var('is_test_run', default=true)%}
        -- To run in PRD `dbt build --m stg_green_tripdata --vars '{'is_test_run':'false'}'`
        --limit 100
        {% endif %}
    ),

    change_data_type as (
        select
            {% for val in model_schema -%}
                cast(
                    `{{ val["name"] }}` as {{ val["type"] }}
                ) as `{{ val.get('rename', val['name']) }}`
                {%- if not loop.last %},{% endif %}
            {% endfor %}
        from base
    ),

    add_trip_id as (
        select
            {{ dbt_utils.generate_surrogate_key(["vendor_id", "pickup_datetime"]) }}
            as trip_id,
            *
        from change_data_type
    ),

    add_payment_type_desc as (
        select
            *,
            {{ get_payment_type_description("payment_type") }}
            as payment_type_description
        from add_trip_id
    ),

    final as (select * from add_payment_type_desc)

select *
from final
