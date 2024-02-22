{{ config(materializaed="table") }}

with
    green_data as (
        select {{ dbt_utils.star(from=ref("stg_green_tripdata"), except=['trip_type']) }},
        'green' as service_type 
        from {{ ref("stg_green_tripdata") }}
    ),
    yellow_data as (
        select {{ dbt_utils.star(from=ref("stg_yellow_tripdata")) }},
        'yellow' as service_type 
        from {{ ref("stg_yellow_tripdata") }}
    ),
    fhv_data as (
        select
            cast(null as string) as `trip_id`,
            null as `vendor_id`,
            null as `ratecode_id`,
            `pickup_location_id`,
            `dropoff_location_id`,
            `pickup_datetime`,
            `dropoff_datetime`,
            cast(null as string) as `store_and_fwd_flag`,
            1 as `passenger_count`,
            null as `trip_distance`,
            null as `fare_amount`,
            null as `extra`,
            null as `mta_tax`,
            null as `tip_amount`,
            null as `tolls_amount`,
            null as `improvement_surcharge`,
            null as `total_amount`,
            null as `payment_type`,
            null as `congestion_surcharge`,
            cast(null as string) as `payment_type_description`,
            'fhv' as service_type 
        from {{ ref("stg_fhv_tripdata") }}
    ),
    trips_unioned as (
        select *
        from green_data
        union all
        select *
        from yellow_data
        union all
        select *
        from fhv_data
    ),
    dim_zones as (
        select {{ dbt_utils.star(from=ref("dim_zones")) }} 
        from {{ ref("dim_zones") }}
        where borough != 'Unknown'
    ),
    final as (
        select
            trips_unioned.`vendor_id`,
            trips_unioned.`ratecode_id`,
            trips_unioned.`pickup_location_id`,
            pickup_zone.borough as pickup_borough,
            pickup_zone.zone as pickup_zone,
            trips_unioned.`dropoff_location_id`,
            dropoff_zone.borough as dropoff_borough,
            dropoff_zone.zone as dropoff_zone,
            trips_unioned.`pickup_datetime`,
            trips_unioned.`dropoff_datetime`,
            trips_unioned.`store_and_fwd_flag`,
            trips_unioned.`passenger_count`,
            trips_unioned.`trip_distance`,
            trips_unioned.`fare_amount`,
            trips_unioned.`extra`,
            trips_unioned.`mta_tax`,
            trips_unioned.`tip_amount`,
            trips_unioned.`tolls_amount`,
            trips_unioned.`improvement_surcharge`,
            trips_unioned.`total_amount`,
            trips_unioned.`payment_type`,
            trips_unioned.`congestion_surcharge`,
            trips_unioned.`payment_type_description`,
            trips_unioned.`service_type`

        from trips_unioned
        inner join
            dim_zones as pickup_zone
            on trips_unioned.pickup_location_id = pickup_zone.locationid
        inner join
            dim_zones as dropoff_zone
            on trips_unioned.dropoff_location_id = dropoff_zone.locationid
    )
select *
from final
