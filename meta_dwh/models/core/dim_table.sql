{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'table_key',
    properties = {
        'format': "'ICEBERG'"
    }
) }}

with incoming_events as (
    select
        table_key,
        catalog_name,
        schema_name,
        table_name,
        full_table_name,
        start_time,
        query_id
    from {{ ref('stg_raw_trino_event_tables') }}
    {% if is_incremental() %}
        where start_time > (select coalesce(max(last_queried_at), timestamp '1970-01-01 00:00:00 UTC') from {{ this }})
    {% endif %}
),

incoming_aggregated as (
    select
        table_key,
        catalog_name,
        schema_name,
        table_name,
        full_table_name,
        min(start_time) as first_queried_at,
        max(start_time) as last_queried_at,
        count(distinct query_id) as query_count_batch
    from incoming_events
    group by
        table_key,
        catalog_name,
        schema_name,
        table_name,
        full_table_name
)

{% if is_incremental() %}
, existing_dim as (
    select * from {{ this }}
),

merged as (
    select
        inc.table_key,
        inc.catalog_name,
        inc.schema_name,
        inc.table_name,
        inc.full_table_name,
        coalesce(least(ext.first_queried_at, inc.first_queried_at), inc.first_queried_at) as first_queried_at,
        coalesce(greatest(ext.last_queried_at, inc.last_queried_at), inc.last_queried_at) as last_queried_at,
        coalesce(ext.total_queries_lifetime, 0) + inc.query_count_batch as total_queries_lifetime,
        current_timestamp as dwh_updated_at
    from incoming_aggregated as inc
    left join existing_dim as ext
        on inc.table_key = ext.table_key
)

select * from merged

{% else %}

select
    table_key,
    catalog_name,
    schema_name,
    table_name,
    full_table_name,
    first_queried_at,
    last_queried_at,
    query_count_batch as total_queries_lifetime,
    current_timestamp as dwh_updated_at
from incoming_aggregated

{% endif %}
