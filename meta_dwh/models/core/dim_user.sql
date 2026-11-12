{{ config(
    materialized = 'table',
    properties = {
        'format': "'ICEBERG'"
    }
) }}

with hr_users as (
    select
        lower(trim(user_name)) as user_name,
        trim(team_name) as team_name,
        trim(department) as department,
        trim(cost_center) as cost_center,
        cast(valid_from as timestamp with time zone) as valid_from,
        cast(valid_to as timestamp with time zone) as valid_to
    from {{ ref('seed_user_teams') }}
),

query_users as (
    select distinct user_name
    from {{ ref('stg_raw_trino_events') }}
),

unregistered_users as (
    select
        qu.user_name,
        'Unassigned / Service Account' as team_name,
        'Unassigned Department' as department,
        'CC-UNASSIGNED' as cost_center,
        timestamp '1970-01-01 00:00:00 UTC' as valid_from,
        timestamp '2099-12-31 23:59:59 UTC' as valid_to,
        true as is_service_account
    from query_users as qu
    left join hr_users as hr
        on qu.user_name = hr.user_name
    where hr.user_name is null
),

hr_active_and_historic as (
    select
        user_name,
        team_name,
        department,
        cost_center,
        valid_from,
        valid_to,
        case
            when user_name in ('airflow', 'trino', 'superset', 'system') then true
            else false
        end as is_service_account
    from hr_users
),

all_users as (
    select * from hr_active_and_historic
    union all
    select * from unregistered_users
),

final as (
    select
        {{ surrogate_key(['user_name', 'cast(valid_from as varchar)']) }} as user_key,
        user_name,
        team_name,
        department,
        cost_center,
        valid_from,
        valid_to,
        is_service_account,
        current_timestamp as dwh_updated_at
    from all_users

    union all

    -- Fallback synthetic record for unknown/missing user keys
    select
        '__UNKNOWN_USER__' as user_key,
        'unknown' as user_name,
        'Unassigned / Service Account' as team_name,
        'Unassigned Department' as department,
        'CC-UNASSIGNED' as cost_center,
        timestamp '1970-01-01 00:00:00 UTC' as valid_from,
        timestamp '2099-12-31 23:59:59 UTC' as valid_to,
        true as is_service_account,
        current_timestamp as dwh_updated_at
)

select * from final
