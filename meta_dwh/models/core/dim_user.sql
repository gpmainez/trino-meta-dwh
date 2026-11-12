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
        trim(cost_center) as cost_center
    from {{ ref('seed_user_teams') }}
),

query_users as (
    select distinct user_name
    from {{ ref('stg_raw_trino_events') }}
),

combined_users as (
    -- Combine HR users with any active Trino users not yet registered in HR
    select
    --Beacuse full join we have to deal with null values
        coalesce(hr.user_name, qu.user_name) as user_name,
        coalesce(hr.team_name, 'Unassigned / Service Account') as team_name,
        coalesce(hr.department, 'Unassigned Department') as department,
        coalesce(hr.cost_center, 'CC-UNASSIGNED') as cost_center,
        case
            when hr.user_name is null then true
            when hr.user_name in ('airflow', 'trino', 'superset', 'system') then true
            else false
        end as is_service_account
    from hr_users as hr
    full join query_users as qu
        on hr.user_name = qu.user_name
),

final as (
    select
        {{ surrogate_key(['user_name']) }} as user_key,
        user_name,
        team_name,
        department,
        cost_center,
        is_service_account,
        current_timestamp as dwh_updated_at
    from combined_users

    union all

    -- Fallback synthetic record for unknown/missing user keys
    select
        '__UNKNOWN_USER__' as user_key,
        'unknown' as user_name,
        'Unassigned / Service Account' as team_name,
        'Unassigned Department' as department,
        'CC-UNASSIGNED' as cost_center,
        true as is_service_account,
        current_timestamp as dwh_updated_at
)

select * from final
