{{ config(materialized='view') }}

with source as (
    select * from {{ source('trouze', 'raw_teams') }}
),

renamed as (
    select
        team_id,
        team_name,
        league,
        division
    from source
)

select * from renamed
