{{ config(materialized='view') }}

with source as (
    select * from {{ source('trouze', 'raw_salaries') }}
),

renamed as (
    select
        player_id,
        team_id,
        year as season_year,
        salary
    from source
)

select * from renamed
