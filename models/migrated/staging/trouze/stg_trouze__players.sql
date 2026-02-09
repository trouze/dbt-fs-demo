{{ config(materialized='view') }}

with source as (
    select * from {{ source('trouze', 'raw_players') }}
),

renamed as (
    select
        player_id,
        first_name,
        last_name,
        position
    from source
)

select * from renamed
