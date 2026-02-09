{{ config(materialized='view') }}

with source as (
    select * from {{ source('trouze', 'raw_game_stats') }}
),

renamed as (
    select
        game_id,
        player_id,
        game_date,
        at_bats,
        hits,
        home_runs,
        rbi,
        stolen_bases,
        walks,
        strikeouts,
        year(game_date) as season_year
    from source
)

select * from renamed
