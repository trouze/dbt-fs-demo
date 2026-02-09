{{ config(materialized='view') }}

with game_stats as (
    select * from {{ ref('stg_trouze__game_stats') }}
),

aggregated as (
    select
        player_id,
        season_year,
        count(game_id) as games_played,
        sum(at_bats) as total_at_bats,
        sum(hits) as total_hits,
        sum(home_runs) as total_home_runs,
        sum(rbi) as total_rbi,
        sum(stolen_bases) as total_stolen_bases,
        sum(walks) as total_walks,
        sum(strikeouts) as total_strikeouts
    from game_stats
    group by 1, 2
)

select * from aggregated
