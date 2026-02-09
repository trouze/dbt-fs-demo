{{
    config(
        materialized='incremental',
        unique_key=['player_id', 'season_year'],
        incremental_strategy='merge'
    )
}}

with stats as (
    select * from {{ ref('int_player_season_stats_aggregated') }}
    {% if var('season_year', none) %}
    where season_year = {{ var('season_year') }}
    {% endif %}
),

players as (
    select * from {{ ref('stg_trouze__players') }}
),

salaries as (
    select * from {{ ref('stg_trouze__salaries') }}
    {% if var('season_year', none) %}
    where season_year = {{ var('season_year') }}
    {% endif %}
),

teams as (
    select * from {{ ref('stg_trouze__teams') }}
),

final as (
    select
        stats.player_id,
        players.first_name,
        players.last_name,
        players.position,
        salaries.team_id,
        teams.team_name,
        teams.league,
        teams.division,
        stats.season_year,
        stats.games_played,
        stats.total_at_bats,
        stats.total_hits,
        stats.total_home_runs,
        stats.total_rbi,
        stats.total_stolen_bases,
        stats.total_walks,
        stats.total_strikeouts,
        
        -- Calculations
        case 
            when stats.total_at_bats > 0 then stats.total_hits / stats.total_at_bats 
            else 0 
        end as batting_avg,
        
        case 
            when (stats.total_at_bats + stats.total_walks) > 0 
            then (stats.total_hits + stats.total_walks) / (stats.total_at_bats + stats.total_walks) 
            else 0 
        end as on_base_pct,
        
        coalesce(salaries.salary, 0) as salary,
        
        case 
            when stats.total_hits > 0 then coalesce(salaries.salary, 0) / stats.total_hits 
            else null 
        end as cost_per_hit

    from stats
    inner join players on stats.player_id = players.player_id
    left join salaries on stats.player_id = salaries.player_id and stats.season_year = salaries.season_year
    left join teams on salaries.team_id = teams.team_id
)

select * from final
