{{ config(
    materialized='incremental',
    unique_key=['player_id', 'season_year'],
    incremental_strategy='merge'
) }}

with batting_stats as (
    select
        player_id,
        year(game_date) as season_year,
        count(game_id) as games_played,
        sum(at_bats) as total_at_bats,
        sum(hits) as total_hits,
        sum(home_runs) as total_home_runs,
        sum(rbi) as total_rbi,
        sum(stolen_bases) as total_stolen_bases,
        sum(walks) as total_walks,
        sum(strikeouts) as total_strikeouts
    from {{ ref('stg_game_stats') }}
    {% if is_incremental() %}
    where year(game_date) = {{ var('season_year') }}
    {% endif %}
    group by 1, 2
),

player_salaries as (
    select
        player_id,
        year as season_year,
        team_id,
        salary
    from {{ ref('stg_salaries') }}
    {% if is_incremental() %}
    where year = {{ var('season_year') }}
    {% endif %}
),

player_info as (
    select
        player_id,
        first_name,
        last_name,
        position
    from {{ ref('stg_players') }}
),

team_info as (
    select
        team_id,
        team_name,
        league,
        division
    from {{ ref('stg_teams') }}
),

final as (
    select
        b.player_id,
        p.first_name,
        p.last_name,
        p.position,
        s.team_id,
        t.team_name,
        t.league,
        t.division,
        b.season_year,
        b.games_played,
        b.total_at_bats,
        b.total_hits,
        b.total_home_runs,
        b.total_rbi,
        b.total_stolen_bases,
        b.total_walks,
        b.total_strikeouts,
        -- Calculated Metrics
        case 
            when b.total_at_bats > 0 then (b.total_hits * 1.0 / b.total_at_bats)
            else 0 
        end as batting_avg,
        case 
            when (b.total_at_bats + b.total_walks) > 0 then ((b.total_hits + b.total_walks) * 1.0 / (b.total_at_bats + b.total_walks))
            else 0 
        end as on_base_pct,
        coalesce(s.salary, 0) as salary,
        case 
            when b.total_hits > 0 then (coalesce(s.salary, 0) * 1.0 / b.total_hits)
            else null 
        end as cost_per_hit
    from batting_stats b
    inner join player_info p on b.player_id = p.player_id
    left join player_salaries s on b.player_id = s.player_id and b.season_year = s.season_year
    left join team_info t on s.team_id = t.team_id
)

select * from final
