{{ config(
    materialized='incremental',
    unique_key=['player_id', 'season_year'],
    incremental_strategy='merge'
) }}

with pitcher_stats as (
    select * from {{ ref('int_pitcher_season_stats') }}
    {% if is_incremental() %}
    where season_year = {{ var('season_year') }}
    {% endif %}
),

player_info as (
    select
        player_id,
        first_name,
        last_name,
        full_name,
        position
    from {{ ref('stg_players') }}
),

salaries as (
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
        ps.player_id,
        p.first_name,
        p.last_name,
        p.full_name,
        p.position,
        ps.pitcher_role,

        t.team_id,
        t.team_name,
        t.league,
        t.division,

        ps.season_year,
        ps.games_pitched,
        ps.total_innings_pitched,
        ps.total_hits_allowed,
        ps.total_earned_runs,
        ps.total_walks,
        ps.total_strikeouts,
        ps.total_wins,
        ps.total_losses,
        ps.total_saves,
        ps.quality_starts,
        ps.era,
        ps.whip,
        ps.k_per_9,
        ps.bb_per_9,
        ps.k_bb_ratio,
        ps.h_per_9,
        ps.win_pct,

        coalesce(s.salary, 0) as salary,

        -- Cost efficiency metrics
        case
            when ps.total_strikeouts > 0
            then round(coalesce(s.salary, 0) / ps.total_strikeouts, 2)
            else null
        end as cost_per_strikeout,

        case
            when ps.total_wins > 0
            then round(coalesce(s.salary, 0) / ps.total_wins, 2)
            else null
        end as cost_per_win,

        case
            when ps.total_innings_pitched > 0
            then round(coalesce(s.salary, 0) / ps.total_innings_pitched, 2)
            else null
        end as cost_per_inning

    from pitcher_stats ps
    inner join player_info p
        on ps.player_id = p.player_id
    left join salaries s
        on ps.player_id = s.player_id
        and ps.season_year = s.season_year
    left join team_info t
        on s.team_id = t.team_id
)

select * from final
