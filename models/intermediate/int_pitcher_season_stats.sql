with pitching_game_logs as (
    select
        player_id,
        game_id,
        game_date,
        year(game_date) as season_year,
        innings_pitched,
        hits_allowed,
        earned_runs,
        walks_allowed,
        strikeouts_pitched,
        win,
        loss,
        save,
        case
            when innings_pitched >= 6 and earned_runs <= 3 then 1
            else 0
        end as is_quality_start
    from {{ ref('stg_game_stats') }}
    where innings_pitched > 0
),

season_aggregates as (
    select
        player_id,
        season_year,
        count(distinct game_id) as games_pitched,
        sum(innings_pitched) as total_innings_pitched,
        sum(hits_allowed) as total_hits_allowed,
        sum(earned_runs) as total_earned_runs,
        sum(walks_allowed) as total_walks,
        sum(strikeouts_pitched) as total_strikeouts,
        sum(win) as total_wins,
        sum(loss) as total_losses,
        sum(save) as total_saves,
        sum(is_quality_start) as quality_starts
    from pitching_game_logs
    group by player_id, season_year
)

select
    player_id,
    season_year,
    games_pitched,
    total_innings_pitched,
    total_hits_allowed,
    total_earned_runs,
    total_walks,
    total_strikeouts,
    total_wins,
    total_losses,
    total_saves,
    quality_starts,

    -- ERA = (earned runs / innings pitched) * 9
    case
        when total_innings_pitched > 0
        then round((total_earned_runs / total_innings_pitched) * 9, 3)
        else null
    end as era,

    -- WHIP = (walks + hits) / innings pitched
    case
        when total_innings_pitched > 0
        then round((total_walks + total_hits_allowed) / total_innings_pitched, 3)
        else null
    end as whip,

    -- K/9 = (strikeouts / innings pitched) * 9
    case
        when total_innings_pitched > 0
        then round((total_strikeouts / total_innings_pitched) * 9, 2)
        else null
    end as k_per_9,

    -- BB/9 = (walks / innings pitched) * 9
    case
        when total_innings_pitched > 0
        then round((total_walks / total_innings_pitched) * 9, 2)
        else null
    end as bb_per_9,

    -- K/BB ratio
    case
        when total_walks > 0
        then round(total_strikeouts::float / total_walks, 2)
        else null
    end as k_bb_ratio,

    -- H/9 = (hits allowed / innings pitched) * 9
    case
        when total_innings_pitched > 0
        then round((total_hits_allowed / total_innings_pitched) * 9, 2)
        else null
    end as h_per_9,

    -- Win percentage
    case
        when (total_wins + total_losses) > 0
        then round(total_wins::float / (total_wins + total_losses), 3)
        else null
    end as win_pct,

    -- Pitcher role classification
    case
        when total_innings_pitched >= 100 then 'Starter'
        when total_saves >= 10 then 'Closer'
        when games_pitched >= 40 then 'Setup'
        else 'Relief'
    end as pitcher_role

from season_aggregates
