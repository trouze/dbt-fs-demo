-- =============================================================================
-- Stored Procedure: SP_PITCHER_SEASON_SUMMARY
-- Database: ANALYTICS
-- Schema: GOLD
-- Description:
--   Builds a season-level pitcher performance summary with salary efficiency
--   metrics. Designed to run once per season (or be backfilled) via an
--   orchestrator that passes the season_year parameter.
--
-- Parameters:
--   SEASON_YEAR (INT) — The season to process (e.g. 2023)
--
-- Target Table: ANALYTICS.GOLD.PITCHER_SEASON_SUMMARY
--
-- Schedule: Runs nightly during the season; once at season close for final.
-- Owner: Data Engineering
-- Last Modified: 2024-11-15
-- =============================================================================

CREATE OR REPLACE PROCEDURE analytics.gold.sp_pitcher_season_summary(SEASON_YEAR INT)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN

    -- =========================================================================
    -- Step 0: Validate input parameter
    -- =========================================================================
    IF (:SEASON_YEAR < 2000 OR :SEASON_YEAR > 2030) THEN
        RETURN 'ERROR: season_year must be between 2000 and 2030';
    END IF;

    -- =========================================================================
    -- Step 1: Build temp table of pitching game logs for the season
    --         Filter to pitchers only (players who recorded innings_pitched > 0)
    -- =========================================================================
    CREATE OR REPLACE TEMPORARY TABLE tmp_pitching_game_logs AS
    SELECT
        gs.player_id,
        gs.game_id,
        gs.game_date,
        gs.innings_pitched,
        gs.hits_allowed,
        gs.earned_runs,
        gs.walks_allowed,
        gs.strikeouts_pitched,
        gs.win,
        gs.loss,
        gs.save,
        CASE
            WHEN gs.innings_pitched >= 6 AND gs.earned_runs <= 3 THEN 1
            ELSE 0
        END AS is_quality_start
    FROM raw.trouze.raw_game_stats gs
    WHERE YEAR(gs.game_date) = :SEASON_YEAR
      AND gs.innings_pitched > 0;

    -- =========================================================================
    -- Step 2: Aggregate to season level per pitcher
    -- =========================================================================
    CREATE OR REPLACE TEMPORARY TABLE tmp_pitcher_season_stats AS
    SELECT
        player_id,
        COUNT(DISTINCT game_id)                          AS games_pitched,
        SUM(innings_pitched)                             AS total_innings_pitched,
        SUM(hits_allowed)                                AS total_hits_allowed,
        SUM(earned_runs)                                 AS total_earned_runs,
        SUM(walks_allowed)                               AS total_walks,
        SUM(strikeouts_pitched)                          AS total_strikeouts,
        SUM(win)                                         AS total_wins,
        SUM(loss)                                        AS total_losses,
        SUM(save)                                        AS total_saves,
        SUM(is_quality_start)                            AS quality_starts
    FROM tmp_pitching_game_logs
    GROUP BY player_id;

    -- =========================================================================
    -- Step 3: Calculate derived pitching metrics
    -- =========================================================================
    CREATE OR REPLACE TEMPORARY TABLE tmp_pitcher_metrics AS
    SELECT
        ps.player_id,
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

        -- ERA = (earned runs / innings pitched) * 9
        CASE
            WHEN ps.total_innings_pitched > 0
            THEN ROUND((ps.total_earned_runs / ps.total_innings_pitched) * 9, 3)
            ELSE NULL
        END AS era,

        -- WHIP = (walks + hits) / innings pitched
        CASE
            WHEN ps.total_innings_pitched > 0
            THEN ROUND((ps.total_walks + ps.total_hits_allowed) / ps.total_innings_pitched, 3)
            ELSE NULL
        END AS whip,

        -- K/9 = (strikeouts / innings pitched) * 9
        CASE
            WHEN ps.total_innings_pitched > 0
            THEN ROUND((ps.total_strikeouts / ps.total_innings_pitched) * 9, 2)
            ELSE NULL
        END AS k_per_9,

        -- BB/9 = (walks / innings pitched) * 9
        CASE
            WHEN ps.total_innings_pitched > 0
            THEN ROUND((ps.total_walks / ps.total_innings_pitched) * 9, 2)
            ELSE NULL
        END AS bb_per_9,

        -- K/BB ratio
        CASE
            WHEN ps.total_walks > 0
            THEN ROUND(ps.total_strikeouts::FLOAT / ps.total_walks, 2)
            ELSE NULL
        END AS k_bb_ratio,

        -- H/9 = (hits allowed / innings pitched) * 9
        CASE
            WHEN ps.total_innings_pitched > 0
            THEN ROUND((ps.total_hits_allowed / ps.total_innings_pitched) * 9, 2)
            ELSE NULL
        END AS h_per_9,

        -- Win percentage
        CASE
            WHEN (ps.total_wins + ps.total_losses) > 0
            THEN ROUND(ps.total_wins::FLOAT / (ps.total_wins + ps.total_losses), 3)
            ELSE NULL
        END AS win_pct,

        -- Classify pitcher role based on games and innings
        CASE
            WHEN ps.total_innings_pitched >= 100 THEN 'Starter'
            WHEN ps.total_saves >= 10            THEN 'Closer'
            WHEN ps.games_pitched >= 40          THEN 'Setup'
            ELSE                                      'Relief'
        END AS pitcher_role

    FROM tmp_pitcher_season_stats ps;

    -- =========================================================================
    -- Step 4: Join with player info, salary, and team data
    -- =========================================================================
    CREATE OR REPLACE TEMPORARY TABLE tmp_final_summary AS
    SELECT
        pm.player_id,
        p.first_name,
        p.last_name,
        p.first_name || ' ' || p.last_name      AS full_name,
        p.position,
        pm.pitcher_role,

        t.team_id,
        t.team_name,
        t.league,
        t.division,

        :SEASON_YEAR                             AS season_year,
        pm.games_pitched,
        pm.total_innings_pitched,
        pm.total_hits_allowed,
        pm.total_earned_runs,
        pm.total_walks,
        pm.total_strikeouts,
        pm.total_wins,
        pm.total_losses,
        pm.total_saves,
        pm.quality_starts,
        pm.era,
        pm.whip,
        pm.k_per_9,
        pm.bb_per_9,
        pm.k_bb_ratio,
        pm.h_per_9,
        pm.win_pct,

        COALESCE(s.salary, 0)                    AS salary,

        -- Cost efficiency metrics
        CASE
            WHEN pm.total_strikeouts > 0
            THEN ROUND(COALESCE(s.salary, 0) / pm.total_strikeouts, 2)
            ELSE NULL
        END AS cost_per_strikeout,

        CASE
            WHEN pm.total_wins > 0
            THEN ROUND(COALESCE(s.salary, 0) / pm.total_wins, 2)
            ELSE NULL
        END AS cost_per_win,

        CASE
            WHEN pm.total_innings_pitched > 0
            THEN ROUND(COALESCE(s.salary, 0) / pm.total_innings_pitched, 2)
            ELSE NULL
        END AS cost_per_inning,

        CURRENT_TIMESTAMP()                      AS updated_at

    FROM tmp_pitcher_metrics pm
    INNER JOIN raw.trouze.raw_players p
        ON pm.player_id = p.player_id
    LEFT JOIN raw.trouze.raw_salaries s
        ON pm.player_id = s.player_id
       AND s.year = :SEASON_YEAR
    LEFT JOIN raw.trouze.raw_teams t
        ON s.team_id = t.team_id;

    -- =========================================================================
    -- Step 5: Create target table if it doesn't exist
    -- =========================================================================
    CREATE TABLE IF NOT EXISTS analytics.gold.pitcher_season_summary (
        player_id               VARCHAR,
        first_name              VARCHAR,
        last_name               VARCHAR,
        full_name               VARCHAR,
        position                VARCHAR,
        pitcher_role            VARCHAR,
        team_id                 VARCHAR,
        team_name               VARCHAR,
        league                  VARCHAR,
        division                VARCHAR,
        season_year             INT,
        games_pitched           INT,
        total_innings_pitched   FLOAT,
        total_hits_allowed      INT,
        total_earned_runs       INT,
        total_walks             INT,
        total_strikeouts        INT,
        total_wins              INT,
        total_losses            INT,
        total_saves             INT,
        quality_starts          INT,
        era                     FLOAT,
        whip                    FLOAT,
        k_per_9                 FLOAT,
        bb_per_9                FLOAT,
        k_bb_ratio              FLOAT,
        h_per_9                 FLOAT,
        win_pct                 FLOAT,
        salary                  FLOAT,
        cost_per_strikeout      FLOAT,
        cost_per_win            FLOAT,
        cost_per_inning         FLOAT,
        updated_at              TIMESTAMP_NTZ,

        CONSTRAINT pk_pitcher_season PRIMARY KEY (player_id, season_year)
    );

    -- =========================================================================
    -- Step 6: MERGE into target — upsert by player + season
    -- =========================================================================
    MERGE INTO analytics.gold.pitcher_season_summary AS target
    USING tmp_final_summary AS source
        ON  target.player_id   = source.player_id
        AND target.season_year = source.season_year
    WHEN MATCHED THEN UPDATE SET
        first_name              = source.first_name,
        last_name               = source.last_name,
        full_name               = source.full_name,
        position                = source.position,
        pitcher_role            = source.pitcher_role,
        team_id                 = source.team_id,
        team_name               = source.team_name,
        league                  = source.league,
        division                = source.division,
        games_pitched           = source.games_pitched,
        total_innings_pitched   = source.total_innings_pitched,
        total_hits_allowed      = source.total_hits_allowed,
        total_earned_runs       = source.total_earned_runs,
        total_walks             = source.total_walks,
        total_strikeouts        = source.total_strikeouts,
        total_wins              = source.total_wins,
        total_losses            = source.total_losses,
        total_saves             = source.total_saves,
        quality_starts          = source.quality_starts,
        era                     = source.era,
        whip                    = source.whip,
        k_per_9                 = source.k_per_9,
        bb_per_9                = source.bb_per_9,
        k_bb_ratio              = source.k_bb_ratio,
        h_per_9                 = source.h_per_9,
        win_pct                 = source.win_pct,
        salary                  = source.salary,
        cost_per_strikeout      = source.cost_per_strikeout,
        cost_per_win            = source.cost_per_win,
        cost_per_inning         = source.cost_per_inning,
        updated_at              = source.updated_at
    WHEN NOT MATCHED THEN INSERT (
        player_id, first_name, last_name, full_name, position, pitcher_role,
        team_id, team_name, league, division, season_year,
        games_pitched, total_innings_pitched, total_hits_allowed,
        total_earned_runs, total_walks, total_strikeouts,
        total_wins, total_losses, total_saves, quality_starts,
        era, whip, k_per_9, bb_per_9, k_bb_ratio, h_per_9, win_pct,
        salary, cost_per_strikeout, cost_per_win, cost_per_inning, updated_at
    ) VALUES (
        source.player_id, source.first_name, source.last_name, source.full_name,
        source.position, source.pitcher_role,
        source.team_id, source.team_name, source.league, source.division,
        source.season_year,
        source.games_pitched, source.total_innings_pitched, source.total_hits_allowed,
        source.total_earned_runs, source.total_walks, source.total_strikeouts,
        source.total_wins, source.total_losses, source.total_saves, source.quality_starts,
        source.era, source.whip, source.k_per_9, source.bb_per_9,
        source.k_bb_ratio, source.h_per_9, source.win_pct,
        source.salary, source.cost_per_strikeout, source.cost_per_win,
        source.cost_per_inning, source.updated_at
    );

    -- =========================================================================
    -- Step 7: Log completion and row count
    -- =========================================================================
    LET row_count INT := (SELECT COUNT(*) FROM tmp_final_summary);

    -- Clean up temp tables
    DROP TABLE IF EXISTS tmp_pitching_game_logs;
    DROP TABLE IF EXISTS tmp_pitcher_season_stats;
    DROP TABLE IF EXISTS tmp_pitcher_metrics;
    DROP TABLE IF EXISTS tmp_final_summary;

    RETURN 'SUCCESS: Loaded ' || :row_count || ' pitcher records for season ' || :SEASON_YEAR;

END;
$$;

-- =============================================================================
-- Example invocation (called by orchestrator per season):
--   CALL analytics.gold.sp_pitcher_season_summary(2023);
--
-- Backfill example:
--   CALL analytics.gold.sp_pitcher_season_summary(2019);
--   CALL analytics.gold.sp_pitcher_season_summary(2020);
--   CALL analytics.gold.sp_pitcher_season_summary(2021);
--   CALL analytics.gold.sp_pitcher_season_summary(2022);
--   CALL analytics.gold.sp_pitcher_season_summary(2023);
-- =============================================================================
