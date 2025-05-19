import duckdb
import pandas as pd
import logging
import function_tools as u

def sql_to_csv(con, sql:str, out_path:str):
    """
    Executes a SQL query and saves the result to a CSV file.
    
    Parameters:
    con : The connection object to the DuckDB database.
    sql : str, The SQL query to execute.
    out_path : str, The path where the CSV file will be saved.
    """
    con.execute(f"""COPY (
                {sql}
                ) TO '{out_path}'
                (HEADER, DELIMITER ',');
                """)
    print("Wrote CSV to", out_path)

def fetch_training_data(con, path, n=1000):
    """Fetches and structures data for training a model."""

    #-- Select a sample of matches for training
    match_data_query ="""
    WITH match_data AS (
        SELECT 
            m.match_id,
            m.start_time,
            m.winning_team
        FROM 
            matches m
        ORDER BY 
            random()  -- Randomly select matches
        LIMIT 1000
    )
    SELECT *
    FROM match_data"""
    sql_to_csv(con, match_data_query, path + "match_data.csv")
    
    #- Separate players by team for each match
    teams_of_players = """
    WITH teams_of_players AS (
        SELECT
            pm.match_id,
            pm.account_id,
            pm.hero_id,
            pm.team,
            pm.kills,
            pm.deaths,
            pm.assists,
            CASE WHEN pm.deaths = 0 THEN pm.kills ELSE CAST(pm.kills AS FLOAT) / pm.deaths END AS kd_ratio
        FROM 
            player_matches pm
        JOIN 
            match_data md ON pm.match_id = md.match_id
    )
    SELECT * from teams_of_players"""
    sql_to_csv(con, teams_of_players, path + "teams_of_players.csv")

    #-- Calculate team-level statistics
    team_stats = """
    WITH team_stats AS (
        SELECT
            match_id,
            team,
            AVG(kd_ratio) AS team_avg_kd,
            MAX(kd_ratio) AS team_max_kd,
            MIN(kd_ratio) AS team_min_kd,
            COUNT(*) AS team_size
        FROM 
            teams_of_players
        GROUP BY 
            match_id, team
    )
    SELECT * FROM team_stats
    """
    sql_to_csv(con, team_stats, path + "team_stats.csv")

    #-- Get player trends and statistics
    player_performance = """
    WITH player_performance AS (
        SELECT
            ta.match_id,
            ta.team,
            -- Basic performance stats
            AVG(pt.p_average_kills) AS avg_team_kills,
            AVG(pt.p_average_deaths) AS avg_team_deaths,
            AVG(pt.p_avg_kd) AS avg_team_kd,
            AVG(pt.p_win_rate) AS avg_team_win_rate,
            AVG(pt.p_total_matches) AS avg_player_matches,
            
            -- Streak information
            AVG(pt.p_win_streak_avg) AS avg_win_streak,
            AVG(pt.p_loss_streak_avg) AS avg_loss_streak,
            SUM(pt.win_streaks_2plus) AS team_win_streaks_2plus,
            SUM(pt.win_streaks_3plus) AS team_win_streaks_3plus,
            SUM(pt.win_streaks_4plus) AS team_win_streaks_4plus,
            SUM(pt.win_streaks_5plus) AS team_win_streaks_5plus,
            SUM(pt.loss_streaks_2plus) AS team_loss_streaks_2plus,
            SUM(pt.loss_streaks_3plus) AS team_loss_streaks_3plus,
            SUM(pt.loss_streaks_4plus) AS team_loss_streaks_4plus,
            SUM(pt.loss_streaks_5plus) AS team_loss_streaks_5plus,
            
            -- Hero comparison metrics
            AVG(pt.p_v_h_kd_pct) AS avg_hero_kd_percentage,
            
            -- Team strength indicators
            MAX(pt.p_win_rate) AS max_player_win_rate,
            MIN(pt.p_win_rate) AS min_player_win_rate,
            MAX(pt.p_avg_kd) AS max_player_kd,
            MIN(pt.p_avg_kd) AS min_player_kd,
            
            -- Experience metrics
            SUM(pt.p_total_matches) AS total_team_experience,
            MAX(pt.p_total_matches) AS most_experienced_player,
            MIN(pt.p_total_matches) AS least_experienced_player,
            
            -- Consistency metrics
            STDDEV(pt.p_win_rate) AS win_rate_consistency,
            STDDEV(pt.p_avg_kd) AS kd_consistency
        FROM 
            teams_of_players ta
        JOIN 
            player_trends pt ON ta.account_id = pt.account_id
        GROUP BY 
            ta.match_id, ta.team
    )
    SELECT * FROM player_performance
    """
    sql_to_csv(con, player_performance, path + "player_performance.csv")

    #-- Calculate hero trend statistics by team
    hero_trend_by_team = """
    WITH hero_trend_by_team AS (
        SELECT
            ta.match_id,
            ta.team,
            AVG(ht.win_rate) AS avg_hero_win_rate,
            AVG(ht.average_kd) AS avg_hero_kd,
            AVG(ht.pick_rate) AS avg_hero_pick_rate,
            SUM(ht.pick_rate) AS total_hero_popularity,
            MAX(ht.win_rate) AS max_hero_win_rate,
            MIN(ht.win_rate) AS min_hero_win_rate,
            MAX(ht.average_kd) AS max_hero_kd,
            MIN(ht.average_kd) AS min_hero_kd,
            STDDEV(ht.win_rate) AS hero_win_rate_variety,
            STDDEV(ht.average_kd) AS hero_kd_variety
        FROM 
            teams_of_players ta
        JOIN 
            hero_trends ht ON ta.hero_id = ht.hero_id
        WHERE 
            ht.trend_window_days = 30  -- Using 30-day trends
        GROUP BY 
            ta.match_id, ta.team
    )
    SELECT * FROM hero_trend_by_team
    """
    sql_to_csv(con, hero_trend_by_team, path + "hero_trend_by_team.csv")

    #-- Get recent player performance (rolling stats)
    player_rolling_stats = """
    WITH recent_performance AS (
        SELECT
            ta.match_id,
            ta.team,
            AVG(prs.p_win_pct_2) AS avg_recent_win_pct_2,
            AVG(prs.p_win_pct_3) AS avg_recent_win_pct_3,
            AVG(prs.p_win_pct_4) AS avg_recent_win_pct_4,
            AVG(prs.p_win_pct_5) AS avg_recent_win_pct_5,
            AVG(prs.p_loss_pct_2) AS avg_recent_loss_pct_2,
            AVG(prs.p_loss_pct_3) AS avg_recent_loss_pct_3,
            AVG(prs.p_loss_pct_4) AS avg_recent_loss_pct_4,
            AVG(prs.p_loss_pct_5) AS avg_recent_loss_pct_5,
            MAX(prs.p_win_pct_5) AS max_recent_win_pct,
            MIN(prs.p_win_pct_5) AS min_recent_win_pct
        FROM 
            teams_of_players ta
        JOIN 
            player_rolling_stats prs ON ta.account_id = prs.account_id AND ta.match_id = prs.match_id
        GROUP BY 
            ta.match_id, ta.team
    )
    SELECT * FROM recent_performance
    """
    sql_to_csv(con, player_rolling_stats, path + "player_rolling_stats.csv")

    #-- create team features
    team_features = """
    WITH team_features AS (
        SELECT
            ts.match_id,
            ts.team,
            -- Match-specific performance
            ts.team_avg_kd,
            ts.team_max_kd,
            ts.team_min_kd,
            
            -- Historical player performance
            pp.avg_team_kills,
            pp.avg_team_deaths,
            pp.avg_team_kd,
            pp.avg_team_win_rate,
            pp.max_player_win_rate,
            pp.min_player_win_rate,
            pp.max_player_kd,
            pp.min_player_kd,
            pp.avg_player_matches,
            pp.total_team_experience,
            pp.most_experienced_player,
            pp.least_experienced_player,
            pp.win_rate_consistency,
            pp.kd_consistency,
            
            -- Streak information
            pp.avg_win_streak,
            pp.avg_loss_streak,
            pp.team_win_streaks_2plus,
            pp.team_win_streaks_3plus,
            pp.team_win_streaks_4plus,
            pp.team_win_streaks_5plus,
            pp.team_loss_streaks_2plus,
            pp.team_loss_streaks_3plus,
            pp.team_loss_streaks_4plus,
            pp.team_loss_streaks_5plus,
            
            -- Hero comparison
            pp.avg_hero_kd_percentage,
            
            -- Hero performance
            ht.avg_hero_win_rate,
            ht.avg_hero_kd,
            ht.avg_hero_pick_rate,
            ht.total_hero_popularity,
            ht.max_hero_win_rate,
            ht.min_hero_win_rate,
            ht.max_hero_kd,
            ht.min_hero_kd,
            ht.hero_win_rate_variety,
            ht.hero_kd_variety,
            
            -- Recent performance
            COALESCE(rp.avg_recent_win_pct_2, 50) AS avg_recent_win_pct_2,
            COALESCE(rp.avg_recent_win_pct_3, 50) AS avg_recent_win_pct_3,
            COALESCE(rp.avg_recent_win_pct_4, 50) AS avg_recent_win_pct_4,
            COALESCE(rp.avg_recent_win_pct_5, 50) AS avg_recent_win_pct_5,
            COALESCE(rp.avg_recent_loss_pct_2, 50) AS avg_recent_loss_pct_2,
            COALESCE(rp.avg_recent_loss_pct_3, 50) AS avg_recent_loss_pct_3,
            COALESCE(rp.avg_recent_loss_pct_4, 50) AS avg_recent_loss_pct_4,
            COALESCE(rp.avg_recent_loss_pct_5, 50) AS avg_recent_loss_pct_5,
            COALESCE(rp.max_recent_win_pct, 50) AS max_recent_win_pct,
            COALESCE(rp.min_recent_win_pct, 50) AS min_recent_win_pct
        FROM 
            team_stats ts
        LEFT JOIN 
            player_performance pp ON ts.match_id = pp.match_id AND ts.team = pp.team
        LEFT JOIN 
            hero_trend_by_team ht ON ts.match_id = ht.match_id AND ts.team = ht.team
        LEFT JOIN 
            recent_performance rp ON ts.match_id = rp.match_id AND ts.team = rp.team
    )
    SELECT * FROM team_features
    """
    sql_to_csv(con, team_features, path + "team_features.csv")


    training_data = """
     -- Final dataset for model training
    SELECT
        md.match_id,
        md.start_time,
        md.winning_team,
        
        -- Team 0 features
        t0.team_avg_kd AS t0_avg_kd,
        t0.team_max_kd AS t0_max_kd,
        t0.team_min_kd AS t0_min_kd,
        t0.avg_team_kills AS t0_avg_kills,
        t0.avg_team_deaths AS t0_avg_deaths,
        t0.avg_team_kd AS t0_avg_historical_kd,
        t0.avg_team_win_rate AS t0_win_rate,
        t0.max_player_win_rate AS t0_max_win_rate,
        t0.min_player_win_rate AS t0_min_win_rate,
        t0.max_player_kd AS t0_max_historical_kd,
        t0.min_player_kd AS t0_min_historical_kd,
        t0.avg_win_streak AS t0_win_streak,
        t0.avg_loss_streak AS t0_loss_streak,
        t0.team_win_streaks_2plus AS t0_win_streaks_2,
        t0.avg_player_matches AS t0_player_matches,
        t0.total_team_experience AS t0_total_experience,
        t0.avg_hero_win_rate AS t0_hero_win_rate,
        t0.avg_recent_win_pct AS t0_recent_wins,
        
        -- Team 1 features
        t1.team_avg_kd AS t1_avg_kd,
        t1.team_max_kd AS t1_max_kd,
        t1.team_min_kd AS t1_min_kd,
        t1.avg_team_kills AS t1_avg_kills,
        t1.avg_team_deaths AS t1_avg_deaths,
        t1.avg_team_kd AS t1_avg_historical_kd,
        t1.avg_team_win_rate AS t1_win_rate,
        t1.max_player_win_rate AS t1_max_win_rate,
        t1.min_player_win_rate AS t1_min_win_rate,
        t1.max_player_kd AS t1_max_historical_kd,
        t1.min_player_kd AS t1_min_historical_kd,
        t1.avg_win_streak AS t1_win_streak,
        t1.avg_loss_streak AS t1_loss_streak,
        t1.team_win_streaks_2plus AS t1_win_streaks_2,
        t1.avg_player_matches AS t1_player_matches,
        t1.total_team_experience AS t1_total_experience,
        t1.avg_hero_win_rate AS t1_hero_win_rate,
        t1.avg_recent_win_pct AS t1_recent_wins,
        
        -- Differential features (team 0 relative to team 1)
        (t0.team_avg_kd - t1.team_avg_kd) AS kd_diff,
        (t0.avg_team_kd - t1.avg_team_kd) AS historical_kd_diff,
        (t0.avg_team_win_rate - t1.avg_team_win_rate) AS win_rate_diff,
        (t0.avg_win_streak - t1.avg_win_streak) AS win_streak_diff,
        (t0.avg_recent_win_pct - t1.avg_recent_win_pct) AS recent_win_diff,
        (t0.avg_hero_win_rate - t1.avg_hero_win_rate) AS hero_win_rate_diff,
        
        -- Target variable: 1 if team 0 won, 0 if team 1 won
        CASE WHEN md.winning_team = '0' THEN 1 ELSE 0 END AS team0_won
    FROM 
        match_data md
    JOIN 
        team_features t0 ON md.match_id = t0.match_id AND t0.team = '0'
    JOIN 
        team_features t1 ON md.match_id = t1.match_id AND t1.team = '1'
    """
    sql_to_csv(con, training_data, path + "training_data.csv")
    # Combine all query parts
    complete_query = (
        match_data_query +
        teams_of_players +
        team_stats +
        player_performance +
        hero_trend_by_team +
        player_rolling_stats +
        team_features +
        training_data
    )
    sql_to_csv(con, complete_query, path + "complete_training_data.csv")
        # Execute the complete query
    try:
        result_df = pd.read_sql_query(complete_query, con)
        print(f"Successfully fetched {len(result_df)} matches for training")
        return result_df
    except Exception as e:
        print(f"Error executing query: {e}")
        # You might want to print the complete query here for debugging
        print(complete_query)
        return None

if __name__ == "__main__":
    con = duckdb.connect("c:/Code/Local Code/deadlock_match_prediction/data/deadlock.db")
    path = "C:/Code/Local Code/deadlock_match_prediction/data/test_data/"
    df = fetch_training_data(con, path)
    print(df.head())