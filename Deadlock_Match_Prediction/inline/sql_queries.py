import duckdb
import pandas as pd

def get_tables():
    tables = con.execute("show Tables").fetchall()
    print(tables)
    print("\nhero_trends\n")
    print(con.execute("DESCRIBE hero_trends").fetchdf())
    print("\nmatches\n")
    print(con.execute("DESCRIBE matches").fetchdf())
    print("\nplayer_matches\n")
    print(con.execute("DESCRIBE player_matches").fetchdf())
    print("\nplayer_trends\n")
    print(con.execute("DESCRIBE player_trends").fetchdf())

if __name__ == "__main__":
    con = duckdb.connect("c:/Code/Local Code/Deadlock Database/Deadlock_Match_Prediction/deadlock.db")
    #get_tables()
    result = con.execute("select COUNT(DISTINCT account_id) from matches").fetchall()
    result2 = con.execute("select count(DISTINCT account_id) from player_matches").fetchall()
    result3 = con.execute("select count(DISTINCT match_id) from matches").fetchall()
    #result4 = con.execute("SELECT COUNT(*) FROM matches WHERE account_id IS NULL").fetchone()
    #print(result)
    #df = pd.read_csv("player_history_dev.csv")
    #print(df['match_id'].value_counts())
    #count of, count of match_player.account_id to matches.match_id. i.e. (1,23),(2,51230)...
    count_missing_account_ids = con.execute("""
        WITH incomplete_matches AS (
        SELECT
        match_id
        FROM player_matches
        GROUP BY match_id
        HAVING COUNT(account_id) < 12 )

        SELECT 
        COUNT(*) as missing_records_count
        FROM matches m
        JOIN incomplete_matches im ON m.match_id = im.match_id
        WHERE NOT EXISTS (
        SELECT 1 
        FROM player_matches pm 
        WHERE pm.match_id = m.match_id 
        AND pm.account_id = m.account_id);
                """).fetchall()

    v2 = con.execute("""
        SELECT
        player_count,
        COUNT(*) AS match_count
        FROM (
        SELECT
            m.match_id,
            COUNT(pm.account_id) AS player_count
        FROM (
            SELECT DISTINCT match_id
            FROM matches
        ) AS m
        LEFT JOIN player_matches AS pm
            ON pm.match_id = m.match_id
        GROUP BY m.match_id
        ) AS per_match
        -- Step 2: bucket those per-match counts into how often each occurs
        GROUP BY player_count
        ORDER BY player_count;
                     """).fetchall()
    count_of_missing_ids = con.execute("""
        WITH incomplete_matches AS (
        SELECT
            match_id
        FROM player_matches
        GROUP BY match_id
        HAVING COUNT(account_id) < 12
        )

        SELECT 
        COUNT(*) as missing_records_count
        FROM matches m
        JOIN incomplete_matches im ON m.match_id = im.match_id
        WHERE NOT EXISTS (
        SELECT 1 
        FROM player_matches pm 
        WHERE pm.match_id = m.match_id 
        AND pm.account_id = m.account_id
        )
        """).fetchall()
    
    check = con.execute("SELECT count(DISTINCT match_id) from matches WHERE start_time < NOW() - INTERVAL 10 DAY limit 10").fetchone()
    print(f"count distinct account id in matches: {result}")
    print(f"\n\n count distinct account id in player_matches: {result2}")
    print(f"\n count of match ids in matches: {result3}\n\n")   
    print(f"\count_missing_account_ids: {count_missing_account_ids}")
    print(f"count of missing ids: {count_of_missing_ids}")
    print(f"expected ids: {result3[0][0]*12}- count of missing ids - {result3[0][0]*12-count_missing_account_ids[0][0]}\nshould match {result[0][0]}")
    print(f"v2 = {v2}")
    print(f"\n\nmatches greater than 10 days {check} ")

    