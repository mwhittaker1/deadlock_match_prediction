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
    result2 = con.execute("select count(*) from player_matches").fetchall()
    result3 = con.execute("select count(DISTINCT match_id) from matches").fetchall()
    #result4 = con.execute("SELECT COUNT(*) FROM matches WHERE account_id IS NULL").fetchone()
    x = con.execute("""
    SELECT COUNT(*) FROM (
        SELECT match_id
        FROM player_matches
        GROUP BY match_id
    )""").fetchone()[0]
    print(f"count distinct matchid from matches= {x}")
    #print(result)
    #df = pd.read_csv("player_history_dev.csv")
    #print(df['match_id'].value_counts())
    
    #count of, count of match_player.account_id to matches.match_id. i.e. (1,23),(2,51230)...
    check_dups = con.execute("""
        SELECT
        match_id,
        COUNT(*) AS player_count
        FROM matches
        GROUP BY match_id
        HAVING COUNT(*) <> 12;
                             """).fetchall()
    v2 = con.execute("""
        SELECT
        player_count,
        COUNT(*) AS match_count
        FROM (
        -- Step 1: for each unique match in `matches`, count how many players it has in `player_matches`
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
    check = con.execute("SELECT * from player_matches WHERE account_id = 75030733").fetchone()
    print(result)
    print(f"\n\n matches distinct account ids: {result} should match player_matches total rows: {result2}")
    print(f"\n count of match ids in matches: {result3}\n\n")   
    print(f"\ncheck dups: {check_dups}")
    print(f"v2 = {v2}")
    print(f"\n\nchecking account id:75030733 in matches. {check} ")