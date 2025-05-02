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
    #result = con.execute("select (DISTINCT account_id) from matches").fetchall()
    #result2 = con.execute("select count(*) from player_matches").fetchall()
    result = con.execute("select DISTINCT match_id from matches LIMIT 40").fetchall()
    #result = con.execute("""
    #SELECT COUNT(*) FROM (
    #    SELECT match_id
    #    FROM player_matches
    #    GROUP BY match_id
    #)""").fetchone()[0]
    #print(f"count distinct matchid from matches= {x}")
    #print(result)
    #df = pd.read_csv("player_history_dev.csv")
    #print(df['match_id'].value_counts())
    
    #count of, count of match_player.account_id to matches.match_id. i.e. (1,23),(2,51230)...
    #result = con.execute("""
    #    SELECT 
    #        player_count,
    #        COUNT(*) AS match_count
    #    FROM (
    #        SELECT 
    #            pm.match_id,
    #            COUNT(*) AS player_count
    #        FROM player_matches pm
    #        INNER JOIN matches m ON pm.match_id = m.match_id
    #        GROUP BY pm.match_id
    #    )
    #    GROUP BY player_count
    #    ORDER BY player_count;
    #    """).fetchall()
    print(result)
    #print(f"\n\n vs all = {result2}")