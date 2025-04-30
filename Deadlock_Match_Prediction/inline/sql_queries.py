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
    get_tables()
    x = con.execute("select count(DISTINCT match_id) from matches").fetchone()[0]
    result = con.execute("""
    SELECT COUNT(*) FROM (
        SELECT match_id
        FROM player_matches
        GROUP BY match_id
    )""").fetchone()[0]
    print(f"count distinct matchid from matches= {x}")
    #print(result)
    #df = pd.read_csv("player_history_dev.csv")
    #print(df['match_id'].value_counts())