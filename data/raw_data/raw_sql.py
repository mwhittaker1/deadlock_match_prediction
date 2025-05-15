import duckdb
import pandas as pd

def insert_match_info():
    con = duckdb.connect(r"C:\Code\Local Code\deadlock_match_prediction\\match_player_raw.duckdb")
    con.execute("DROP TABLE IF EXISTS match_info_history")
    table_name = "match_info_history"
    for x in range(1, 35):
        filename = f"data/raw_data/match_info_{x}.parquet"
        if x == 1:
            con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM '{filename}'")
        else:
            con.execute(f"INSERT INTO {table_name} SELECT * FROM '{filename}'")
        df = con.execute("SELECT * FROM match_info_history LIMIT 5").fetchdf()
        print(df)
    print("done")

def test():
    con = duckdb.connect(r"C:\Code\Local Code\deadlock_match_prediction\\match_player_raw.duckdb")
    result = con.execute("select * from match_info_history LIMIT 1").fetchdf()
    
    print(result.columns)


if __name__ == "__main__":
#    test()
    con = duckdb.connect(r"C:\Code\Local Code\deadlock_match_prediction\\match_player_raw.duckdb")
    result = con.execute("select count(*) from matches_for_training LIMIT 1").fetchone()
    print(f" count matches_for_training  {result}")
    result = con.execute("select * from matches_for_training  LIMIT 1").fetchdf()
    print(f" columns for matches_for_training  {result.columns}")
    #print(db.execute("SELECT COUNT(*)FROM (SELECT match_id, account_id FROM staging_cleaned GROUP BY match_id, account_id HAVING COUNT(*) > 1) AS duplicates;").fetchall())
    #df = con.execute("Select * from 'data/raw_data/match_info_2.parquet' limit 10").fetchdf()