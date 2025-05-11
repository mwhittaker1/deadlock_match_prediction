import duckdb


table_name = "staging_cleaned"
db = duckdb.connect("match_player_raw.duckdb")
#print(db.execute("SHOW TABLES").fetchall())
#print((db.execute(f"select * from {table_name} limit 10").fetchdf()).columns)
#print(db.execute(f"select count(DISTINCT account_id) from {table_name}").fetchall())
#print(db.execute(f"PRAGMA table_info({table_name});").fetchall())
