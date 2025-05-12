import duckdb


table_name = "staging_cleaned"
db = duckdb.connect("match_player_raw.duckdb")
print(db.execute("SHOW TABLES").fetchall())
print((db.execute(f"select * from {table_name} limit 10").fetchdf()).columns)
print(db.execute("SELECT COUNT(*)FROM (SELECT match_id, account_id FROM staging_cleaned GROUP BY match_id, account_id HAVING COUNT(*) > 1) AS duplicates;").fetchall())