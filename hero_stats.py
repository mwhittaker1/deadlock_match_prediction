import re
import time
from collections import defaultdict
from typing import Generator, Iterable, Dict, List, Tuple

import boto3
import duckdb
from botocore import UNSIGNED
from botocore.config import Config

S3_URL = "https://s3-cache.deadlock-api.com"
BUCKET = "db-snapshot"
BUCKET_URL = f"{S3_URL}/{BUCKET}"

def list_parquet_files() -> Generator[str, None, None]:
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED), endpoint_url=S3_URL)
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix="public/"):
        for obj in page.get("Contents", []):  # guard
            key = obj["Key"]
            if key.endswith(".parquet"):
                yield f"{BUCKET_URL}/{key}"

def group_parquet_files_by_table(file_urls: Iterable[str]) -> Dict[str, List[str]]:
    table_files: Dict[str, List[str]] = defaultdict(list)
    indexed_file_pattern = re.compile(r"(.+)_(\d+)\.parquet$")
    simple_file_pattern = re.compile(r"(.+)\.parquet$")

    for url in file_urls:
        filename = url.split("/")[-1]
        m = indexed_file_pattern.match(filename) or simple_file_pattern.match(filename)
        table_name = m.group(1) if m else filename
        table_files[table_name].append(url)
    return table_files

def get_tables() -> Dict[str, List[str]]:
    return group_parquet_files_by_table(list_parquet_files())

def setup_views(con: duckdb.DuckDBPyConnection) -> None:
    # Ensure httpfs is available for HTTPS parquet
    con.execute("INSTALL httpfs; LOAD httpfs;")
    tables = get_tables()
    for name, urls in tables.items():
        safe = duckdb.quote_ident(name)
        con.execute(f"DROP VIEW IF EXISTS {safe}")
        # Pass the URL list as a parameter; DuckDB will expand it
        con.execute(f"CREATE VIEW {safe} AS SELECT * FROM read_parquet(?);", [urls])

def get_hero_stats(con: duckdb.DuckDBPyConnection, ids: List[int]) -> List[Tuple]:
    # Better: load IDs into a temp table to avoid massive IN (...)
    con.execute("CREATE TEMP TABLE tmp_ids(account_id BIGINT);")
    con.executemany("INSERT INTO tmp_ids VALUES (?)", [(i,) for i in ids])

    q = """
SELECT
    mp.account_id,
    mp.hero_id,
    COUNT(*) AS matches_played,
    MAX(mi.start_time) AS last_played,
    SUM(mp.duration_s) AS time_played,
    SUM(mp.won) AS wins,
    AVG(mp.max_level) AS ending_level,
    SUM(mp.kills) AS kills,
    SUM(mp.deaths) AS deaths,
    SUM(mp.assists) AS assists,
    AVG(mp.denies) AS denies_per_match,
    AVG(60.0 * mp.kills  / GREATEST(1, mp.duration_s)) AS kills_per_min,
    AVG(60.0 * mp.deaths / GREATEST(1, mp.duration_s)) AS deaths_per_min,
    AVG(60.0 * mp.assists/ GREATEST(1, mp.duration_s)) AS assists_per_min,
    AVG(60.0 * mp.denies / GREATEST(1, mp.duration_s)) AS denies_per_min,
    AVG(60.0 * mp.net_worth / GREATEST(1, mp.duration_s)) AS networth_per_min,
    AVG(60.0 * mp.last_hits / GREATEST(1, mp.duration_s)) AS last_hits_per_min,
    AVG(60.0 * mp.max_player_damage / GREATEST(1, mp.duration_s)) AS damage_per_min,
    AVG(mp.max_player_damage / NULLIF(mp.net_worth,0)) AS damage_per_soul,
    AVG(60.0 * mp.max_damage_mitigated / GREATEST(1, mp.duration_s)) AS damage_mitigated_per_min,
    AVG(60.0 * mp.max_player_damage_taken / GREATEST(1, mp.duration_s)) AS damage_taken_per_min,
    AVG(mp.max_player_damage_taken / NULLIF(mp.net_worth,0)) AS damage_taken_per_soul,
    AVG(60.0 * mp.max_creep_kills / GREATEST(1, mp.duration_s)) AS creeps_per_min,
    AVG(60.0 * mp.max_neutral_damage / GREATEST(1, mp.duration_s)) AS obj_damage_per_min,
    AVG(mp.max_neutral_damage / NULLIF(mp.net_worth,0)) AS obj_damage_per_soul,
    AVG(mp.max_shots_hit / GREATEST(1, mp.max_shots_hit + mp.max_shots_missed)) AS accuracy,
    AVG(mp.max_hero_bullets_hit_crit / GREATEST(1, mp.max_hero_bullets_hit_crit + mp.max_hero_bullets_hit)) AS crit_shot_rate
FROM match_player AS mp
JOIN match_info AS mi USING (match_id)
JOIN tmp_ids t ON t.account_id = mp.account_id
GROUP BY mp.account_id, mp.hero_id
ORDER BY mp.account_id, mp.hero_id;
"""
    return con.execute(q).fetchall()

if __name__ == "__main__":
    with open("account_ids.csv", "r") as f:
        account_ids = [int(line.strip()) for line in f if line.strip()]
    with duckdb.connect() as con:
        setup_views(con)
        print("DuckDB is set up")
        start = time.time()
        result = get_hero_stats(con, account_ids)
        print(f"Time taken: {time.time() - start:.2f}s")
        print(f"Fetched {len(result)} rows")
        if result:
            print(f"First row: {result[0]}")
