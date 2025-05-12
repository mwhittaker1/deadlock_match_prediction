import duckdb


def main(db, table_name):
    i=1
    for x in range(1, 25):
        
        filename = f"data/raw_data/match_player_{x}.parquet"
        print(f"Loading {filename} into DuckDB")

        try:
            exclude_prefixes = ("book_", "stats.", "death_", "items.")
            # Get column names directly from file
            all_cols = db.execute(f"SELECT * FROM '{filename}' LIMIT 1").fetchdf().columns
            filtered_cols = [col for col in all_cols if not col.startswith(exclude_prefixes)]
            col_list = ", ".join([f'"{col}"' for col in filtered_cols])

            if i == 1:
                db.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT {col_list} FROM '{filename}'")
                i+=1
            else:
                db.execute(f"INSERT INTO {table_name} SELECT {col_list} FROM '{filename}'")
        except Exception as e:
            print(f"❌ Failed loading {filename}: {e}")

def test(con,tbl_name):
    # Test the connection
    row_c = con.execute("""SELECT count(*) FROM 'data/raw_data/match_player_25.parquet'""").fetchone()[0]
    print(row_c)



def test1():
    df = con.execute("""SELECT * FROM 'data/raw_data/match_player_25.parquet' LIMIT 5""").fetchdf()
    cols_to_exclude = [col for col in df.columns if col.startswith("book_") or col.startswith("stats.") or col.startswith("death_") or col.startswith("items.")]
    df = df.drop(columns=cols_to_exclude)
    print(df.columns)

def test_insert_raw_data():
    table_name = "staging_cleaned"
    db = duckdb.connect("match_player_raw.duckdb")
    db.execute(f"DROP TABLE IF EXISTS {table_name}")

    df = db.execute("""SELECT * FROM 'data/raw_data/match_player_25.parquet' LIMIT 5""").fetchdf()
    cols_to_exclude = [col for col in df.columns if col.startswith("book_") or col.startswith("stats.") or col.startswith("death_") or col.startswith("items.")]
    df = df.drop(columns=cols_to_exclude)

    db.register("filtered_df", df)
    db.execute(f"""
        CREATE TABLE {table_name} AS SELECT * FROM filtered_df
    """)

if __name__ == "__main__":
    table_name = "staging_cleaned"
    db = duckdb.connect("match_player_raw.duckdb")
    #db.execute(f"DROP TABLE IF EXISTS {table_name}")
    main(db, table_name)
