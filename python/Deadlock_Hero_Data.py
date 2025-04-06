import requests
import pandas as pd
import numpy as np

#Fetches data and returns DF
def fetch_hero_data(url):
    response = requests.get(url)
    
    # Check if the request was successful
    if response.status_code == 200:
        heroes_data = response.json()  # Converts the JSON response to a Python dictionary
    else:
        print(f"Failed to retrieve data: {response.status_code}")
    
    df = pd.DataFrame(heroes_data)
    return df

def filter_hero_data(filters, df):
    filtered_data = []
    for _, item in df.iterrows():
        filtered_item = {key: item[key] for key in filters if key in item}
        filtered_data.append(filtered_item)
    
    return pd.DataFrame(filtered_data)

def format_hero_data(df):
    df.rename(columns={'id' : 'hero_id'}, inplace=True)
    return df



def insert_hero_data_to_db(df,db):
    import sqlite3

    db = "Deadlock.db"

    #create or connect to database file
    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    def count_rows():
        cursor.execute('SELECT COUNT(*) FROM match_data')
        return cursor.fetchone()[0]  # Get the count (first item in the result)

    initial_count = count_rows()

    for hero in df:
        cursor.execute("""
            INSERT INTO heroes (hero_id, name, player_selectable, disabled)
            VALUES (?, ?, ?, ?)
        """, (
                 hero['hero_id'], 
            hero['name'],
            hero['player_selectable'], 
            hero['disabled'], 
        ))
    conn.commit()
    print("Data was successfully loaded")

    # Count the rows after insertion
    final_count = count_rows()
    
    # Calculate how many rows were added
    rows_added = final_count - initial_count

    # Get the number of rows inserted
    rows_inserted = cursor.rowcount  # This will return the number of rows inserted

    rows_matched_str = "Rows inserted matched DB diff"
    rows_not_matched_str = print(f"Rows inserted: {rows_inserted} \n DB Rows initially: {initial_count} \n DB Rows after insertion: {final_count}\n DB Rows rows after - rows initial: {rows_added} \n Expected match between rows inserted: {rows_inserted} and rows added {rows_added}\n actual diff = {rows_inserted-rows_added}")


    # Print the results
    if {rows_inserted} == {final_count-initial_count}:
        print(rows_matched_str)
    else:
        print(rows_not_matched_str)


site = "https://assets.deadlock-api.com"
endpoint = "/v2/heroes/"
url = site+endpoint
#columns to fetch
filters = ["id", "classname", "name", "description", "player_selectable", "disabled", "starting_stats", "level_info", "scaling_stats", "standard_level_up_upgrades"]

#fetch
heroes_df = fetch_hero_data(url)
#filter
heroes_df = filter_hero_data(filters, heroes_df)
#format names
heroes_df = format_hero_data(heroes_df)

