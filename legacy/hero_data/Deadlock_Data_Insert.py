import sqlite3


def insert_hero_data_to_db(df,db=None):
    if db == None:
        db = "dev_Deadlock.db"

    #create or connect to database file
    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    if dev == 1:
        def count_rows():
            cursor.execute('SELECT COUNT(*) FROM heroes')
            return cursor.fetchone()[0]  # Get the count (first item in the result)

    initial_count = count_rows()

    for index, hero in df.iterrows():
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

    if dev ==1:
            
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