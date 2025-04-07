import requests
import pandas as pd
import numpy as np

from Deadlock_Data_Fetch import fetch_data
from Deadlock_Data_Format import format_hero_data
from Deadlock_Data_Filter import filter_data
from Deadlock_Data_Insert import insert_hero_data_to_db

#Create new function that passes a list (e.g. [hero], [match], or [hero,match] and runs appropriate functions)
def main(steps: dict, *args):
    if steps.get("fetch") == True:
        print(f"Fetching data for {args}")
        raw_data = fetch_data(args) #Returns dict of DataFrames based on numer of data tyles in *args
    if steps.get("filter") == True:
        filtered = {}
        for key, df in raw_data.items(): #Calls filter_data for each df in raw_data (steps as dict of df)
            print(f"filtering data for {key}")
            filtered.update(filter_data(df, key))
    if steps.get("format") == True:
       print(f"Formatting data for {filtered.get("hero")}")
       formated_hero_data = pd.DataFrame()
       formated_hero_data = format_hero_data(filtered.get("hero"))
    if steps.get("insert") == True:
        print(f"insterting data for {formated_hero_data}")
        insert_hero_data_to_db(formated_hero_data)

steps = {
        "fetch" : True, 
        "filter" : True,
        "format" : True,
        "insert" : False
}
#data types:
#hero = hero_id and base stats
#match = match data, including player subdata
data_types = []
hero = True
match = True
if hero == True:
    data_types.append("hero")
if match == True:
    data_types.append("match")


main(steps,data_types)