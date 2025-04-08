import requests
import pandas as pd
import numpy as np
import json

from Deadlock_Data_Fetch import fetch_data
from Deadlock_Data_Format import format_hero_data
from Deadlock_Data_Filter import filter_data
from Deadlock_Data_Insert import insert_hero_data_to_db

#Create new function that passes a list (e.g. [hero], [match], or [hero,match] and runs appropriate functions)
def main(steps: dict, args):
    current_step = ""
    if steps.get("fetch") == True:
        print(f"Fetching data for {args}")
        raw_data = fetch_data(args) #Returns dict of DataFrames based on numer of data tyles in *args
        current_step = "fetch"

    if steps.get("filter") == True:
        filtered = {}
        for key, df in raw_data.items(): #Calls filter_data for each df in raw_data (steps as dict of df)
            print(f"filtering data for {key}")
            filtered.update(filter_data(df, key))
        current_step = "filter"

    if steps.get("format") == True:
        print(f"Formatting data for {filtered.get("hero")}")
        formated_hero_data = pd.DataFrame()
        formated_hero_data = format_hero_data(filtered.get("hero"))
        current_step = "format"

    if steps.get("insert") == True:
        print(f"insterting data for {formated_hero_data}")
        insert_hero_data_to_db(formated_hero_data)
        current_step = "insert"

    print(f"all steps completed. Steps completed were: {steps}. \n ")
    if current_step == "fetch":
        print(f"fetched data is: {raw_data[:3]}")
    if current_step == "filter":
        print(f"Filtering completed, data is")
        for i in range(3):
            print({key: filtered[key][i] for key in filtered})
    if current_step == "format":
        print(f"formatting completed, data is: {formated_hero_data[:3]}")
    if current_step == "insert":
        print(f"inserting data completd, data is: {format_hero_data[:3]}")

steps = {
        "fetch" : True, 
        "filter" : True,
        "format" : False, #Format = True requires hero = True
        "insert" : False
}
#data types:
#hero = hero_id and base stats
#match = match data, including player subdata
data_types = []
hero = False
match = True
if hero == True:
    data_types.append("hero")
if match == True:
    data_types.append("match")

if hero != steps["format"]:
    print("Hero and format do not match boolean. hero = {hero} and format = {format}")
else: 
    print(f"Let's go!, data_types are: {data_types} and steps are {steps}")
    main(steps,data_types)