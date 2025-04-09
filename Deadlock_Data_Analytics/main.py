import requests
import pandas as pd
import numpy as np
import json

from Deadlock_Data_Fetch import fetch_data
from Deadlock_Data_Format import format_hero_data
from Deadlock_Data_Filter import filter_data
from Deadlock_Data_Insert import insert_hero_data_to_db
from Deadlock_Data_to_csv import to_csv

#Create new function that passes a list (e.g. [hero], [match], or [hero,match] and runs appropriate functions)
def main(steps: dict, args):
    current_step = ""

    #Fetches data based on true values (match/hero)
    if steps.get("fetch") == True:
        print(f"Fetching data for {args}")
        raw_data = fetch_data(args) #Returns dict of DataFrames based on numer of data tyles in *args
        current_step = "fetch"

    #Filters columns by match_filters and hero_filters. output contains 
    if steps.get("filter") == True:
        filtered = filter_data(raw_data)
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
    
    if steps.get("to_csv") == True:
        print(f"saving to .csv, filtered type is: {type(filtered)} keys are {filtered.keys()}")
        to_csv(filtered)
        current_step = "to_csv"

    print(f"all steps completed. Steps completed were: {steps}. \n ")
    if current_step == "fetch":
        print(f"fetched data is: {raw_data}")
    if current_step == "filter":
        print(f"Filtering completed, data is")
        for i,item in enumerate(filtered):
            if i ==3:
                break
            print(item)
    if current_step == "format":
        print(f"formatting completed, data is: {formated_hero_data[:3]}")
    if current_step == "insert":
        print(f"inserting data completd, data is: {format_hero_data[:3]}")
    if current_step == "to_csv":
        print("To_text was completed")

steps = {
        "fetch" : True, 
        "filter" : True,
        "format" : False, #Format = True requires hero = True
        "insert" : False,
        "to_csv" : True
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