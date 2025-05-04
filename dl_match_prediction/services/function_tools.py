import pandas as pd
import logging
import time
from datetime import timedelta, datetime

import logging

def setup_logger(log_file="etl_log.txt"):
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] [%(name)s.%(funcName)s] %(message)s"
    )

#Accepts dict and saves to a .xlsx with the items name.
def df_to_xlsx(file, fname):
    df = pd.DataFrame(file)
    df.to_excel(f'{fname}.xlsx', index=False)
    print(f"{df} passed to .xlsx")

def df_to_csv(file, fname):
    df = pd.DataFrame(file)
    df.to_csv(f'{fname}.csv', index=False)
    print(f"{df} passed to .csv")

def get_unix_time(days_ago=0):   
    c_unix_timestamp = int(time.time()) #current time
    return int(c_unix_timestamp - timedelta(days=days_ago).total_seconds())

def get_time_delta(min_unix_time,max_time):
    """Returns string for url if short=False, else just the int"""
    min_unix_time = int(time.time()) #current time
    return (int(min_unix_time - timedelta(days=max_time).total_seconds()))

def test():
    data = {
    'name': ['John', 'Alice', 'Bob'],
    'age': [30, 25, 35],
    'city': ['New York', 'Los Angeles', 'Chicago']
    }
    df = pd.DataFrame(data)
    #to_xlsx(df)

def parse_ts(rec):
    FMT = "%Y-%m-%d %H:%M:%S"
    return datetime.strptime(rec["start_time"], FMT)

if __name__ == "__main__":
    test()