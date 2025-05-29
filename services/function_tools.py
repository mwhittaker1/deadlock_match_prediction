import logging
import time
import json
import csv
import os
import pandas as pd
from typing import Any, List, Dict, Union
from datetime import timedelta, datetime

def any_to_csv(data: Any, file_name: str) -> str:
    """
    Converts various data formats to CSV and saves to a file.
    
    Parameters:
    -----------
    data : Any
        The data to convert, can be DataFrame, JSON string, list, dict, 
        list of DataFrames, list of dicts, or complex nested structures.
    file_name : str
        The output file name (will append .csv if not present)
    
    Returns:
    --------
    str
        The path to the saved CSV file
    
    Raises:
    -------
    ValueError
        If the data format cannot be processed
    """
    # Ensure file has .csv extension
    if not file_name.endswith('.csv'):
        file_name += '.csv'
    
    # Convert based on data type
    if isinstance(data, pd.DataFrame):
        # Handle pandas DataFrame
        data.to_csv(file_name, index=False)
    
    elif isinstance(data, str):
        # Try to parse as JSON
        try:
            json_data = json.loads(data)
            df = pd.json_normalize(json_data)
            df.to_csv(file_name, index=False)
        except json.JSONDecodeError:
            raise ValueError("String input must be valid JSON")
    
    elif isinstance(data, list):
        if all(isinstance(item, pd.DataFrame) for item in data):
            # List of DataFrames - concatenate them
            pd.concat(data, ignore_index=True).to_csv(file_name, index=False)
        
        elif all(isinstance(item, dict) for item in data):
            # List of dictionaries
            pd.DataFrame(data).to_csv(file_name, index=False)
        
        elif len(data) > 0:
            # Other list types - try to convert to DataFrame
            try:
                pd.DataFrame(data).to_csv(file_name, index=False)
            except Exception:
                # For complex nested lists, flatten it first
                flattened_data = flatten_nested_list(data)
                pd.DataFrame(flattened_data).to_csv(file_name, index=False)
        else:
            # Empty list
            pd.DataFrame().to_csv(file_name, index=False)
    
    elif isinstance(data, dict):
        # Handle dictionary
        pd.DataFrame([data]).to_csv(file_name, index=False)
    
    else:
        raise ValueError(f"Unsupported data type: {type(data)}")
    
    return file_name

def flatten_nested_list(nested_list: List) -> List[Dict]:
    """
    Flattens a complex nested list structure into a list of dictionaries
    that can be converted to a DataFrame.
    
    Parameters:
    -----------
    nested_list : List
        A possibly nested list structure
    
    Returns:
    --------
    List[Dict]
        A flattened list ready for DataFrame conversion
    """
    result = []
    
    # If all items are similar, try to preserve structure
    if all(isinstance(item, list) for item in nested_list) and all(len(item) == len(nested_list[0]) for item in nested_list):
        # Possibly a matrix or table-like structure
        return [dict(zip(range(len(row)), row)) for row in nested_list]
    
    # Process each item
    for item in nested_list:
        if isinstance(item, dict):
            result.append(item)
        elif isinstance(item, list):
            # Recursively flatten nested lists
            flattened = flatten_nested_list(item)
            result.extend(flattened)
        else:
            # Basic items become single-column entries
            result.append({"value": item})
    
    return result

def df_to_xlsx(file, fname):
    df = pd.DataFrame(file)
    df.to_excel(f'{fname}.xlsx', index=False)


def df_to_csv(file, fname):
    df = pd.DataFrame(file)
    df.to_csv(f'{fname}.csv', index=False)


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