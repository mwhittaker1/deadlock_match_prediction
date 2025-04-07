import requests
import pandas as pd
import numpy as np

def format_hero_data(df):
    df.rename(columns={'id' : 'hero_id'}, inplace=True)
    return df