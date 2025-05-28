import pandas as pd
import duckdb
from sklearn.model_selection import train_test_split
from fastai.tabular.all import *
import matplotlib.pyplot as plt
import numpy as np
from services import model_queries as mq

# Connect to database
con = duckdb.connect("data/deadlock.db")

# Execute your SQL query to get the structured data
df = mq.fetch_training_data(con)

# Check the data
print(f"Dataset shape: {df.shape}")
print(f"Sample of data:\n{df.head()}")

#check balance of the target variable
print(f"Target distribution:\n{df['team0_won'].value_counts(normalize=True)}")