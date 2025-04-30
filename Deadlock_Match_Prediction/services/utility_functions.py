import pandas as pd
import logging
import time
import openpyxl
from datetime import timedelta

# logging setup
def setup_logging(verbose=False):
    level = logging.DEBUG if verbose else logging.WARNING

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler("script.log"),
            logging.StreamHandler()
        ]
    )
    logging.debug("This is a debug message")
    logging.info("This is an info message")
    logging.warning("This is a warning")
    logging.error("This is an error")
    logging.critical("This is critical")

def initialize_logging(verbose):
    setup_logging(verbose=True)
    logger = logging.getLogger(__name__)
    logger.debug("Debug mode on")

#Accepts dict and saves to a .xlsx with the items name.
def to_xlsx(file, fname):
    df = pd.DataFrame(file)
    df.to_excel(f'{fname}.xlsx', index=False)
    print(f"{df} passed to .xlsx")

def to_csv(file, fname):
    df = pd.DataFrame(file)
    df.to_csv(f'{fname}.csv', index=False)
    print(f"{df} passed to .csv")

def get_time_delta(days,short=False,max=False):
    """Returns string for url if short=False, else just the int"""
    c_unix_timestamp = int(time.time()) #current time
    x_days_ago = (int(c_unix_timestamp - timedelta(days=days).total_seconds()))
    if max:
        x_days_ago = f"max_unix_timestamp={x_days_ago}"
    else:
        if short:
            return x_days_ago    
        x_days_ago = f"min_unix_timestamp={x_days_ago}"
    
        return x_days_ago

def main():
    data = {
    'name': ['John', 'Alice', 'Bob'],
    'age': [30, 25, 35],
    'city': ['New York', 'Los Angeles', 'Chicago']
    }
    df = pd.DataFrame(data)
    #to_xlsx(df)

#if __name__ == main():
#    main()