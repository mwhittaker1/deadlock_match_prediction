from services import orchestrators as o
from data import db
import os
from services import database_functions as dbf
from services import function_tools as u
import logging

log_file = os.getenv("LOGGING_LOC")  
logging.basicConfig(
    filename=log_file,
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] [%(name)s.%(funcName)s] %(message)s"
)

def main():
    #dbf.reset_all_tables(db.con)
    #run_etl_bulk_matches(max_days_fetch=5)
    o.run_etl_hero_trends()
    #dbf.create_player_rolling_stats(db.con)
    pass
if __name__ == "__main__":
    main()
