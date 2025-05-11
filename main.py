from services import orchestrators as o
from data import db
import os
from services import database_functions as dbf
from services import function_tools as u
import logging

log_file = os.getenv("LOGGING_LOC")  
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s.%(funcName)s] %(message)s"
)

def main():
    #dbf.reset_all_tables(con=db.con)

    o.run_etl_bulk_matches(max_days_fetch=60)
    #o.run_etl_hero_trends()
    #o.run_etl_player_hero_match_trends()
    pass
if __name__ == "__main__":
    main()
