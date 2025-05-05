from handlers import orchestrators as o
from data import db
from services import database_functions as dbf
from services import function_tools as u
import logging
u.setup_logger()
logging.info("Logger initialized.")

def main():
    o.run_etl_bulk_matches(max_days_fetch=3)

if __name__ == "__main__":
    con = db.con
    dbf.reset_all_tables(con)
    main()
