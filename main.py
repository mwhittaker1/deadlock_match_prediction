from services import orchestrators as o
from data import db
import time
from datetime import datetime, timedelta
import os
from services import database_functions as dbf
from services import function_tools as u
import logging
import duckdb
from services import model_queries as mt

log_file = os.getenv("LOGGING_LOC")  
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s.%(funcName)s] %(message)s"
)

def main():
    # ETL daily matches for 100+ badge, over max_days_fetch days. API->db.matches
    #o.run_etl_bulk_matches(max_days_fetch=60)

    # ETL hero trends for 7 and 30 days. API->db.hero_trends
    #o.run_etl_hero_trends()

    # ETL player hero trends for all players in player_matches table. db->db.player_hero_trends, db.player_roll_trends
    #o.setup_duckdb_indexes()
    o.batched_etl_player_hero_match_trends_from_db()

    # etl hero synergy stats for {days} min_average_badge, {min_matches} min_matches
    #o.run_etl_hero_synergy_trends_from_either(counter=True, synergy=True)

    # Not used in training.ETL player hero trends for all players in player_matches table. API->db.player_hero_trends, db.player_roll_trends
    #o.run_etl_player_hero_match_trends()

    pass

def train(test=True):
    # train a random forest model using weighted team stats
    if test:
        # fetch 1000 matches of data
        #test_matches = mt.fetch_and_structure_data(db.con, n=1000)
        # transofrom data

        # run training
        # run first prediction

if __name__ == "__main__":
    main()
