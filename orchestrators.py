import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

import numpy as np
import pandas as pd
import pyparsing
import requests

import create_model as cm
import create_team_stats as cts
import run_predictions as rp
from data import fetch_data as fd
from data import process_data as dp

logging.basicConfig(level=logging.INFO)
logging = logging.getLogger(__name__)



def create_training_data(start_date,end_date,name="test"):
    """fetch player, match, and hero data to be used for training a model
    Example launch command: python orchestrators.py 2025-08-01 2025-08-05 --name my_output_folder
    
    """
    # Ensure the output folder exists
    folder_name = f"v2_data//pred_data//test_pred_v2_{start_date}_{end_date}//{name}"

    os.makedirs(folder_name, exist_ok=True)

    batch_matches = fd.bulk_fetch_matches(start_date, end_date, limit=5000)

    raw_matches, raw_players = dp.separate_match_players(batch_matches)
    player_matches = raw_players[['account_id', 'match_id']]

    player_hero_stats= fd.fetch_player_hero_stats_batch(
    account_ids=raw_players["account_id"].unique().tolist(),
    fetch_till_date=start_date,
    fetch_from_date=None,
    batch_size=700
    )

    hero_stats = pd.json_normalize(fd.fetch_hero_stats(fetch_from_date='2020-01-01', fetch_till_date=end_date))
    hero_stats = hero_stats.add_prefix('h_')
    hero_stats = hero_stats.rename(columns={'h_hero_id': 'hero_id'})

    player_stats = fd.process_player_stats(player_hero_stats)

    raw_players.to_csv(f"{folder_name}/raw_players.csv", index=False)
    raw_matches.to_csv(f"{folder_name}/raw_matches.csv", index=False)

    player_matches.to_csv(f"{folder_name}/player_matches.csv", index=False)
    player_stats.to_csv(f"{folder_name}/p_stats.csv", index=False)
    player_hero_stats.to_csv(f"{folder_name}/player_hero_stats.csv", index=False)
    hero_stats.to_csv(f"{folder_name}/h_stats.csv", index=False)

    logging.info(f"Checking unique naming for hero, player_hero, and player stats")

    player_hero_stats, player_stats, hero_stats = dp.check_unique_naming(player_hero_stats, p_stats, hero_stats)

    logging.info(f"Merging stats into player_player_hero_hero_stats (p_ph_h_stats)")

    p_ph_h_stats = dp.merge_player_hero_stats(player_hero_stats, player_stats, hero_stats)

    logging.info(f"Merging completed, saving as p_ph_h_stats.csv to {folder_name}")
    p_ph_h_stats.to_csv(f"{folder_name}/p_ph_h_stats.csv", index=False)

    logging.info(f"calculating all stats")
    all_stats = dp.calculate_ph_stats(p_ph_h_stats)

    logging.info(f"Calculations completed, saving all stats to {folder_name}")
    all_stats.to_csv(f"{folder_name}/all_stats.csv", index=False)

    logging.info(f"Preparing match data to be merged with stats")
    player_match_stats = dp.prepare_match_stats(raw_matches, player_stats)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("start_date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("end_date", help="End date (YYYY-MM-DD)")
    parser.add_argument("--name", default="test", help="Output folder name")
    args = parser.parse_args()
    create_training_data(args.start_date, args.end_date, args.name)