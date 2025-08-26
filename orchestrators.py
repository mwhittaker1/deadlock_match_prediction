from data import fetch_data as fd
from data import process_data as dp
import create_model as cm
import create_team_stats as cts
import run_predictions as rp
import pandas as pd
import numpy as np
import json
import requests
import os
import sys
from datetime import timedelta, datetime
from urllib.parse import urlencode
import argparse
import pyparsing
import time
from datetime import timedelta, datetime, timezone

def create_training_data(start_date,end_date,name="test"):
    """fetch player, match, and hero data to be used for training a model"""
    # Ensure the output folder exists
    folder_name = f"v2_data//pred_data//test_pred_v2_{start_date}_{end_date}//{name}"

    os.makedirs(folder_name, exist_ok=True)

    batch_matches = fd.bulk_fetch_matches(start_date, end_date, limit=5000)

    raw_matches, raw_players = dp.separate_match_players(batch_matches)
    player_matches = raw_players[['account_id', 'match_id']]

    bulk_player_hero_stats= fd.fetch_player_hero_stats_batch(
    account_ids=raw_players["account_id"].unique().tolist(),
    fetch_till_date=start_date,
    fetch_from_date=None,
    batch_size=700
    )

    hero_stats = pd.json_normalize(fd.fetch_hero_stats(fetch_from_date='2020-01-01', fetch_till_date=end_date))
    hero_stats = hero_stats.add_prefix('h_')
    hero_stats = hero_stats.rename(columns={'h_hero_id': 'hero_id'})

    p_stats = fd.process_player_stats(bulk_player_hero_stats)

    bulk_player_hero_stats.to_csv(f"{folder_name}/player_hero_stats.csv", index=False)
    raw_players.to_csv(f"{folder_name}/raw_players.csv", index=False)
    raw_matches.to_csv(f"{folder_name}/raw_matches.csv", index=False)
    hero_stats.to_csv(f"{folder_name}/h_stats.csv", index=False)
    player_matches.to_csv(f"{folder_name}/player_matches.csv", index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("start_date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("end_date", help="End date (YYYY-MM-DD)")
    parser.add_argument("--name", default="test", help="Output folder name")
    args = parser.parse_args()
    create_training_data(args.start_date, args.end_date, args.name)