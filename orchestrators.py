import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix


import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import pyparsing
import requests


import data.create_model as cm
import create_team_stats as cts
import run_predictions as rp
from data import fetch_data as fd
from data import process_data as dp

logging.basicConfig(level=logging.INFO)
logging = logging.getLogger(__name__)



def create_training_data(start_date,end_date,name="test",team_stat_model="diff"):
    """fetch player, match, and hero data to be used for training a model
    Example launch command: python orchestrators.py 2025-08-01 2025-08-05 --name my_output_folder
    
    team_stat_model = 'std' or 'diff' 
        std = creates team team stats as raw values, 2 columns per stat (team0, team1)
        diff = creates a differential of the team stats, 1 column per stats.
    """

    # Ensure the output folder exists
    folder_name = f"v2_data//pred_data//test_pred_v2_{start_date}_{end_date}//{name}"

    os.makedirs(folder_name, exist_ok=True)

    logging.info(f"Starting creation of training data. Folder_name set to {folder_name}")

    logging.info(f"Fetching batch matches from {start_date} to {end_date}")
    batch_matches = fd.bulk_fetch_matches(start_date, end_date, limit=5000)

    logging.info(f"Fetched {len(batch_matches)} matches, splitting into players and matches")
    raw_matches, raw_players = dp.separate_match_players(batch_matches)
    player_matches = raw_players[['account_id', 'match_id']]

    logging.info(f"Fetching player_hero stats, this may take a few seconds...")
    player_hero_stats= fd.fetch_player_hero_stats_batch(
    account_ids=raw_players["account_id"].unique().tolist(),
    fetch_till_date=start_date,
    fetch_from_date=None,
    batch_size=700
    )

    logging.info(f"fetched player stats for {len(player_hero_stats)}, breaking down and formatting data")
    hero_stats = pd.json_normalize(fd.fetch_hero_stats(fetch_from_date='2020-01-01', fetch_till_date=end_date))
    hero_stats = hero_stats.add_prefix('h_')
    hero_stats = hero_stats.rename(columns={'h_hero_id': 'hero_id'})

    logging.info(f"processing player stats")
    player_stats = fd.process_player_stats(player_hero_stats)

    logging.info(f"Preparing match data to be merged with stats")
    match_players = dp.prepare_match_stats(raw_players, raw_matches)

    logging.info(f"saving raw and calculated files to .csv as checkpoint.")
    # Raw files
    raw_players.to_csv(f"{folder_name}/raw_players.csv", index=False)
    raw_matches.to_csv(f"{folder_name}/raw_matches.csv", index=False)
    player_matches.to_csv(f"{folder_name}/player_matches.csv", index=False)

    # Aggregated and calculated files
    match_players.to_csv(f"{folder_name}/match_players.csv", index=False)
    player_stats.to_csv(f"{folder_name}/player_stats.csv", index=False)
    player_hero_stats.to_csv(f"{folder_name}/player_hero_stats.csv", index=False)
    hero_stats.to_csv(f"{folder_name}/hero_stats.csv", index=False)

    logging.info(f"Checking unique naming for hero, player_hero, and player stats")

    player_hero_stats, player_stats, hero_stats = dp.check_unique_naming(player_hero_stats, player_stats, hero_stats)

    logging.info(f"Merging stats into player_player_hero_hero_stats (p_ph_h_stats)")

    p_ph_h_stats = dp.merge_player_hero_stats(player_hero_stats, player_stats, hero_stats)

    logging.info(f"Merging completed, saving as p_ph_h_stats.csv to {folder_name}")
    p_ph_h_stats.to_csv(f"{folder_name}/p_ph_h_stats.csv", index=False)

    logging.info(f"calculating all stats")
    all_stats = dp.calculate_ph_stats(p_ph_h_stats)

    logging.info(f"Calculations completed, saving all stats to {folder_name}")
    all_stats.to_csv(f"{folder_name}/all_stats.csv", index=False)

    logging.info(f"Merging match data and player stats data.")
    p_m_stats = cts.merge_match_player_stats(match_players, all_stats)

    logging.info(f"Merging completed, saving as p_m_stats.csv to {folder_name}")
    p_m_stats.to_csv(f"{folder_name}/p_m_stats.csv", index=False)

    logging.info(f"Creating team stats using {team_stat_model}")
    if team_stat_model == 'std':
        team_stats = cts.create_std_team_stats(p_m_stats)
        training_data = cts.create_training_data(team_stats)
    elif team_stat_model == 'diff':
        team_stats = cts.create_std_team_stats(p_m_stats)
        training_data = cts.create_differential_training_data(team_stats)
    elif team_stat_model == 'basic':
        team_stats = cts.create_basic_team_stats(p_m_stats)
        training_data = cts.create_training_data(team_stats)

    logging.info(f"Team stats created, saving as team_stats.csv to {folder_name}")
    team_stats.to_csv(f"{folder_name}/team_stats.csv", index=False)
    training_data.to_csv(f"{folder_name}/training_data.csv", index=False)

def create_ml_model(
        training_data_file_name: str,
        training_data_folder_name: str,
        start_date: str,
        end_date: str,
        model_identifier: str,
        model_folder_name: str,
        config: str,
        test_data: str = None):
    """
    Create a random forest model using the provided training data and parameters, optional define specific test data.

    Args:
        training_data_file_name - the name of the training_data file, expects .csv
        training_data_folder_name - the unique name of the folder for training data, i.e. ...//my_output_folder
        start_date - the start date for the training data - used for folder name
        end_date - the end date for the training data - used for folder name
        model_identifier - a unique identifier for the model
        model_folder_name - the name of the folder to save the model, e.g. 8.26.25//rf_v4
        config - the path to the configuration file
        test_data - optional test data to evaluate the model

    """


    # .cfg for params to use for training
    with open(config, "r") as f:
        params = json.load(f)

    file_path = f"v2_data//pred_data//test_pred_v2_{start_date}_{end_date}//{training_data_folder_name}"

    #load and check training data
    training_data = pd.read_csv(f"{file_path}//{training_data_file_name}.csv")
    bol = cm.check_data_issues(training_data)

    if not bol:
        logging.error("Data issues found in training data.")
        sys.exit(1)
    
    # prepare training data
    X_train, X_test, y_train, y_test = cm.prep_training_data(training_data, test_data)

    model, model_id, y_pred = cm.train_random_forest(X_train, X_test, y_train, y_test, params, model_identifier)

    results = cm.evaluate_model(model, y_test, y_pred, X_test)

    cm.save_model(model, params, model_folder_name, model_id, X_test.columns)

    cm.save_report(training_data, model_id, model_folder_name, results)

    return model, model_id, y_pred, results

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["train_data", "ml_model"], required=True, help="Which function to run")
    parser.add_argument("start_date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("end_date", help="End date (YYYY-MM-DD)")
    parser.add_argument("--name", default="test", help="Output folder name")
    parser.add_argument("--team_stat_model", default="diff", help="Team stat model to use")
    # Add additional args for create_ml_model as needed
    parser.add_argument("--model_identifier", help="Model identifier (for ml_model mode)")
    parser.add_argument("--model_folder_name", help="Model folder name (for ml_model mode)")
    parser.add_argument("--config", help="Config file path (for ml_model mode)")
    parser.add_argument("--training_data_file_name", help="Training data file name (for ml_model mode)")
    parser.add_argument("--test_data", help="Test data file name (optional, for ml_model mode)")
    parser.add_argument("--training_data_folder_name", help="Training data folder name (for ml_model mode)")
    args = parser.parse_args()

    if args.mode == "train_data":
        create_training_data(args.start_date, args.end_date, args.name, args.team_stat_model)
    elif args.mode == "ml_model":
        create_ml_model(
            args.training_data_file_name,
            args.training_data_folder_name,
            args.start_date,
            args.end_date,
            args.model_identifier,
            args.model_folder_name,
            args.config,
            args.test_data
        )