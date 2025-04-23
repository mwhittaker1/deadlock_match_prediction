import pandas as pd
from Deadlock_Match_Prediction.services.utility_functions import to_csv, to_xlsx, get_time_delta, setup_logging, initialize_logging
from Deadlock_Match_Prediction.services.dl_fetch_data import fetch_active_match_data, fetch_hero_data, fetch_match_data, fetch_player_hero_stats, fetch_hero_info, fetch_player_match_history
from Deadlock_Match_Prediction.services.dl_process_data import filter_account_data, filter_match_data, filter_player_hero_data, split_players_from_matches, calculate_hero_stats, calculate_player_hero_stats, match_data_outcome_add
from Deadlock_Match_Prediction.data_orchestrator import orchestrate_match_data

### fetch set of matches for training ###


### Build full dataset for each match ###
## for each hero in match -> hero_stats ##
## for each player, player_match_history -> player_hero_match_history
# df,match_id, account_id, hero_id, player_7d_wl_pct, player_30d_wl_pct, hero_7d_wl_pct, hero_30d_wl_pct, player_hero_total_matches, player_hero_pick_%
#



def orchestrate_match_hero_data(limit,days,min_average_badge):
    limit = 1
    single_match = orchestrate_match_data(limit,days,min_average_badge)
    m_id = single_match['match_id'][0]
    match_data = orchestrate_match_data(limit,days,min_average_badge,m_id)
    for row in match_data:
        current_p_id = row["account_id"]

    print(f"\n\n *** from orchestrate_match_hero_data. m_id = {m_id}***")
    return