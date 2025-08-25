import logging
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def merge_match_player_stats(p_m_stats,p_stats)-> pd.DataFrame:
    p_m_stats = p_m_stats[['account_id','match_id','team','winning_team','win','hero_id']]
    p_m_stats = p_m_stats.merge(p_stats, on=['account_id', 'hero_id'], how='left')
    return p_m_stats

def create_std_team_stats(stats_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create team-level stats based on player&player_hero stats in min, max, mean, and std.
    """

    phm_stats = pd.DataFrame()
    phm_stats = stats_df.copy()

    # set the team stats to be set to quantiles
    team_stats = [
        'p_total_matches_played', 'p_total_time_played',
        'p_win_rate', 'ph_matches_played', 'ph_time_played',
        'ph_kills_per_min', 'ph_deaths_per_min', 'ph_accuracy',
        'ph_total_kd', 'h_total_kd', 'ph_kd_ratio', 
        'ph_hero_xp_ratio', 'ph_avg_match_length',
        'ph_avg_damage_per_match', 'h_damage_per_match', 'ph_damage_ratio',
        'ph_assists_ratio', 'ph_win_rate', 'h_total_win_rate',
        'ph_win_rate_ratio'
    ]

    # check for missing columns
    missing_cols = [col for col in team_stats if col not in phm_stats.columns]
    if missing_cols:
        print(f"*CRITICAL* Missing columns in team stats: {missing_cols}")
        return pd.DataFrame()  # Return an empty DataFrame if missing columns are found

    # for each columm, set min, max, and quantiles
    agg_function = {
        col: ["min", "max",
                "mean",
                "std"]
        for col in team_stats
    }

    agg_function['win'] = 'first'
    
    # group by columns, using agg_function as the aggregation function
    phm_stats = (phm_stats.groupby(
        ['match_id','team']).agg(
            agg_function))    
    
    # converts lambda tuples into col:val pairs
    def clean_columns(c_tuple):
        col, stat = c_tuple
        if stat == "min":
            return f"{col}_min"
        elif stat == "max":
            return f"{col}_max"
        elif stat == "mean":
            return f"{col}_mean"
        elif stat == "std":
            return f"{col}_std"
        elif callable(stat):
            return f"{col}_{stat.__name__}"
        else:
            return f"{col}_{stat}"

    phm_stats.columns = [clean_columns(col) for col in phm_stats.columns]

    return phm_stats.reset_index()





def create_training_data(team_stat_base:pd.DataFrame) -> pd.DataFrame:
    """
    Create training data by merging team stats with match outcomes.
    """
    t_stats = team_stat_base.copy()
    t_stats = t_stats.pivot(index='match_id', columns='team')

    t_stats.columns = [f'{col[0]}_{col[1]}' for col in t_stats.columns]
    t_stats['team_0_win'] = t_stats['win_first_Team0']
    t_stats.drop('win_first_Team0', axis=1, inplace=True)
    t_stats.drop('win_first_Team1', axis=1, inplace=True)
    t_stats = t_stats.reset_index()

    return t_stats

def create_differential_training_data(team_stat_base:pd.DataFrame) -> pd.DataFrame:
    """
    Create training data by merging team stats with match outcomes.
    Each stat is a differential: Team0 - Team1
    """
    t_stats = team_stat_base.copy()
    diff_cols = {}

    # create differential columns
    for col in t_stats.columns:
        if col.endswith('_Team0'):
            base_col = col[:-6]  # remove '_Team0'
            team1_col = f'{base_col}_Team1'
            if team1_col in t_stats.columns:
                diff_name = f'{base_col}_diff'
                diff_cols[diff_name] = t_stats[col] - t_stats[team1_col]

    non_team_cols = [col for col in t_stats.columns if not (col.endswith('_Team0') or col.endswith('_Team1'))]
    result = t_stats[non_team_cols].copy()
    for diff_name, diff_series in diff_cols.items():
        result[diff_name] = diff_series.round(3)
    result = result.reset_index(drop=True)
    return result


if __name__ == "__main__":
    start_date = "2025-08-19"
    end_date = "2025-08-21"
    folder_name = f"v2_data//pred_data//test_pred_v2_{start_date}_{end_date}"

    player_match_stats = pd.read_csv(f"{folder_name}/player_match_stats.csv")
    all_stats = pd.read_csv(f"{folder_name}/all_stats.csv")

    p_m_stats = merge_match_player_stats(player_match_stats, all_stats)
    p_m_stats.to_csv(f"{folder_name}/p_m_stats.csv", index=False)

    # Create standard deviation stats
    team_stats = create_std_team_stats(p_m_stats)
    team_stats.to_csv(f"{folder_name}/team_match_stats.csv", index=False)

    # Format to training data
    training_data = create_training_data(team_stats)
    training_data.to_csv(f"{folder_name}/training_data.csv", index=False)

    # Create differential training data
    diff_training_data = create_differential_training_data(team_stats)
    diff_training_data.to_csv(f"{folder_name}/diff_training_data.csv", index=False)