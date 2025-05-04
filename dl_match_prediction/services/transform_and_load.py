import duckdb
import pandas as pd
import json


def normalize_bulk_matches(matches_grouped_by_day: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Normalizes bulk match data into two dataframes: matches and players."""
    matches = []
    players = []

    for day in matches_grouped_by_day: #day = key, match = value
        for match in matches_grouped_by_day[day]: # match: day = key: value | match_id: 7432551
            match_id = match["match_id"]
            start_time = match["start_time"]
            game_mode = match["game_mode"]
            match_mode = match["match_mode"]
            duration_s = match["duration_s"]
            winning_team = match["winning_team"]

            # Append to matches list
            matches.append({
                "match_id": match_id, # PK
                "start_time": start_time,
                "game_mode": game_mode,
                "match_mode": match_mode,
                "duration_s": duration_s,
                "winning_team": winning_team
            })

            # Append each player to players list
            for player in match["players"]:
                player.append({
                    "account_id": player["account_id"],
                    "match_id": match_id,
                    "team": player["team"],
                    "hero": player["hero"],
                    "kills": player["kills"],
                    "deaths": player["deaths"],
                    "assists": player["assists"],
                    "gold_per_minute": player["gold_per_minute"],
                    "xp_per_minute": player["xp_per_minute"],
                    "last_hits_per_minute": player["last_hits_per_minute"],
                    "net_worth": player["net_worth"]
                })

    # Convert lists to DataFrames
    df_matches = pd.DataFrame(matches)
    df_players = pd.DataFrame(players)

    return df_matches, df_players