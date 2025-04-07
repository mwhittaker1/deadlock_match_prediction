from Deadlock_Hero_Data import fetch_hero_data, filter_hero_data, format_hero_data, insert_hero_data_to_db
from Deadlock_Match_Data import fetch_match_data, filter_match_data

def handle_hero_data():
    site = "https://assets.deadlock-api.com"
    endpoint = "/v2/heroes/"
    url = site+endpoint

    #columns to fetch
    filters = ["id", "classname", "name", "description", "player_selectable", "disabled", "starting_stats", "level_info", "scaling_stats", "standard_level_up_upgrades"]
    db = "Deadlock.db"

    #fetch
    heroes_df = fetch_hero_data(url)
    #filter
    heroes_df = filter_hero_data(filters, heroes_df)
    #format names
    heroes_df = format_hero_data(heroes_df)
    #insert to database
    insert_hero_data_to_db(heroes_df,db)

    print("Hero data handled")

def handle_match_data():
    #fetch
    active_matches_df = fetch_match_data()
    #filter
    simple_match_df, match_player_df = filter_match_data(active_matches_df)

    print("Match data handled")

