

#Fetch hero data
## Hero pickrate for high ranked games over past 7 days

## Hero w/l for high ranked games over past month

## Hero w/l short term trend, past 7 days

#Collect match data
#[players],"start_time", "winning_team", "match_id", "lobby_id", "duration_s","match_mode", "game_mode","region_mode_parsed" 

#Collect match players
## Expecting DataFrame of 12 account_id, hero_id pairs as pd.DataFrame[match_players]

## Sort match players, 0-5 = winning team, 6-12 = losing team

#for each player Fetch p_hero stats (dict)
### See Player/account_id/hero-stats
## p_hero total games played as p_hero_total_games (int)

## p_hero w/l over last 2 months as p_hero_2m_wl (int)

## p_hero w/l over last week as p_hero_1w_wl (int)

## player w/l over last 3 games as player_3g_wl (int)

## player w/l over last month as player_1m_wl (int)

###Collect as dict{match_data: {m_data}, team_win: [[w_team]], team_lose: [[l_team]] } 

#Send data to AI for analytics

#Return prediction

