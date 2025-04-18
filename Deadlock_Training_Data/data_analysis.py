

# Collect base data to be used for training an AI model.


#                          Hero Stats                  #
## Hero pickrate for high ranked games over past 7 days (Identify short term trends)

## Hero w/l for high ranked games over past month (Long-term win/loss bias)

## Hero w/l short term trend, past 7 days (Short-term win/loss bias)


#                         Player Stats                #
## Player w/l over last 3 games (Short term tilt factor)
## Player w/l over past month (Bias for winning?)

#                        Player_Hero Stats             #
## p_hero total games as hero (experience modifier)

## p_hero w/l over past 30 days (Player skill)

## p_hero hero recency frequency (Player may be rusty with hero)




#                       Match Stats                 #

## Sort match players, 0-5 = winning team, 6-12 = losing team
#Collect match data for prior time period
#[players],"start_time", "winning_team", "match_id", "lobby_id", "duration_s","match_mode", "game_mode","region_mode_parsed" 


###Collect as dict{match_data: {m_data}, team_win: [[w_team]], team_lose: [[l_team]] } 

#Send data to AI for analytics

#Return prediction

