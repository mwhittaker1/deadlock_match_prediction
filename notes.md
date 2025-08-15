# Training Stages

### Stage 1 ###
User player stats and player hero stats for all players on a team to create team-level aggregates
Compare team data and predict outcome

Player features- 
  KD, 
  Total Matches, 

Hero features-
  kd_mean
  time_played_mean
  matches_mean

Player Hero features-
  player_hero_score, # What is this player_hero's stats compared to this players other hereos? (resolved with ratios of player vs hero average)
  kd,


Team aggregated stats- 
  team min avg_kd, 
  team median avg_kd, 
  team max avg_kd,
  team min experience (total matches)
  team median experience
  team max experience
  player_hero min avg_kd,
  player_hero median avg_kd,
  player_hero max avg_kd,
  player_hero min experiances
  player_hero max experiances
  player_hero median experiances
  player_hero min skill_score
  player_hero Max skill_score
  player_hero median skill_score


Attempt prediction

Player - Total K:D

Normalize player stats