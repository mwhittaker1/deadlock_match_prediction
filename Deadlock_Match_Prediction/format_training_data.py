import pandas as pd
import duckdb
import pickle
from inline.database_foundations import manage_tbl_temp_p_m_history
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
"""
for 1 match, a player has:
(need to calculate?) current_match_id
account_id
hero_id
p_win_pct_3
p_win_pct_5
p_streak_3
p_streak_5
h_win_pct_3
h_win_pct_5
h_streak_3
h_streak_5
last_4_matches_win_pct
last_4_matches_h_win_pct
---hero---
trend_window_7
pick_rate
win_rate
average_kills
average_deaths
average_assists
trend_window_30
pick_rate
win_rate
average_kills
average_deaths
average_assists
trend_window_30
"""

def dev_build_player_match_history(con, out_csv=False, insert=False, output_csv="player_history_dev.csv"):
    """
    From database:
    Build player match history and hero match history.
    Output as CSV or insert to temp_tbl_p_m_history.
    
    con: DuckDB connection
    output_csv: Path to save the CSV output
        
    Returns:
        DataFrame with player match history features
    """

    # Execute the window function query
    enriched_df = con.execute("""
    WITH player_match_history AS (
    SELECT
        pm.account_id,
        pm.match_id,
        pm.hero_id,
        pm.player_team,
        pm.won,
        m.start_time,
                              
        AVG(CAST(pm.won AS INTEGER)) OVER (
        PARTITION BY pm.account_id
        ORDER BY m.start_time
        ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) * 100 AS last4_matches_win_pct,
 
        COUNT(*) OVER (
        PARTITION BY pm.account_id
        ORDER BY m.start_time
        ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS prev_match_count
    FROM player_matches pm
    JOIN matches m ON pm.match_id = m.match_id
    ORDER BY pm.account_id, m.start_time),
    player_hero_history AS (
    SELECT
        pmh.*,
        AVG(CASE WHEN hero_id = pmh.hero_id THEN won::INT ELSE NULL END) OVER (
        PARTITION BY account_id, hero_id
        ORDER BY start_time
        ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) * 100 AS last4_hero_matches_win_pct,
        COUNT(CASE WHEN hero_id = pmh.hero_id THEN 1 ELSE NULL END) OVER (
        PARTITION BY account_id, hero_id
        ORDER BY start_time
        ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        ) AS prev_hero_match_count
    FROM player_match_history pmh
    )
    SELECT
    account_id,
    match_id,
    hero_id,
    player_team,
    won,
    CASE WHEN prev_match_count >= 1 THEN last4_matches_win_pct ELSE NULL END AS last4_matches_win_pct,
    CASE WHEN prev_hero_match_count >= 1 THEN last4_hero_matches_win_pct ELSE NULL END AS last4_hero_matches_win_pct
    FROM player_hero_history
    ORDER BY account_id, start_time;
    """).fetchdf()
    
    # Save to CSV
    if out_csv:
        enriched_df.to_csv(output_csv, index=False)
    print(f"***Exported player match history to {output_csv}***")

    if insert:
        manage_tbl_temp_p_m_history(enriched_df,True)
        print(f"\n\n*DEBUG* manage_tbl_temp_p_m_history completed.")
    return enriched_df

def build_match_player_level(df)->pd.DataFrame:
    """
    df: dataframe of players level (chance to win)
    returns as match grouped data to calculate match chance to win
    """
    
    matches = []

    for match_id, group in df.groupby('match_id'):
        if group.shape[0] != 12:
            continue
        
        team0 = group[group['player_team'] == 0].sort_values('hero_id').reset_index(drop=True)
        team1 = group[group['player_team'] == 1].sort_values('hero_id').reset_index(drop=True)
        
        if team0.shape[0] != 6 or team1.shape[0] != 6:
            continue

        team0_won = team0['won'].mode()[0]
        match_win = 1 if team0_won else 0

        row = {'match_id': match_id, 'won': match_win}
        team0 = team0.reset_index(drop=True)
        team1 = team1.reset_index(drop=True)
        for idx, player in team0.iterrows():
            prefix = f"t0_p{idx+1}"
            row.update({
                f"{prefix}_last4_matches_win_pct": player['last4_matches_win_pct'],
                f"{prefix}_last4_hero_matches_win_pct": player['last4_hero_matches_win_pct']
            })
        for idx, player in team1.iterrows():
            prefix = f"t1_p{idx+1}"
            row.update({
                f"{prefix}_last4_matches_win_pct": player['last4_matches_win_pct'],
                f"{prefix}_last4_hero_matches_win_pct": player['last4_hero_matches_win_pct']
            })

        matches.append(row)
    matches.fillna(0,inplace=True)
    matches.to_excel(f'match_df.xlsx', index=False)
    return pd.DataFrame(matches)

def normalize_for_training(df):
    """"""
    feature_cols = [col for col in df.columns if col not in ['match_id', 'won']]
    X = df[feature_cols]
    y = df['won']  
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    return X_scaled, y, X, feature_cols

def train_model(x_train, y_train):
    model = LogisticRegression(max_iter=1000)
    model.fit(x_train, y_train)
    with open('trained_model.pkl', 'wb') as f:
        pickle.dump(model, f)
    return model

def open_model():
    with open('trained_model.pkl', 'rb') as f:
        model = pickle.load(f)
    return model

def show_model_features(model, feature_cols):
    coefficients = model.coef_[0]  # model.coef_ returns array of shape (1, n_features)
    feature_importance = pd.DataFrame({
    'feature': feature_cols,
    'weight': coefficients
    })

    # Sort by absolute value of weight (biggest influence)
    feature_importance['abs_weight'] = feature_importance['weight'].abs()
    feature_importance = feature_importance.sort_values(by='abs_weight', ascending=False)

    print(feature_importance[['feature', 'weight']].head(20))

def predict_matches(model, X_test, y_test):
    y_pred = model.predict(X_test)
    y_pred_proba = model.predict_proba(X_test)
    for idx in range(min(5, len(X_test))):  # Show 10 predictions as a sample
        match_id = idx  # (we don't have real match_id, so just indexing for now)

        team0_score = y_pred_proba[idx][1]  # Probability Team 0 wins
        team1_score = 1 - team0_score        # Probability Team 1 wins
        predicted_team_won = 1 if team0_score > 0.5 else 0
        confidence = max(team0_score, team1_score) * 100
        actual_team_won = y_test.iloc[idx]
        correct = (predicted_team_won == actual_team_won)

        print(f"---")
        print(f"Evaluated match: {match_id}")
        print(f"  Team 0 score: {team0_score:.2f}")
        print(f"  Team 1 score: {team1_score:.2f}")
        print(f"  Expected winning team: Team {predicted_team_won} with confidence {confidence:.1f}%")
        print(f"  Actual winning team: Team {actual_team_won}")
        if correct:
            print(f"✅ Model was correct!")
        else:
            print(f"❌ Model was incorrect.")

def build_training():
    """takes df from build_player_match_history
    builds training data
    splits 20% of matches for testing
    trains model
    """
    player_df = pd.read_csv("player_history_dev.csv")

    """test data didn't have correc matches, this pulls only matches with 12 players"""
    match_counts = player_df['match_id'].value_counts()
    valid_match_ids = match_counts[match_counts == 12].index
    filtered_player_df = player_df[player_df['match_id'].isin(valid_match_ids)]
    print(f"***Filtered to {filtered_player_df['match_id'].nunique()} full matches***")
    
    match_df = build_match_player_level(filtered_player_df)
    #match_df = pd.read_excel("match_df.xlsx")

    X_scaled, y, X, feature_cols = normalize_for_training(match_df)
    X_train, y_train, X_test, y_test, feature_cols = train_test_split(X_scaled, y, test_size=0.2, random_state=42)
    dfX_test = pd.DataFrame(X_test)
    dfX_test.to_csv("X_test.csv",index=False)
    dfY_test = pd.DataFrame(y_test)
    dfY_test.to_csv("y_test.csv",index=False)
    dffeature_cols = pd.DataFrame(feature_cols)
    dffeature_cols.to_csv("feature_cols.csv",index=False)
    model = train_model(X_train, y_train)
    return feature_cols, model


if __name__ == "__main__":
    feature_cols, model = build_training()
    #model = open_model()
    show_model_features(model,feature_cols)
    #X_test=(pd.read_csv(f"X_test.csv"))
    #y_test=(pd.read_csv(f"y_test.csv"))
    #predict_matches(model, X_test, y_test)


