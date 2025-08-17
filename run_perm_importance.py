
import pandas as pd
import numpy as np
import joblib
import json
import os
import argparse
from sklearn.inspection import permutation_importance
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

""" Run permutation importance on a set of models, compare the outputs between models
    Two ways to run the script:
        python run_perm_importance.py --mode individual (default): Run permutation importance on individual features
        python run_perm_importance.py --mode grouped: Run permutation importance on feature groups
    The script automatically groups features by their base type, e.g.
        ph_wins_min_Team0, ph_wins_max_Team0, ph_wins_median_Team0 → grouped as ph_wins
        ph_kd_ratio_q25_Team1, ph_kd_ratio_q75_Team1 → grouped as ph_kd_ratio
        damage_per_min_diff, damage_per_min_Team0 → grouped as damage_per_min
"""

def load_model_data(model_folder):
    """Load model, metadata, and test data from a model folder"""
    try:
        # Load model
        model = joblib.load(os.path.join(model_folder, 'model.joblib'))
        
        # Load metadata
        with open(os.path.join(model_folder, 'meta.json'), 'r') as f:
            meta = json.load(f)
        
        # Load test data
        test_data = pd.read_csv(os.path.join(model_folder, 'samples', 'Xy_test.csv'))
        X_test = test_data.drop(columns=['target'])
        y_test = test_data['target']
        
        return model, meta, X_test, y_test
    except Exception as e:
        print(f"Error loading model from {model_folder}: {e}")
        return None, None, None, None

def group_features_by_type(feature_names):
    """Group features by their base type (e.g., all win_rate features together)"""
    feature_groups = {}
    
    for feature in feature_names:
        # Skip index and match_id columns
        if feature in ['Unnamed: 0', 'match_id']:
            continue
            
        # Extract base feature type
        base_feature = feature
        
        # Remove differential suffix first if present
        if base_feature.endswith('_diff'):
            base_feature = base_feature[:-5]
        
        # Remove team indicators (Team0, Team1)
        base_feature = base_feature.replace('_Team0', '').replace('_Team1', '')
        
        # Remove statistical suffixes (min, max, q25, median, q75, mean, std)
        statistical_suffixes = ['_min', '_max', '_q25', '_median', '_q75', '_mean', '_std']
        for suffix in statistical_suffixes:
            if base_feature.endswith(suffix):
                base_feature = base_feature[:-len(suffix)]
                break
        
        # Remove ratio suffix if present (e.g., ph_kd_ratio -> ph_kd)
        if base_feature.endswith('_ratio'):
            base_feature = base_feature[:-6]
        
        # Group features
        if base_feature not in feature_groups:
            feature_groups[base_feature] = []
        feature_groups[base_feature].append(feature)
    
    return feature_groups

def run_permutation_importance(model, X_test, y_test, feature_names, n_repeats=5, random_state=42):
    """Run permutation importance on individual features"""
    print(f"Running individual permutation importance with {len(feature_names)} features...")
    
    perm_importance = permutation_importance(
        model, X_test, y_test, 
        n_repeats=n_repeats, 
        random_state=random_state,
        scoring='accuracy'
    )
    
    # Create DataFrame with results
    importance_df = pd.DataFrame({
        'feature': feature_names,
        'importance_mean': perm_importance.importances_mean,
        'importance_std': perm_importance.importances_std
    }).sort_values('importance_mean', ascending=False)
    
    return importance_df

def run_grouped_permutation_importance(model, X_test, y_test, feature_groups, n_repeats=5, random_state=42):
    """Run permutation importance on feature groups"""
    print(f"Running grouped permutation importance on {len(feature_groups)} feature groups...")
    
    group_importance_results = []
    
    for group_name, features in feature_groups.items():
        print(f"  Processing group: {group_name} ({len(features)} features)")
        
        # Get indices of features in this group
        feature_indices = [X_test.columns.get_loc(feat) for feat in features if feat in X_test.columns]
        
        if not feature_indices:
            print(f"    Warning: No features found for group {group_name}")
            continue
        
        # Calculate baseline score
        baseline_score = model.score(X_test, y_test)
        
        # Perform grouped permutation
        group_scores = []
        for _ in range(n_repeats):
            X_permuted = X_test.copy()
            
            # Permute all features in the group together
            permutation = np.random.RandomState(random_state + _).permutation(len(X_test))
            for idx in feature_indices:
                X_permuted.iloc[:, idx] = X_permuted.iloc[permutation, idx]
            
            # Calculate score with permuted features
            permuted_score = model.score(X_permuted, y_test)
            group_scores.append(baseline_score - permuted_score)
        
        # Store results
        group_importance_results.append({
            'feature_group': group_name,
            'importance_mean': np.mean(group_scores),
            'importance_std': np.std(group_scores),
            'num_features': len(features),
            'features': ', '.join(features)
        })
    
    # Convert to DataFrame and sort
    importance_df = pd.DataFrame(group_importance_results).sort_values('importance_mean', ascending=False)
    
    return importance_df

def save_permutation_results(importance_df, model_folder, model_id, mode="individual"):
    """Save permutation importance results to CSV"""
    suffix = "grouped" if mode == "grouped" else "individual"
    output_file = os.path.join(model_folder, f"{model_id}_permutation_importance_{suffix}.csv")
    importance_df.to_csv(output_file, index=False)
    print(f"Saved {mode} permutation importance to: {output_file}")
    return output_file

def compare_model_features(all_results, top_n=20, mode="individual"):
    """Compare feature importance across models"""
    comparison_data = []
    
    # Get top N features for each model
    for model_name, importance_df in all_results.items():
        top_features = importance_df.head(top_n)
        for idx, row in top_features.iterrows():
            if mode == "grouped":
                comparison_data.append({
                    'model': model_name,
                    'feature_group': row['feature_group'],
                    'importance_mean': row['importance_mean'],
                    'importance_std': row['importance_std'],
                    'num_features': row['num_features'],
                    'rank': idx + 1
                })
            else:
                comparison_data.append({
                    'model': model_name,
                    'feature': row['feature'],
                    'importance_mean': row['importance_mean'],
                    'importance_std': row['importance_std'],
                    'rank': idx + 1
                })
    
    comparison_df = pd.DataFrame(comparison_data)
    
    # Create pivot tables
    feature_col = 'feature_group' if mode == "grouped" else 'feature'
    rank_pivot = comparison_df.pivot(index=feature_col, columns='model', values='rank')
    importance_pivot = comparison_df.pivot(index=feature_col, columns='model', values='importance_mean')
    
    return comparison_df, rank_pivot, importance_pivot

def analyze_feature_differences(rank_pivot, importance_pivot):
    """Analyze differences in feature importance across models"""
    analysis = {}
    
    # Features that appear in top N for all models
    complete_features = rank_pivot.dropna().index.tolist()
    analysis['features_in_all_models'] = complete_features
    
    # Features that appear in some but not all models
    partial_features = rank_pivot[rank_pivot.isnull().any(axis=1)].index.tolist()
    analysis['features_in_some_models'] = partial_features
    
    # Most variable features (highest std deviation in ranks)
    if len(complete_features) > 0:
        rank_std = rank_pivot.loc[complete_features].std(axis=1).sort_values(ascending=False)
        analysis['most_variable_features'] = rank_std.head(10).to_dict()
    
    # Most consistent features (lowest std deviation in ranks)
    if len(complete_features) > 0:
        analysis['most_consistent_features'] = rank_std.tail(10).to_dict()
    
    return analysis

def main():
    parser = argparse.ArgumentParser(description='Run permutation importance on models')
    parser.add_argument('--mode', choices=['individual', 'grouped'], default='individual',
                       help='Run permutation importance on individual features or grouped by type')
    args = parser.parse_args()
    
    models_dir = "models"
    results_dir = os.path.join(models_dir, "model_perm_results")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    mode = args.mode
    
    print(f"Running permutation importance in {mode} mode")
    
    # Create results directory if it doesn't exist
    os.makedirs(results_dir, exist_ok=True)
    
    # Find all model folders (exclude the results folder)
    model_folders = [d for d in os.listdir(models_dir) 
                    if os.path.isdir(os.path.join(models_dir, d)) and d != "model_perm_results"]
    print(f"Found {len(model_folders)} model folders: {model_folders}")
    
    all_results = {}
    
    # Process each model
    for folder in model_folders:
        model_folder = os.path.join(models_dir, folder)
        print(f"\n{'='*50}")
        print(f"Processing model: {folder}")
        print(f"{'='*50}")
        
        # Load model and data
        model, meta, X_test, y_test = load_model_data(model_folder)
        
        if model is None:
            print(f"Skipping {folder} due to loading error")
            continue
        
        print(f"Model ID: {meta['run_id']}")
        print(f"Test set size: {len(X_test)} samples, {len(X_test.columns)} features")
        
        # Run permutation importance based on mode
        if mode == "grouped":
            # Group features and run grouped permutation importance
            feature_names = X_test.columns.tolist()
            feature_groups = group_features_by_type(feature_names)
            
            print(f"Feature groups identified:")
            for group_name, features in feature_groups.items():
                print(f"  {group_name}: {len(features)} features")
            
            importance_df = run_grouped_permutation_importance(model, X_test, y_test, feature_groups)
        else:
            # Run individual feature permutation importance
            feature_names = X_test.columns.tolist()
            importance_df = run_permutation_importance(model, X_test, y_test, feature_names)
        
        # Save results
        save_permutation_results(importance_df, model_folder, meta['run_id'], mode)
        
        # Store for comparison
        all_results[folder] = importance_df
        
        # Print top 10 features/groups
        if mode == "grouped":
            print(f"\nTop 10 most important feature groups:")
            print(importance_df.head(10)[['feature_group', 'importance_mean', 'importance_std', 'num_features']].to_string(index=False))
        else:
            print(f"\nTop 10 most important features:")
            print(importance_df.head(10)[['feature', 'importance_mean', 'importance_std']].to_string(index=False))
    
    # Compare across models
    if len(all_results) > 1:
        print(f"\n{'='*50}")
        print("COMPARING MODELS BY FEATURE ENGINEERING APPROACH")
        print(f"{'='*50}")
        
        # Group models by feature engineering approach
        regular_models = {k: v for k, v in all_results.items() if '_diff' not in k}
        differential_models = {k: v for k, v in all_results.items() if '_diff' in k}
        
        # Compare regular models
        if len(regular_models) > 1:
            print(f"\n{'-'*30}")
            print("REGULAR MODELS COMPARISON")
            print(f"{'-'*30}")
            print(f"Models: {list(regular_models.keys())}")
            
            comparison_df, rank_pivot, importance_pivot = compare_model_features(regular_models, top_n=20, mode=mode)
            
            # Save comparison results
            suffix = f"_{mode}" if mode == "grouped" else ""
            comparison_df.to_csv(os.path.join(results_dir, f"permutation_comparison_regular{suffix}_{timestamp}.csv"), index=False)
            rank_pivot.to_csv(os.path.join(results_dir, f"feature_ranks_comparison_regular{suffix}_{timestamp}.csv"))
            importance_pivot.to_csv(os.path.join(results_dir, f"feature_importance_comparison_regular{suffix}_{timestamp}.csv"))
            
            # Analyze differences
            analysis = analyze_feature_differences(rank_pivot, importance_pivot)
            
            feature_type = "feature groups" if mode == "grouped" else "features"
            
            print(f"\n{feature_type.title()} appearing in top 20 of all regular models ({len(analysis['features_in_all_models'])}):")
            for feat in analysis['features_in_all_models'][:10]:
                print(f"  {feat}")
            
            if 'most_variable_features' in analysis and len(analysis['most_variable_features']) > 0:
                print(f"\nMost variable {feature_type} across regular models (by rank):")
                for feat, std_val in list(analysis['most_variable_features'].items())[:5]:
                    print(f"  {feat}: {std_val:.2f}")
            
            if 'most_consistent_features' in analysis and len(analysis['most_consistent_features']) > 0:
                print(f"\nMost consistent {feature_type} across regular models (by rank):")
                for feat, std_val in list(analysis['most_consistent_features'].items())[:5]:
                    print(f"  {feat}: {std_val:.2f}")
            
            # Save analysis summary
            with open(os.path.join(results_dir, f"feature_analysis_summary_regular{suffix}_{timestamp}.txt"), 'w') as f:
                f.write(f"PERMUTATION IMPORTANCE ANALYSIS SUMMARY - REGULAR MODELS ({mode.upper()} MODE)\n")
                f.write("="*50 + "\n\n")
                f.write(f"Models analyzed: {list(regular_models.keys())}\n\n")
                
                f.write(f"{feature_type.title()} in all models ({len(analysis['features_in_all_models'])}):\n")
                for feat in analysis['features_in_all_models']:
                    f.write(f"  {feat}\n")
                
                f.write(f"\n{feature_type.title()} in some models ({len(analysis['features_in_some_models'])}):\n")
                for feat in analysis['features_in_some_models']:
                    f.write(f"  {feat}\n")
                
                if 'most_variable_features' in analysis:
                    f.write(f"\nMost variable {feature_type}:\n")
                    for feat, std_val in analysis['most_variable_features'].items():
                        f.write(f"  {feat}: {std_val:.2f}\n")
                
                if 'most_consistent_features' in analysis:
                    f.write(f"\nMost consistent {feature_type}:\n")
                    for feat, std_val in analysis['most_consistent_features'].items():
                        f.write(f"  {feat}: {std_val:.2f}\n")
        
        # Compare differential models
        if len(differential_models) > 1:
            print(f"\n{'-'*30}")
            print("DIFFERENTIAL MODELS COMPARISON")
            print(f"{'-'*30}")
            print(f"Models: {list(differential_models.keys())}")
            
            comparison_df, rank_pivot, importance_pivot = compare_model_features(differential_models, top_n=20, mode=mode)
            
            # Save comparison results
            suffix = f"_{mode}" if mode == "grouped" else ""
            comparison_df.to_csv(os.path.join(results_dir, f"permutation_comparison_differential{suffix}_{timestamp}.csv"), index=False)
            rank_pivot.to_csv(os.path.join(results_dir, f"feature_ranks_comparison_differential{suffix}_{timestamp}.csv"))
            importance_pivot.to_csv(os.path.join(results_dir, f"feature_importance_comparison_differential{suffix}_{timestamp}.csv"))
            
            # Analyze differences
            analysis = analyze_feature_differences(rank_pivot, importance_pivot)
            
            feature_type = "feature groups" if mode == "grouped" else "features"
            
            print(f"\n{feature_type.title()} appearing in top 20 of all differential models ({len(analysis['features_in_all_models'])}):")
            for feat in analysis['features_in_all_models'][:10]:
                print(f"  {feat}")
            
            if 'most_variable_features' in analysis and len(analysis['most_variable_features']) > 0:
                print(f"\nMost variable {feature_type} across differential models (by rank):")
                for feat, std_val in list(analysis['most_variable_features'].items())[:5]:
                    print(f"  {feat}: {std_val:.2f}")
            
            if 'most_consistent_features' in analysis and len(analysis['most_consistent_features']) > 0:
                print(f"\nMost consistent {feature_type} across differential models (by rank):")
                for feat, std_val in list(analysis['most_consistent_features'].items())[:5]:
                    print(f"  {feat}: {std_val:.2f}")
            
            # Save analysis summary
            with open(os.path.join(results_dir, f"feature_analysis_summary_differential{suffix}_{timestamp}.txt"), 'w') as f:
                f.write(f"PERMUTATION IMPORTANCE ANALYSIS SUMMARY - DIFFERENTIAL MODELS ({mode.upper()} MODE)\n")
                f.write("="*50 + "\n\n")
                f.write(f"Models analyzed: {list(differential_models.keys())}\n\n")
                
                f.write(f"{feature_type.title()} in all models ({len(analysis['features_in_all_models'])}):\n")
                for feat in analysis['features_in_all_models']:
                    f.write(f"  {feat}\n")
                
                f.write(f"\n{feature_type.title()} in some models ({len(analysis['features_in_some_models'])}):\n")
                for feat in analysis['features_in_some_models']:
                    f.write(f"  {feat}\n")
                
                if 'most_variable_features' in analysis:
                    f.write(f"\nMost variable {feature_type}:\n")
                    for feat, std_val in analysis['most_variable_features'].items():
                        f.write(f"  {feat}: {std_val:.2f}\n")
                
                if 'most_consistent_features' in analysis:
                    f.write(f"\nMost consistent {feature_type}:\n")
                    for feat, std_val in analysis['most_consistent_features'].items():
                        f.write(f"  {feat}: {std_val:.2f}\n")
        
        print(f"\nComparison files saved to {results_dir}:")
        if len(regular_models) > 1:
            print(f"  Regular models:")
            print(f"    - permutation_comparison_regular{suffix}_{timestamp}.csv")
            print(f"    - feature_ranks_comparison_regular{suffix}_{timestamp}.csv") 
            print(f"    - feature_importance_comparison_regular{suffix}_{timestamp}.csv")
            print(f"    - feature_analysis_summary_regular{suffix}_{timestamp}.txt")
        if len(differential_models) > 1:
            print(f"  Differential models:")
            print(f"    - permutation_comparison_differential{suffix}_{timestamp}.csv")
            print(f"    - feature_ranks_comparison_differential{suffix}_{timestamp}.csv") 
            print(f"    - feature_importance_comparison_differential{suffix}_{timestamp}.csv")
            print(f"    - feature_analysis_summary_differential{suffix}_{timestamp}.txt")
    
    print(f"\n{'='*50}")
    print("PERMUTATION IMPORTANCE ANALYSIS COMPLETE")
    print(f"{'='*50}")

if __name__ == "__main__":
    main()

