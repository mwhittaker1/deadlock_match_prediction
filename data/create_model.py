import logging
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_data_issues(df):
    """Check for common missing/null/error values in the DataFrame and print summary. Also print rows with errors."""
    import numpy as np
    logging.info("--- Data Issues Summary ---")

    bol=True
    # Check for NaN values
    nan_counts = df.isna().sum()
    if nan_counts.any():
        logging.warning("NaN values found:")
        logging.warning(nan_counts[nan_counts > 0])
        bol=False
    else:
        logging.info("No NaN values found.")

    # Check for infinite values
    inf_counts = np.isinf(df.select_dtypes(include=[float, int])).sum()
    if inf_counts.any():
        logging.warning("Infinite values found:")
        logging.warning(inf_counts[inf_counts > 0])
        bol=False
    else:
        logging.info("No infinite values found.")

    # Check for string 'inf', '-inf', 'nan', 'None', or empty string
    error_strings = ['inf', '-inf', 'nan', 'None', '']
    error_rows = set()
    for col in df.select_dtypes(include=[object]).columns:
        for err in error_strings:
            mask = (df[col] == err)
            count = mask.sum()
            if count > 0:
                logging.warning(f"Column '{col}' has {count} occurrences of '{err}'")
                error_rows.update(df[mask].index.tolist())
                bol=False

    # Collect all error rows (NaN, inf, error strings)
    nan_rows = set(df[df.isna().any(axis=1)].index.tolist())
    inf_rows = set(df[np.isinf(df.select_dtypes(include=[float, int])).any(axis=1)].index.tolist())
    all_error_rows = nan_rows.union(inf_rows).union(error_rows)

    logging.info("--- End of Data Issues Summary ---")
    if all_error_rows:
        logging.info(f"\nDetailed view of rows with data errors ({len(all_error_rows)} rows) saved to .csv:")
        df.loc[sorted(all_error_rows)].to_csv(f"{df}_data_errors.csv", index=False)
        bol=False
    else:
        logging.info("No rows with data errors found.")

    return bol

def prep_training_data(training_data, test_data):
    """ Split training data into features and targets"""
    y = training_data['team_0_win']
    X = training_data.drop(columns=['team_0_win','match_id']).copy()

    if test_data:
        y_test = test_data['team_0_win']
        X_test = test_data.drop(columns=['team_0_win','match_id']).copy()
        return X, X_test, y, y_test
    else:
        # Split  data
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=42)

    return X_train, X_test, y_train, y_test

def train_random_forest(X_train, X_test, y_train, y_test, params, model_type):

    # Create unique model identifier
    ## {model_type} {date_time} {random_state}
    from datetime import datetime

    date_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    model_id = f"{model_type}_{date_time}_{params.get('random_state', 42)}"

    # Initialize the model with these starting parameters
    rf_model = RandomForestClassifier(
        n_estimators=params.get("n_estimators", 100),
        max_depth=params.get("max_depth", None),
        min_samples_split=params.get("min_samples_split", 2),
        min_samples_leaf=params.get("min_samples_leaf", 1),
        max_features=params.get("max_features", "sqrt"),
        random_state=params.get("random_state", 42),
        n_jobs=-1                # Use all available cores
    )
    
    # Print Model params and start
    print(f"Starting Training, Modelid = {model_id}")
    print("Training Random Forest with parameters:")
    for key, value in params.items():
        print(f"{key}: {value}")

    # Train the model
    rf_model.fit(X_train, y_train)

    # Make predictions
    y_pred = rf_model.predict(X_test)


    return rf_model, model_id, y_pred

def evaluate_model(model, y_test, y_pred, X)-> dict:
    """Create a report evaluating model passed, returns dict of report data"""

    report = classification_report(y_test, y_pred, output_dict=True)
    conf_matrix = confusion_matrix(y_test, y_pred).tolist()
    feature_importance = pd.DataFrame(
        {'feature': X.columns, 'importance': model.feature_importances_},
    ).sort_values('importance', ascending=False)

    # Evaluate the model
    accuracy = accuracy_score(y_test, y_pred)
    print(f"Accuracy: {accuracy:.4f}")
    print("\nClassification Report:")
    print(classification_report(y_test, y_pred))
    print("\nConfusion Matrix:")
    print(confusion_matrix(y_test, y_pred))

    print("\nTop 25 important features:")
    print(feature_importance.head(25))

    return {
        'accuracy': accuracy,
        'classification_report': report,
        'confusion_matrix': conf_matrix,
        'feature_importance': feature_importance.to_dict(orient='records')
    }

def save_model(model, params, folder, model_id, feature_names,X_train,X_test,y_train,y_test):
    from sklearn.pipeline import Pipeline
    import joblib, json, platform, os

    # create subfolders
    os.makedirs(f"{folder}/samples", exist_ok=True)

    joblib.dump(model, f"{folder}/model.joblib")

    print(f"Model saved to {folder}")

    json.dump(params, open(f"{folder}/params.json","w"))

    json.dump({
    "run_id": model_id, 
    "python": platform.python_version(),
    "feature_names": list(feature_names),
    "random_state": params.get("random_state", 42),
    }, open(f"{folder}/meta.json","w"))

    train = X_train.copy()
    train["target"] = y_train
    train.to_csv(f"{folder}/samples/Xy_train.csv", index=False)

    test = X_test.copy()
    test["target"] = y_test
    test.to_csv(f"{folder}/samples/Xy_test.csv", index=False)

def save_report(training_data, model_id, folder, results):
    import json

    training_data.to_csv(f"{folder}/{model_id}_training_data.csv")

    with open(f"{folder}/{model_id}_results.txt", "w") as f:
        f.write(f"Accuracy: {results['accuracy']}\n\n")
        f.write("Classification Report:\n")
        f.write(json.dumps(results['classification_report'], indent=2))
        f.write("\n\nConfusion Matrix:\n")
        f.write(str(results['confusion_matrix']))
        f.write("\n\nTop Features:\n")
        for feat in results['feature_importance'][:25]:
            f.write(f"{feat['feature']}: {feat['importance']}\n")


if __name__ == "__main__":
    
    # name model for ID
    model_type = "rf_std_v2"

    #### MAKE SURE TO CHANGE THE FOLDER!!!!! #####
    model_folder = f"models//8.24.25//{model_type}"
    
    start_date = "2025-08-19"
    end_date = "2025-08-21"
    folder_name = f"v2_data//pred_data//test_pred_v2_{start_date}_{end_date}//training"
    
    params = {
        "n_estimators": 100,
        "max_depth": None,
        "min_samples_split": 2,
        "min_samples_leaf": 1,
        "max_features": "sqrt",
        "random_state": 42
    }

    # Set path for .csv where training data is stored.
    training_path = f"{folder_name}//training_data.csv"
    diff_training_path = f"{folder_name}//differential_training_data.csv"

    # Load training data
    training_data = pd.read_csv(training_path)
    diff_training_data = pd.read_csv(diff_training_path)

    check_data_issues(training_data)
    check_data_issues(diff_training_data)

    X_train, X_test, y_train, y_test = prep_training_data(training_data)

    model, model_id, y_pred = train_random_forest(X_train, X_test, y_train, y_test, params, model_type)

    X = X_train.columns

    report = evaluate_model(model, y_test, y_pred,X)

    
    features = X.tolist()

    save_model(model, params, model_folder, model_id, features,X_train,X_test,y_train,y_test)
    save_report(training_data, model_id, model_folder, report)

    train = X_train.copy()
    train["target"] = y_train
    train.to_csv(f"{model_folder}/samples/Xy_train.csv", index=False)

    test = X_test.copy()
    test["target"] = y_test
    test.to_csv(f"{model_folder}/samples/Xy_test.csv", index=False)
