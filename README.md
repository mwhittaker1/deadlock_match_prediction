# Deadlock Match Prediction

This repository contains a machine learning pipeline to predict match outcomes in the game **Deadlock** based on hero compositions. It leverages a Random Forest classifier and has achieved up to **72% accuracy on high-ranked matches**.

## Table of Contents

* [Data Pipeline](#data-pipeline)
* [Model Training](#model-training)
* [Evaluation](#evaluation)
* [Usage](#usage)
* [Project Structure](#project-structure)

---

## Data Pipeline

1. **Fetch raw data**
   Scripts in `services/` pull match, player, and hero data from the Deadlock API and store it in a DuckDB file.
2. **Preprocess & feature engineering**
   Transforms raw tables into a flat training set (e.g. `data/final_training_data.csv`).
3. **Staging vs. live**
   Toggle between using a staged CSV, calling API directly, or querying the DuckDB directly via flags in `main.py`.

---

## FastAI Model Training

Run the main training script:

```bash
python main.py \
  --use_staged_csv False \
  --staged_csv_path data/final_training_data.csv \
  --duckdb_path data/match_player_raw.duckdb \
  --duckdb_table training_set
```

Configuration parameters at the top of `main.py`:

```python
USE_STAGED_CSV   = False
STAGED_CSV_PATH  = "data/final_training_data.csv"
DUCKDB_PATH      = "data/match_player_raw.duckdb"
DUCKDB_TABLE     = "training_set"
```

---

## Evaluation

After training, a classification report is printed showing precision, recall, F1-score, and overall accuracy. The trained model artifact is saved to:

```
models/random_forest.joblib
```

---

## Usage

Example of loading the saved model and making a prediction:

```python
from joblib import load
import pandas as pd

# Load model
clf = load("models/random_forest.joblib")

# Prepare feature vector (example)
features = pd.DataFrame([{
    "hero1_id": 12,
    "hero2_id": 27,
    # ... other engineered features ...
}])

# Predict probability of first team winning
prob = clf.predict_proba(features)[:, 1]
print(f"Win probability: {prob[0]:.2%}")
```

---

## Project Structure

```text
deadlock_match_prediction/
├── data/                       # Raw & processed datasets
├── models/                     # Saved model artifacts
├── services/                   # API clients & preprocessing scripts
├── function_tests.py           # Unit tests for core functions
├── main.py                     # Training & evaluation entrypoint
├── requirements.txt            # Python dependencies
└── .gitignore
```

