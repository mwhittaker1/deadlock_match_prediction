# db.py
import duckdb

con = duckdb.connect("c:/Code/Local Code/deadlock_match_prediction/data/deadlock.db")
testcon = duckdb.connect("c:/Code/Local Code/deadlock_match_prediction/data/test_deadlock.db")

DB_PATH = "c:/Code/Local Code/deadlock_match_prediction/data/deadlock.db"
TEST_DB_PATH = "c:/Code/Local Code/deadlock_match_prediction/data/test_deadlock.db"