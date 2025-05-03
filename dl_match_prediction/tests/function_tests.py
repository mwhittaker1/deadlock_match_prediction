import requests
import json
import pandas as pd
import services.utility_functions as u
import services.fetch_data as fd


class DummyResponse:
    def __init__(self, data):
        self._data = data
    def json(self):
        return self._data
    

def test_match_fetch_data():
    # Mock the requests.get method to return a dummy response
    def mock_get(url, params=None):
        if "metadata" in url:
            return DummyResponse({"match_id": 12345, "players": []})
        return DummyResponse([])

    # Replace the requests.get with the mock_get function
    requests.get = mock_get

    # Call the function with a specific match ID
    result = fd.fetch_match_data(m_id="12345")
    
    # Check that the result is as expected
    assert result == {"match_id": 12345, "players": []}, f"Expected match data for ID 12345, got {result}"

if __name__ == "__main__":
    test_match_fetch_data()
    print("Test passed!")