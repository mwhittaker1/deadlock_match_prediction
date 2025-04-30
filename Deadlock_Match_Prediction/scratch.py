import requests

url = "https://s3-cache.deadlock-api.com/db-snapshot/public/match_player.parquet"
output_file = "player_match_history.parquet"

# Stream download
with requests.get(url, stream=True, timeout=60) as r:
    r.raise_for_status()
    with open(output_file, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024*1024):  # 1MB chunks
            if chunk:  # filter out keep-alive chunks
                f.write(chunk)

print("Download completed successfully.")
