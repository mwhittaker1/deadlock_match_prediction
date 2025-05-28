import requests
import os
from pathlib import Path
import time

def download_large_file(url, filename, chunk_size=1024*1024, max_retries=10):
    """Download a file with resume capability and retries"""
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            headers = {}
            
            # Ensure the directory exists
            Path(filename).parent.mkdir(parents=True, exist_ok=True)
            
            # Check if file partially exists
            if os.path.exists(filename):
                file_size = os.path.getsize(filename)
                headers['Range'] = f'bytes={file_size}-'
                mode = 'ab'
                print(f"Resuming download from {file_size} bytes")
            else:
                file_size = 0
                mode = 'wb'
            
            with requests.get(url, headers=headers, stream=True, timeout=30) as r:
                if r.status_code == 416:  # Range not satisfiable - file already complete
                    print(f"{filename} already fully downloaded")
                    return True
                
                r.raise_for_status()
                
                total_size = int(r.headers.get('content-length', 0))
                if 'Range' in headers:
                    total_size += file_size
                
                downloaded = file_size
                
                with open(filename, mode) as f:
                    for chunk in r.iter_content(chunk_size=chunk_size):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            if total_size > 0:
                                percent = (downloaded / total_size) * 100
                                print(f"\rProgress: {percent:.1f}% ({downloaded}/{total_size} bytes)", end='', flush=True)
                
                print(f"\nDownload complete: {filename}")
                return True
                
        except (requests.exceptions.ConnectionError, requests.exceptions.ChunkedEncodingError) as e:
            retry_count += 1
            print(f"\nConnection error (attempt {retry_count}/{max_retries}): {str(e)}")
            
            if retry_count < max_retries:
                wait_time = min(30, 2 ** retry_count)  # Exponential backoff, max 30 seconds
                print(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"Max retries reached for {filename}")
                return False
                
        except Exception as e:
            print(f"\nUnexpected error: {e}")
            retry_count += 1
            
            if retry_count < max_retries:
                print(f"Retrying... (attempt {retry_count}/{max_retries})")
                time.sleep(5)
            else:
                return False
    
    return False

def main():
    base_url = "https://s3-cache.deadlock-api.com/db-snapshot/public/match_metadata"
    start_file = 0
    end_file = 35
    
    # Create downloads directory on Desktop
    download_dir = os.path.join(os.path.expanduser("~"), "Desktop", "deadlock_downloads")
    os.makedirs(download_dir, exist_ok=True)
    
    print(f"Files will be downloaded to: {download_dir}")
    
    # Keep track of failed downloads
    failed_downloads = []
    
    for x in range(start_file, end_file):
        url = f"{base_url}/match_info_{x}.parquet"
        filename = os.path.join(download_dir, f"match_info_{x}.parquet")
        
        print(f"\n{'='*50}")
        print(f"Downloading match_info_{x}.parquet")
        print(f"{'='*50}")
        
        success = download_large_file(url, filename)
        
        if not success:
            failed_downloads.append(x)
    
    # Retry failed downloads
    if failed_downloads:
        print(f"\n{'='*50}")
        print(f"Retrying {len(failed_downloads)} failed downloads...")
        print(f"{'='*50}")
        
        for x in failed_downloads[:]:  # Copy the list to modify during iteration
            url = f"{base_url}/match_info_{x}.parquet"
            filename = os.path.join(download_dir, f"match_info_{x}.parquet")
            
            print(f"\nRetrying match_info_{x}.parquet")
            success = download_large_file(url, filename, max_retries=10)  # More retries for failed files
            
            if success:
                failed_downloads.remove(x)
    
    # Final report
    print(f"\n{'='*50}")
    print("Download Summary:")
    print(f"{'='*50}")
    print(f"Successfully downloaded: {end_file - start_file - len(failed_downloads)} files")
    
    if failed_downloads:
        print(f"Failed to download: {len(failed_downloads)} files")
        print(f"Failed files: {[f'match_info_{x}.parquet' for x in failed_downloads]}")
    else:
        print("All files downloaded successfully!")

if __name__ == "__main__":
    main()