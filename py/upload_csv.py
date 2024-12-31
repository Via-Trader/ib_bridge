import os
import requests

file1_path = r"C:\CoralBayT\reports\total_pnl.csv"
file2_path = r"C:\CoralBayT\reports\trades.csv"

# Define the target URL
upload_url = "http://via-trader.com/cbt/"  # Replace with your website's upload endpoint

# Function to upload a file
def upload_file(file_path):
    try:
        with open(file_path, 'rb') as file:
            files = {'file': (os.path.basename(file_path), file)}
            response = requests.post(upload_url, files=files)
            
            if response.status_code == 200:
                print(f"Uploaded {os.path.basename(file_path)} successfully!")
            else:
                print(f"Failed to upload {os.path.basename(file_path)}. Status: {response.status_code}")
                print(response.text)
    except Exception as e:
        print(f"Error uploading {file_path}: {e}")

# Upload the files
upload_file(file1_path)
upload_file(file2_path)
