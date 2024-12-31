import requests

url = "http://www.viatrader.com/cbt/tickdata"
file_path = r"C:\CoralBayT\reports\total_pnl.csv"

with open(file_path, 'rb') as file:
    files = {'file': file}
    response = requests.post(url, files=files)

print("Status Code:", response.status_code)
print("Response Text:", response.text)
