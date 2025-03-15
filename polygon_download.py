import requests
import pandas as pd
import datetime
import pytz

pd.set_option('display.max_rows', None)

API_KEY = 'APIKEY'
BASE_URL = 'https://api.polygon.io'

def get_histdata_polygon(ticker, start_date, end_date, timespan, multiplier):
    url = f"{BASE_URL}/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{start_date}/{end_date}?apiKey={API_KEY}"
    response = requests.get(url)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to retrieve data. Status code: {response.status_code}")
        return None

# Define parameters
ticker = 'SPY'
start_date = '2003-09-10'
#end_date = '2004-03-10'
end_date = '2025-03-15'
timespan = 'minute'
multiplier = '1'

# Convert end_date to datetime object for easier manipulation
end_date_dt = datetime.datetime.strptime(end_date, '%Y-%m-%d')

# Initialize start_date as a datetime object
start_date_dt = datetime.datetime.strptime(start_date, '%Y-%m-%d')

all_data = []

while start_date_dt <= end_date_dt:
    # Calculate end_date for this batch, ensuring it doesn't exceed the end_date
    batch_end_date_dt = start_date_dt + datetime.timedelta(days=10)  # Fetch data in monthly batches
    
    # Ensure batch_end_date doesn't exceed end_date
    if batch_end_date_dt > end_date_dt:
        batch_end_date_dt = end_date_dt
    
    # Convert batch dates back to strings for API request
    batch_start_date = start_date_dt.strftime('%Y-%m-%d')
    batch_end_date = batch_end_date_dt.strftime('%Y-%m-%d')

    print(batch_start_date," -> ",batch_end_date)
    
    # Fetch data for this batch
    data = get_histdata_polygon(ticker, batch_start_date, batch_end_date, timespan, multiplier)
    
    if data and 'results' in data:
        # Filter out after-hours data and convert timezone
        utc = pytz.UTC
        et = pytz.timezone('US/Eastern')
        
        market_hours_data = []
        for entry in data['results']:
            dt = datetime.datetime.fromtimestamp(entry['t'] / 1000, utc)
            dt_et = dt.astimezone(et)
            
#            if (dt_et.hour == 9 and dt_et.minute >= 30) or (dt_et.hour > 9 and dt_et.hour < 16):
            if 1 == 1:
                market_hours_data.append({
                    't': entry['t'],
                    'v': entry['v'],
                    'vw': entry['vw'],
                    'o': entry['o'],
                    'c': entry['c'],
                    'h': entry['h'],
                    'l': entry['l'],
                    'human_readable_timestamp': dt_et.strftime('%Y-%m-%d %H:%M:%S')
                })
        
        all_data.extend(market_hours_data)
    else:
        print(f"No results found for {batch_start_date} to {batch_end_date}.")
    
    # Move to the next batch
    start_date_dt += datetime.timedelta(days=10)  # Move forward by one month

# Convert to DataFrame
if all_data:
    df = pd.DataFrame(all_data)
    print(df)
else:
    print("No data retrieved.")

df.to_pickle("data/SPY_1min")
