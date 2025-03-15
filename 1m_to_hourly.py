import numpy as np
import pandas as pd

df = pd.read_pickle("SPY_1min")



# Ensure 't' column is datetime
df['t'] = pd.to_datetime(df['t'], unit='ms')

# Set timezone to New York
df['t'] = df['t'].dt.tz_localize('UTC').dt.tz_convert('America/New_York')

# Initialize lists to hold aggregated values
datetimes = []
opens = []
highs = []
lows = []
closes = []
volumes = []

# Create hourly bins starting at 09:30
start_time = df['t'].min().replace(hour=9, minute=30)
end_time = df['t'].max()

# Loop through each hour
count = 0
while start_time < end_time:
    end_time_bin = start_time + pd.Timedelta(hours=1)
    count += 1
    if count %100 == 0:
        print(start_time)
    
    # Filter data for the current hour
    bin_data = df[(df['t'] >= start_time) & (df['t'] < end_time_bin)]
    
    if not bin_data.empty:
        datetimes.append(start_time)
        opens.append(bin_data['o'].iloc[0])
        highs.append(bin_data['h'].max())
        lows.append(bin_data['l'].min())
        closes.append(bin_data['c'].iloc[-1])
        volumes.append(bin_data['v'].sum())
    else:
        # Skip this hour if there's no data
        start_time += pd.Timedelta(hours=1)
        continue
    
    start_time += pd.Timedelta(hours=1)

# Create DataFrame with aggregated data
df_hourly = pd.DataFrame({
    'Datetime': datetimes,
    'Open': opens,
    'High': highs,
    'Low': lows,
    'Close': closes,
    'Volume': volumes,
    'IsGap': False  # Assuming no gaps for simplicity
})

# Save DataFrame to CSV
df_hourly.to_csv('hourly_candle_bars.csv', index=False)

