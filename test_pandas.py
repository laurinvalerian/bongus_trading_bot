import pandas as pd
import io, requests, zipfile

url = 'https://data.binance.vision/data/futures/um/monthly/fundingRate/BTCUSDT/BTCUSDT-fundingRate-2023-01.zip'
response = requests.get(url)
with zipfile.ZipFile(io.BytesIO(response.content)) as z:
    csv_filename = z.namelist()[0]
    with z.open(csv_filename) as f:
        raw_df = pd.read_csv(f, header=None)

if raw_df.iloc[0, 0] == 'calc_time':
    raw_df = raw_df[1:].reset_index(drop=True)

raw_df = raw_df.iloc[:, :3] # Take only first 3 columns
raw_df.columns = ['calc_time', 'funding_interval_hours', 'funding_rate']
print("before string cast:", raw_df.head())
raw_df['calc_time'] = pd.to_numeric(raw_df['calc_time'])
raw_df['timestamp'] = pd.to_datetime(raw_df['calc_time'], unit='ms')
raw_df['funding_rate'] = pd.to_numeric(raw_df['funding_rate'])
df = raw_df[['timestamp', 'funding_rate']]
print(df.head())