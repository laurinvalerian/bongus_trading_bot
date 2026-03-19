import pandas as pd
import io, requests, zipfile, os
from get_binance_data import get_months, process_funding, URLS

START_MONTH = "2023-01"
END_MONTH = "2024-12"
DATA_DIR = "data"

def main():
    months = get_months(START_MONTH, END_MONTH)
    funding_dfs = []
    
    for month in months:
        print(f"Downloading Funding Rates for {month}...")
        funding_url = URLS["funding"].format(month)
        
        response = requests.get(funding_url)
        if response.status_code == 200:
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                csv_filename = z.namelist()[0]
                with z.open(csv_filename) as f:
                    raw_df = pd.read_csv(f, header=None)
                    funding_dfs.append(process_funding(raw_df))
        else:
            print(f"  [!] Failed to download: {funding_url} (Status: {response.status_code})")
            
    print("\nSaving to Parquet...")
    if funding_dfs:
        funding_df = pd.concat(funding_dfs, ignore_index=True)
        funding_df = funding_df.drop_duplicates(subset=['timestamp']).sort_values('timestamp')
        funding_df.to_parquet(os.path.join(DATA_DIR, "funding_rates.parquet"), index=False)
        print(f"✅ Saved funding_rates.parquet ({len(funding_df)} rows)")

if __name__ == "__main__":
    main()
