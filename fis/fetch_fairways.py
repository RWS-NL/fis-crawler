
import requests
import pandas as pd
import pathlib
import json

def fetch_geotype(geotype, generation, output_dir):
    base_url = "https://www.vaarweginformatie.nl/wfswms/dataservice/1.4"
    url = f"{base_url}/{generation}/{geotype}"
    
    records = []
    offset = 0
    while True:
        resp = requests.get(f"{url}?offset={offset}")
        if resp.status_code != 200:
            print(f"Error fetching {geotype}: {resp.status_code}")
            break
            
        data = resp.json()
        result = data.get("Result", [])
        if not result:
            break
            
        records.extend(result)
        
        count = data.get("Count", 0)
        total = data.get("TotalCount", 0)
        offset = data.get("Offset", 0)
        
        print(f"Fetched {len(result)} records for {geotype} (Total: {len(records)}/{total})")
        
        if offset + count >= total:
            break
        offset += count

    if records:
        df = pd.DataFrame(records)
        output_path = output_dir / f"{geotype}.jsonl"
        df.to_json(output_path, orient="records", lines=True)
        print(f"Saved {len(records)} records to {output_path}")
        
        # Convert to parquet
        df.to_parquet(output_dir / f"{geotype}.parquet")
        print(f"Saved parquet to {output_dir}/{geotype}.parquet")

def main():
    output_dir = pathlib.Path("fis-export")
    output_dir.mkdir(exist_ok=True)
    
    # Get generation
    gen_resp = requests.get("https://www.vaarweginformatie.nl/wfswms/dataservice/1.4/geogeneration")
    generation = gen_resp.json()["GeoGeneration"]
    print(f"Current GeoGeneration: {generation}")
    
    for gt in ["fairway", "route", "fairwaysection"]: # Added fairwaysection just in case
        print(f"Fetching {gt}...")
        try:
            fetch_geotype(gt, generation, output_dir)
        except Exception as e:
            print(f"Failed to fetch {gt}: {e}")

if __name__ == "__main__":
    main()
