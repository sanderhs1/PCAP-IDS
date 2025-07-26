import pandas as pd
from glob import glob
import os

def clean_and_save_each(file_pattern, output_dir):
    files = glob(file_pattern)
    for file in files:
        print(f"Processing {file}")
        df = pd.read_csv(file)
        
        df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]

        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], dayfirst=True, errors='coerce')
            df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        numeric_cols = [
            'flow_bytes_per_s', 'flow_packets_per_s',
            'total_length_of_fwd_packets', 'total_length_of_bwd_packets'
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        for ip_col in ['source_ip', 'destination_ip']:
            if ip_col in df.columns:
                df[ip_col] = df[ip_col].str.strip()
        
        df = df.drop_duplicates()
        df = df.dropna(subset=['timestamp', 'source_ip', 'destination_ip'])
        
        base_name = os.path.basename(file)  
        cleaned_name = os.path.splitext(base_name)[0] + "_cleaned.csv"  
        output_path = os.path.join(output_dir, cleaned_name)
        
        df.to_csv(output_path, index=False)
        print(f"Saved cleaned data to {output_path}")


clean_and_save_each(r"")