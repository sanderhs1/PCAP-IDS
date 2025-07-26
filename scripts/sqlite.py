import sqlite3
import pandas as pd

csv_files = {
    "Monday": r"data\cleaned\Monday-WorkingHours.pcap_ISCX_cleaned.csv",
    "Tuesday": r"data\cleaned\Tuesday-WorkingHours.pcap_ISCX_cleaned.csv",
}

db_path = "network_flows.db"
table_name = "network_flows"


columns = """
    flow_id TEXT PRIMARY KEY,
    source_ip TEXT,
    source_port INTEGER,
    destination_ip TEXT,
    destination_port INTEGER,
    protocol INTEGER,
    timestamp TEXT,
    flow_duration INTEGER,
    total_fwd_packets INTEGER,
    total_backward_packets INTEGER,
    total_length_of_fwd_packets REAL,
    total_length_of_bwd_packets REAL,
    fwd_packet_length_max REAL,
    fwd_packet_length_min REAL,
    fwd_packet_length_mean REAL,
    fwd_packet_length_std REAL,
    bwd_packet_length_max REAL,
    bwd_packet_length_min REAL,
    bwd_packet_length_mean REAL,
    bwd_packet_length_std REAL,
    flow_bytes_per_s REAL,
    flow_packets_per_s REAL,
    flow_iat_mean REAL,
    flow_iat_std REAL,
    flow_iat_max REAL,
    flow_iat_min REAL,
    fwd_iat_total REAL,
    fwd_iat_mean REAL,
    fwd_iat_std REAL,
    fwd_iat_max REAL,
    fwd_iat_min REAL,
    bwd_iat_total REAL,
    bwd_iat_mean REAL,
    bwd_iat_std REAL,
    bwd_iat_max REAL,
    bwd_iat_min REAL,
    fwd_psh_flags INTEGER,
    bwd_psh_flags INTEGER,
    fwd_urg_flags INTEGER,
    bwd_urg_flags INTEGER,
    fwd_header_length INTEGER,
    bwd_header_length INTEGER,
    fwd_packets_per_s REAL,
    bwd_packets_per_s REAL,
    min_packet_length REAL,
    max_packet_length REAL,
    packet_length_mean REAL,
    packet_length_std REAL,
    packet_length_variance REAL,
    fin_flag_count INTEGER,
    syn_flag_count INTEGER,
    rst_flag_count INTEGER,
    psh_flag_count INTEGER,
    ack_flag_count INTEGER,
    urg_flag_count INTEGER,
    cwe_flag_count INTEGER,
    ece_flag_count INTEGER,
    down_up_ratio REAL,
    average_packet_size REAL,
    avg_fwd_segment_size REAL,
    avg_bwd_segment_size REAL,
    fwd_header_length_1 INTEGER,
    fwd_avg_bytes_bulk REAL,
    fwd_avg_packets_bulk REAL,
    fwd_avg_bulk_rate REAL,
    bwd_avg_bytes_bulk REAL,
    bwd_avg_packets_bulk REAL,
    bwd_avg_bulk_rate REAL,
    subflow_fwd_packets INTEGER,
    subflow_fwd_bytes INTEGER,
    subflow_bwd_packets INTEGER,
    subflow_bwd_bytes INTEGER,
    init_win_bytes_forward INTEGER,
    init_win_bytes_backward INTEGER,
    act_data_pkt_fwd INTEGER,
    min_seg_size_forward INTEGER,
    active_mean REAL,
    active_std REAL,
    active_max REAL,
    active_min REAL,
    idle_mean REAL,
    idle_std REAL,
    idle_max REAL,
    idle_min REAL,
    label TEXT,
    day TEXT
"""

def create_table(conn):
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
    cursor.execute(f"CREATE TABLE {table_name} ({columns});")
    conn.commit()

def sanitize_columns(df):
    # Replace problematic chars in column names
    df.columns = [col.replace('/', '_').replace('-', '_').replace('.', '_') for col in df.columns]
    return df

def load_csv_to_db(conn, csv_path, table_name):
    print(f"Loading {table_name} data from {csv_path} ...")
    df = pd.read_csv(csv_path)
    df = sanitize_columns(df)  # sanitize columns before loading
    df.to_sql(table_name, conn, if_exists='append', index=False)
    print(f"Loaded {len(df)} records into {table_name} table.")

def main():
    conn = sqlite3.connect(db_path)
    create_table(conn)
    for day, path in csv_files.items():
        print(f"Loading {day} data from {path} ...")
        load_csv_to_db(conn, path, day)
    print("Done loading data.")
    conn.close()

if __name__ == "__main__":
    main()