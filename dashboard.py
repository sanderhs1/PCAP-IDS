import streamlit as st
import sqlite3
import pandas as pd

def query_traffic_by_hour(db_path='network_flows.db', table_name='Monday'):
    conn = sqlite3.connect(db_path)
    query = f"""
    SELECT strftime('%Y-%m-%d %H:00', timestamp) AS hour, COUNT(*) AS flow_count
    FROM {table_name}
    GROUP BY hour
    ORDER BY hour;
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def main():
    st.title("CIC-IDS2017 Network Traffic Dashboard")

    st.sidebar.header("Settings")
    table_name = st.sidebar.selectbox("Select table", ['Monday', 'Tuesday'])
    
    st.write(f"### Traffic volume per hour from table: {table_name}")
    df = query_traffic_by_hour(table_name=table_name)
    
    if df.empty:
        st.write("No data found.")
    else:
        df['hour'] = pd.to_datetime(df['hour'])
        df = df.set_index('hour')
        st.line_chart(df['flow_count'])

if __name__ == "__main__":
    main()