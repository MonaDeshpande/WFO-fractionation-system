import psycopg2
import sys
import time
import csv
import pandas as pd
from datetime import datetime, timedelta

# ==============================================================================
# CONFIGURATION
# ==============================================================================
PG_HOST = "localhost"
PG_PORT = "5432"
PG_USER = "postgres"
PG_PASSWORD = "ADMIN"  # <-- IMPORTANT: Add your PostgreSQL password here
PG_DB_NAME = "scada_data_analysis"
PG_RAW_TABLE = "wide_scada_data"  # Updated raw data table name
PG_CLEANED_TABLE = "scada_data_cleaned_report"  # Updated cleaned data table name
TAGS_CSV_FILE = "TAG_INDEX_FINAL.csv"

# --- USER INPUT ---
# Placeholder: Change this to your desired start and end date for a single run.
START_DATE = "2025-08-08 00:00:00"
END_DATE = "2025-08-15 00:00:00"

# Faulty value constant
FAULTY_VALUE = 32767
# Thresholds for anomaly detection
SPIKE_THRESHOLD_C = 20
FLOW_ANOMALY_THRESHOLD = 0.5

# Distillation column tag sequences for temperature and pressure profiles
DISTILLATION_COLUMNS = {
    "Column_1": {
        "temperature": ["TI61", "TI62", "TI63", "TI64", "TI65"],
        "pressure": ["PI61", "PI62"]
    },
    "Column_2": {
        "temperature": ["TI03", "TI04", "TI05", "TI06"],
        "pressure": ["PI03", "PI04"]
    },
    "Column_3": {
        "temperature": ["TI13", "TI14", "TI15", "TI16", "TI17", "TI18", "TI19", "TI20", "TI21", "TI22", "TI23", "TI24"],
        "pressure": ["PI13", "PI24"]
    },
    "Column_4": {
        "temperature": ["TI31", "TI32", "TI33", "TI34", "TI35", "TI36", "TI37", "TI38", "TI39"],
        "pressure": ["PI31", "PI39"]
    }
}

# Tag types for multi-sensor logic
TAG_TYPES = {
    "TI": ["TI61", "TI62", "TI63", "TI64", "TI65", "TI03", "TI04", "TI05", "TI06", "TI13", "TI14", "TI15", "TI16", "TI17", "TI18", "TI19", "TI20", "TI21", "TI22", "TI23", "TI24", "TI31", "TI32", "TI33", "TI34", "TI35", "TI36", "TI37", "TI38", "TI39", "TI215"],
    "PI": ["PI61", "PI62", "PI03", "PI04", "PI13", "PI24", "PI31", "PI39"],
    "LI": ["LI61", "LI62", "LI03", "LI04"],
    "FI": ["FI08", "FI09", "FI10"]
}


def create_cleaned_table(pg_cursor, columns_from_raw_data):
    """Creates the cleaned data table dynamically based on raw table columns."""
    print("--- Verifying database table for cleaned data ---")
    
    # Exclude DateAndTime from the dynamic columns as it's the primary key
    data_columns = [f'"{col}" DOUBLE PRECISION' for col in columns_from_raw_data if col != "DateAndTime"]
    
    flag_columns = [
        '"is_faulty_sensor" BOOLEAN DEFAULT FALSE',
        '"is_temp_anomaly" BOOLEAN DEFAULT FALSE',
        '"is_pressure_anomaly" BOOLEAN DEFAULT FALSE',
        '"is_process_excursion" BOOLEAN DEFAULT FALSE',
        '"is_flow_level_anomaly" BOOLEAN DEFAULT FALSE',
        '"is_stuck_sensor" BOOLEAN DEFAULT FALSE',
        '"is_plant_shutdown" BOOLEAN DEFAULT FALSE',
        '"is_boiler_anomaly" BOOLEAN DEFAULT FALSE',
        '"imputed_with" VARCHAR(50) DEFAULT NULL'
    ]
    
    create_cleaned_table_query = f"""
    CREATE TABLE IF NOT EXISTS "{PG_CLEANED_TABLE}" (
        "DateAndTime" TIMESTAMP PRIMARY KEY,
        {", ".join(data_columns)},
        {", ".join(flag_columns)}
    );
    """
    pg_cursor.execute(create_cleaned_table_query)
    pg_cursor.connection.commit()
    print(f"✅ Table verified/created: {PG_CLEANED_TABLE}")

def generate_excel_report(start_timestamp, end_timestamp):
    """
    Connects to the database, fetches cleaned data for a specific range, and generates an Excel report.
    """
    print("\n--- Generating Excel Report ---")
    pg_conn = None
    try:
        pg_conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD, dbname=PG_DB_NAME)
        
        query = f"""
        SELECT * FROM "{PG_CLEANED_TABLE}"
        WHERE "DateAndTime" BETWEEN %s AND %s
        ORDER BY "DateAndTime";
        """
        
        df = pd.read_sql_query(query, pg_conn, params=(start_timestamp, end_timestamp))
        
        timestamp_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        report_filename = f"SCADA_Cleaned_Data_Report_{timestamp_str}.xlsx"
        
        df.to_excel(report_filename, index=False)
        
        print(f"📊 Report successfully generated at: {report_filename}")
        print(f"   Rows exported: {len(df)}")

    except Exception as e:
        print(f"❌ An error occurred during Excel report generation: {e}", file=sys.stderr)
    finally:
        if pg_conn:
            pg_conn.close()

def process_scada_data_in_range(start_timestamp, end_timestamp):
    """
    Processes raw SCADA data from a wide table, applies cleaning logic, and inserts into a new table.
    """
    pg_conn = None
    try:
        print(f"\n--- Connecting to PostgreSQL and processing data from {start_timestamp} to {end_timestamp} ---")
        pg_conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD, dbname=PG_DB_NAME)
        pg_cursor = pg_conn.cursor()
        print("✅ Successfully connected to PostgreSQL.")

        # Fetch all data within the range from the wide table
        fetch_raw_data_query = f"""
        SELECT * FROM "{PG_RAW_TABLE}"
        WHERE "DateAndTime" BETWEEN %s AND %s
        ORDER BY "DateAndTime" ASC;
        """
        pg_cursor.execute(fetch_raw_data_query, (start_timestamp, end_timestamp))
        
        # Get column names from the cursor description
        columns = [desc[0] for desc in pg_cursor.description]
        raw_data = pg_cursor.fetchall()
        
        if not raw_data:
            print("No data found in the specified range. Exiting.")
            return

        create_cleaned_table(pg_cursor, columns)
        
        cleaned_data_to_insert = []
        for row in raw_data:
            minute_data = dict(zip(columns, row))
            
            flags = {
                "is_faulty_sensor": False,
                "is_temp_anomaly": False,
                "is_pressure_anomaly": False,
                "is_process_excursion": False,
                "is_flow_level_anomaly": False,
                "is_stuck_sensor": False,
                "is_plant_shutdown": False,
                "is_boiler_anomaly": False,
                "imputed_with": None
            }

            # Apply cleaning logic
            for tag_name, value in minute_data.items():
                if isinstance(value, (int, float)):
                    if value == FAULTY_VALUE:
                        flags["is_faulty_sensor"] = True
                    
                    if tag_name in TAG_TYPES.get("TI", []):
                        if value > 500 and not flags["is_faulty_sensor"]:
                            flags["is_process_excursion"] = True

            for col_name, col_data in DISTILLATION_COLUMNS.items():
                temp_tags = col_data.get("temperature", [])
                for i in range(len(temp_tags) - 1):
                    current_temp = minute_data.get(temp_tags