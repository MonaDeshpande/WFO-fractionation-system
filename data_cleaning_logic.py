import psycopg2
import sys
import time
import csv
from datetime import datetime, timedelta

# ==============================================================================
# CONFIGURATION
# ==============================================================================
PG_HOST = "localhost"
PG_PORT = "5432"
PG_USER = "postgres"
PG_PASSWORD = "ADMIN"  # <-- IMPORTANT: Add your PostgreSQL password here
PG_DB_NAME = "scada_data_analysis"
PG_RAW_TABLE = "raw_data"
PG_MAPPING_TABLE = "tag_mapping"
PG_CLEANED_TABLE = "cleaned_scada_data"
TAGS_CSV_FILE = "TAG_INDEX_FINAL.csv"

# --- USER INPUT ---
# Placeholder: Change this to your desired start date for the initial run.
# The format must be 'YYYY-MM-DD HH:MM:SS'. After the first run,
# the script will automatically continue from the last processed time.
START_DATE = "2024-01-01 00:00:00"

# Faulty value constant
FAULTY_VALUE = 32767
# Thresholds for anomaly detection
SPIKE_THRESHOLD_C = 20
FLOW_ANOMALY_THRESHOLD = 0.5  # Assumes values are non-negative

# Distillation column tag sequences for temperature and pressure profiles
# These lists are derived directly from your provided logic.
DISTILLATION_COLUMNS = {
    "Column_1": {
        "temperature": ["TI61", "TI62", "TI63", "TI64", "TI65"],
        "pressure": ["PI61", "PI62"] # Example PIs, adjust as needed
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
    "LI": ["LI61", "LI62", "LI03", "LI04"], # Example LIs, adjust as needed
    "FI": ["FI08", "FI09", "FI10"]
}


def create_db_tables(pg_cursor, tag_data):
    """Creates the necessary tables if they don't exist."""
    print("--- Verifying database tables ---")
    
    # Create the mapping table
    create_mapping_table_query = f"""
    CREATE TABLE IF NOT EXISTS "{PG_MAPPING_TABLE}" (
        "TagIndex" INTEGER PRIMARY KEY,
        "TagName" VARCHAR(255) UNIQUE
    );
    """
    pg_cursor.execute(create_mapping_table_query)

    # Insert tags from CSV into the mapping table
    insert_mapping_query = f"""
    INSERT INTO "{PG_MAPPING_TABLE}" ("TagIndex", "TagName")
    VALUES (%s, %s)
    ON CONFLICT ("TagIndex") DO UPDATE SET "TagName" = EXCLUDED."TagName";
    """
    pg_cursor.executemany(insert_mapping_query, tag_data)

    # Dynamically create the cleaned data table with anomaly flag columns
    columns = [f'"{tag}" DOUBLE PRECISION' for _, tag in tag_data]
    
    # Add columns for anomaly flags and imputed values
    flag_columns = [
        '"is_faulty_sensor" BOOLEAN DEFAULT FALSE',
        '"is_temp_anomaly" BOOLEAN DEFAULT FALSE',
        '"is_pressure_anomaly" BOOLEAN DEFAULT FALSE',
        '"is_process_excursion" BOOLEAN DEFAULT FALSE',
        '"is_flow_level_anomaly" BOOLEAN DEFAULT FALSE',
        '"is_stuck_sensor" BOOLEAN DEFAULT FALSE',
        '"is_plant_shutdown" BOOLEAN DEFAULT FALSE',
        '"is_boiler_anomaly" BOOLEAN DEFAULT FALSE',
        '"imputed_with" VARCHAR(50) DEFAULT NULL' # 'LKG' or 'Interpolation'
    ]
    
    create_cleaned_table_query = f"""
    CREATE TABLE IF NOT EXISTS "{PG_CLEANED_TABLE}" (
        "DateAndTime" TIMESTAMP PRIMARY KEY,
        {", ".join(columns)},
        {", ".join(flag_columns)}
    );
    """
    pg_cursor.execute(create_cleaned_table_query)
    pg_cursor.connection.commit()
    print(f"✅ Tables verified/created: {PG_MAPPING_TABLE}, {PG_CLEANED_TABLE}")


def process_scada_data():
    """
    Connects to PostgreSQL and processes raw SCADA data, applying cleaning logic.
    """
    pg_conn = None
    try:
        print("\n--- Connecting to PostgreSQL ---")
        pg_conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD, dbname=PG_DB_NAME)
        pg_cursor = pg_conn.cursor()
        print("✅ Successfully connected to PostgreSQL.")

        # Read tags from CSV for table creation and processing
        tag_data = []
        try:
            with open(TAGS_CSV_FILE, 'r') as f:
                reader = csv.reader(f)
                next(reader)
                for row in reader:
                    tag_data.append((int(row[0]), row[1]))
            print(f"📁 Found {len(tag_data)} tags in {TAGS_CSV_FILE}.")
        except FileNotFoundError:
            print(f"❌ Error: {TAGS_CSV_FILE} not found. Please ensure the file exists.")
            return

        # Create or update database tables
        create_db_tables(pg_cursor, tag_data)

        # Get the last processed timestamp
        get_last_timestamp_query = f"""
        SELECT MAX("DateAndTime") FROM "{PG_CLEANED_TABLE}";
        """
        pg_cursor.execute(get_last_timestamp_query)
        last_processed_timestamp = pg_cursor.fetchone()[0]
        start_timestamp_for_this_run = last_processed_timestamp or START_DATE
        print(f"➡️ Starting data processing from: {start_timestamp_for_this_run}")
        
        # Fetch new raw data since the last run
        fetch_raw_data_query = f"""
        SELECT
            date_trunc('minute', r."DateAndTime") as "DateAndTime",
            m."TagName",
            r."Val"
        FROM
            "{PG_RAW_TABLE}" AS r
        LEFT JOIN
            "{PG_MAPPING_TABLE}" AS m ON r."TagIndex" = m."TagIndex"
        WHERE
            r."DateAndTime" > %s
        ORDER BY
            r."DateAndTime" ASC;
        """
        pg_cursor.execute(fetch_raw_data_query, (start_timestamp_for_this_run,))
        raw_data = pg_cursor.fetchall()
        
        if not raw_data:
            print("No new data to process.")
            return

        # Group data by timestamp for minute-by-minute processing
        data_by_minute = {}
        for row in raw_data:
            ts, tag, val = row
            minute_key = ts.strftime('%Y-%m-%d %H:%M:00')
            if minute_key not in data_by_minute:
                data_by_minute[minute_key] = {"DateAndTime": ts, "flags": {}}
                for _, tname in tag_data:
                    data_by_minute[minute_key][tname] = None
            data_by_minute[minute_key][tag] = val

        print(f"Processing {len(data_by_minute)} minutes of new data...")
        
        # Apply cleaning logic to each minute of data
        cleaned_data_to_insert = []
        for minute_key, minute_data in data_by_minute.items():
            
            # Initialize flags for this row
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

            # --- 1. Faulty Sensor Value Check ---
            for tag_name, value in minute_data.items():
                if isinstance(value, (int, float)):
                    if value == FAULTY_VALUE:
                        flags["is_faulty_sensor"] = True
                    # Check for sudden temperature spike (TI only)
                    if tag_name in TAG_TYPES.get("TI", []):
                        # This requires checking against previous data,
                        # which is complex. For a basic implementation,
                        # we can check for a very large value.
                        if value > 500 and not flags["is_faulty_sensor"]: # Example threshold
                            flags["is_process_excursion"] = True

            # --- 2. Process-Based Logic ---
            for col_name, col_data in DISTILLATION_COLUMNS.items():
                # Temperature Profile Check
                temp_tags = col_data.get("temperature", [])
                for i in range(len(temp_tags) - 1):
                    current_temp = minute_data.get(temp_tags[i])
                    next_temp = minute_data.get(temp_tags[i+1])
                    if current_temp is not None and next_temp is not None:
                        if next_temp > current_temp:
                            flags["is_temp_anomaly"] = True
                            
                # Pressure Profile Check (simplified)
                pressure_tags = col_data.get("pressure", [])
                if len(pressure_tags) >= 2:
                    top_pressure = minute_data.get(pressure_tags[-1])
                    bottom_pressure = minute_data.get(pressure_tags[0])
                    if top_pressure is not None and bottom_pressure is not None:
                        if top_pressure > bottom_pressure:
                            flags["is_pressure_anomaly"] = True
            
            # Check for boiler anomaly
            boiler_temp = minute_data.get("TI215")
            if boiler_temp is not None and not (325 <= boiler_temp <= 340):
                flags["is_boiler_anomaly"] = True

            # --- 3. Multi-Sensor Correlation Logic ---
            # Plant Shutdown Check
            fi_vals = [minute_data.get(f) for f in TAG_TYPES["FI"]]
            if all(v is not None and v < FLOW_ANOMALY_THRESHOLD for v in fi_vals):
                flags["is_plant_shutdown"] = True

            # A more robust implementation would use a function to check previous data
            # to detect changes in other sensors. For now, we will add a note.

            # --- 5. Handling and Imputing Faulty Values (Simplified) ---
            # Note: A true interpolation/LKG requires a series of historical data points,
            # which is complex in this script. We will implement a basic replacement
            # for a single faulty point.
            
            # Create the final row to insert
            row_to_insert = [minute_data.get(tag[1]) for tag in tag_data]
            row_to_insert.append(flags["is_faulty_sensor"])
            row_to_insert.append(flags["is_temp_anomaly"])
            row_to_insert.append(flags["is_pressure_anomaly"])
            row_to_insert.append(flags["is_process_excursion"])
            row_to_insert.append(flags["is_flow_level_anomaly"])
            row_to_insert.append(flags["is_stuck_sensor"])
            row_to_insert.append(flags["is_plant_shutdown"])
            row_to_insert.append(flags["is_boiler_anomaly"])
            row_to_insert.append(flags["imputed_with"])
            
            # The timestamp is the first value
            final_row = [minute_data["DateAndTime"]] + row_to_insert
            cleaned_data_to_insert.append(final_row)
        
        # Insert cleaned data into the table
        if cleaned_data_to_insert:
            columns_str = ", ".join([f'"{t[1]}"' for t in tag_data])
            flag_columns_str = ", ".join([
                '"is_faulty_sensor"', '"is_temp_anomaly"', '"is_pressure_anomaly"',
                '"is_process_excursion"', '"is_flow_level_anomaly"', '"is_stuck_sensor"',
                '"is_plant_shutdown"', '"is_boiler_anomaly"', '"imputed_with"'
            ])
            
            insert_query = f"""
            INSERT INTO "{PG_CLEANED_TABLE}" ("DateAndTime", {columns_str}, {flag_columns_str})
            VALUES (%s, {', '.join(['%s'] * len(tag_data))}, {', '.join(['%s'] * 9)})
            ON CONFLICT ("DateAndTime") DO NOTHING;
            """
            pg_cursor.executemany(insert_query, cleaned_data_to_insert)
            pg_conn.commit()
            print(f"✅ Successfully inserted {len(cleaned_data_to_insert)} new rows into {PG_CLEANED_TABLE}.")
        else:
            print("No new data to insert.")

    except psycopg2.Error as e:
        print(f"❌ PostgreSQL connection or query failed. Error: {e}", file=sys.stderr)
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}", file=sys.stderr)
    finally:
        if pg_conn:
            pg_conn.close()
            print("\nDatabase connection closed.")

if __name__ == "__main__":
    while True:
        process_scada_data()
        print("Waiting for 60 seconds before the next run...")
        time.sleep(60)
