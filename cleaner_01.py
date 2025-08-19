import psycopg2
import sys
import csv
import pandas as pd
from datetime import datetime, timedelta
import xlsxwriter

# ==============================================================================
# CONFIGURATION
# ==============================================================================
PG_HOST = "localhost"
PG_PORT = "5432"
PG_USER = "postgres"
PG_PASSWORD = "ADMIN"  # <-- IMPORTANT: Add your PostgreSQL password here
PG_DB_NAME = "scada_data_analysis"
PG_RAW_TABLE = "wide_scada_data"
PG_MAPPING_TABLE = "tag_mapping"
PG_CLEANED_TABLE = "cleaned_scada_data_report"
TAGS_CSV_FILE = "TAG_INDEX_FINAL.csv"

# --- USER INPUT ---
START_DATE = "2025-08-08 00:00:00"
END_DATE = "2025-08-15 00:00:00"

# Faulty value constant
FAULTY_VALUE = 32767
# Thresholds for anomaly detection
FLOW_ANOMALY_THRESHOLD = 0.5
STUCK_SENSOR_DURATION = timedelta(minutes=60)

# Distillation column tag sequences
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


def create_db_tables(pg_cursor, tag_data):
    """Creates the necessary tables if they don't exist."""
    print("--- Verifying database tables ---")
    create_mapping_table_query = f"""
    CREATE TABLE IF NOT EXISTS "{PG_MAPPING_TABLE}" (
        "TagIndex" INTEGER PRIMARY KEY,
        "TagName" VARCHAR(255) UNIQUE
    );
    """
    pg_cursor.execute(create_mapping_table_query)

    insert_mapping_query = f"""
    INSERT INTO "{PG_MAPPING_TABLE}" ("TagIndex", "TagName")
    VALUES (%s, %s)
    ON CONFLICT ("TagIndex") DO UPDATE SET "TagName" = EXCLUDED."TagName";
    """
    pg_cursor.executemany(insert_mapping_query, tag_data)

    columns = [f'"{tag}" DOUBLE PRECISION' for _, tag in tag_data]
    
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
        {", ".join(columns)},
        {", ".join(flag_columns)}
    );
    """
    pg_cursor.execute(create_cleaned_table_query)
    pg_cursor.connection.commit()
    print(f"✅ Tables verified/created: {PG_MAPPING_TABLE}, {PG_CLEANED_TABLE}")

def get_last_known_good_value(pg_cursor, tag_name, end_date):
    """Fetches the last known good value for a given tag before a certain time."""
    query = f"""
    SELECT "{tag_name}"
    FROM "{PG_CLEANED_TABLE}"
    WHERE "DateAndTime" < %s AND "{tag_name}" IS NOT NULL
    ORDER BY "DateAndTime" DESC
    LIMIT 1;
    """
    pg_cursor.execute(query, (end_date,))
    result = pg_cursor.fetchone()
    return result[0] if result else None

def is_stuck_sensor(pg_cursor, tag_name, start_date):
    """Checks if a sensor value has been constant for a defined duration."""
    historical_start = start_date - STUCK_SENSOR_DURATION
    query = f"""
    SELECT COUNT(DISTINCT "{tag_name}")
    FROM "{PG_RAW_TABLE}" r
    JOIN "{PG_MAPPING_TABLE}" m ON r."TagIndex" = m."TagIndex"
    WHERE m."TagName" = %s AND r."DateAndTime" BETWEEN %s AND %s;
    """
    pg_cursor.execute(query, (tag_name, historical_start, start_date))
    unique_values = pg_cursor.fetchone()[0]
    return unique_values <= 1


def generate_excel_report(summary_data, detail_log, start_dt, end_dt):
    """Generates an Excel report with summary and detailed logs."""
    report_filename = f"SCADA_Report_{start_dt.strftime('%Y%m%d')}_to_{end_dt.strftime('%Y%m%d')}.xlsx"
    print(f"\n--- Generating Excel Report: {report_filename} ---")
    
    # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pd.ExcelWriter(report_filename, engine='xlsxwriter')
    workbook = writer.book

    # Create a new sheet for the summary
    summary_sheet = workbook.add_worksheet('Summary')
    summary_sheet.write('A1', 'SCADA Data Analysis Report')
    summary_sheet.write('A2', f"Period: {start_dt.strftime('%Y-%m-%d %H:%M:%S')} to {end_dt.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Write Anomaly Summary
    summary_sheet.write('A4', 'Anomaly Summary')
    anomaly_summary = pd.DataFrame(summary_data['anomaly_counts'], index=['Total Count']).T
    anomaly_summary.to_excel(writer, sheet_name='Summary', startrow=5, startcol=0)
    
    # Write Top Faulty Instruments
    summary_sheet.write('A10', 'Top 5 Most Faulty Instruments by Percentage')
    faulty_instruments_df = pd.DataFrame(summary_data['faulty_instruments']).sort_values(by='Faulty Readings (%)', ascending=False).head(5)
    faulty_instruments_df.to_excel(writer, sheet_name='Summary', startrow=11, startcol=0, index=False)

    # Create a new sheet for the detailed log
    detail_df = pd.DataFrame(detail_log)
    if not detail_df.empty:
        detail_df.to_excel(writer, sheet_name='Detailed Log', index=False)

    # Close the Pandas Excel writer and save the file
    writer.close()
    print(f"✅ Report saved to {report_filename}")

def process_scada_data_in_range(start_timestamp, end_timestamp):
    """Connects to PostgreSQL and processes raw SCADA data for a specific time range."""
    pg_conn = None
    try:
        print(f"\n--- Connecting to PostgreSQL and processing data from {start_timestamp} to {end_timestamp} ---")
        pg_conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD, dbname=PG_DB_NAME)
        pg_cursor = pg_conn.cursor()
        print("✅ Successfully connected to PostgreSQL.")

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

        create_db_tables(pg_cursor, tag_data)
        
        # Fetch raw data for the specified range
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
            r."DateAndTime" BETWEEN %s AND %s
        ORDER BY
            r."DateAndTime" ASC;
        """
        pg_cursor.execute(fetch_raw_data_query, (start_timestamp, end_timestamp))
        raw_data = pg_cursor.fetchall()
        
        if not raw_data:
            print("No data found in the specified range.")
            return

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
        
        cleaned_data_to_insert = []
        anomaly_counts = {
            "is_faulty_sensor": 0,
            "is_temp_anomaly": 0,
            "is_pressure_anomaly": 0,
            "is_process_excursion": 0,
            "is_flow_level_anomaly": 0,
            "is_stuck_sensor": 0,
            "is_plant_shutdown": 0,
            "is_boiler_anomaly": 0
        }
        faulty_readings_count = {tag: 0 for _, tag in tag_data}
        total_readings_count = {tag: 0 for _, tag in tag_data}
        detailed_log = []

        for minute_key, minute_data in data_by_minute.items():
            
            flags = {key: False for key in anomaly_counts.keys()}
            imputed_with = None
            row_to_insert = {tag: minute_data.get(tag) for _, tag in tag_data}

            # --- Imputation and Faulty Value Check ---
            for tag_name, value in row_to_insert.items():
                if value is not None:
                    total_readings_count[tag_name] += 1
                
                if value == FAULTY_VALUE:
                    flags["is_faulty_sensor"] = True
                    anomaly_counts["is_faulty_sensor"] += 1
                    faulty_readings_count[tag_name] += 1
                    
                    lkg_value = get_last_known_good_value(pg_cursor, tag_name, minute_data["DateAndTime"])
                    
                    # Log the faulty value replacement
                    log_entry = {
                        "DateAndTime": minute_data["DateAndTime"],
                        "Instrument": tag_name,
                        "Faulty Value": value,
                        "Replaced With": lkg_value,
                        "Reason": "Faulty sensor reading (32767). Replaced with Last Known Good (LKG) value."
                    }
                    detailed_log.append(log_entry)
                    
                    row_to_insert[tag_name] = lkg_value
                    if lkg_value is not None:
                        imputed_with = "LKG"

            # --- Anomaly and Process-Based Logic ---
            # Plant Shutdown Check
            fi_vals = [row_to_insert.get(f) for f in TAG_TYPES.get("FI", []) if row_to_insert.get(f) is not None]
            if fi_vals and all(v < FLOW_ANOMALY_THRESHOLD for v in fi_vals):
                flags["is_plant_shutdown"] = True
                anomaly_counts["is_plant_shutdown"] += 1

            # Stuck Sensor Check
            for tag_name, value in row_to_insert.items():
                if tag_name in TAG_TYPES.get("TI", []) and value is not None and not flags["is_faulty_sensor"]:
                    if is_stuck_sensor(pg_cursor, tag_name, minute_data["DateAndTime"]):
                        flags["is_stuck_sensor"] = True
                        anomaly_counts["is_stuck_sensor"] += 1
                        break

            # Distillation Column Checks
            for col_name, col_data in DISTILLATION_COLUMNS.items():
                temp_tags = col_data.get("temperature", [])
                for i in range(len(temp_tags) - 1):
                    current_temp = row_to_insert.get(temp_tags[i])
                    next_temp = row_to_insert.get(temp_tags[i+1])
                    if current_temp is not None and next_temp is not None:
                        if next_temp > current_temp: # Monotonicity check
                            flags["is_temp_anomaly"] = True
                            anomaly_counts["is_temp_anomaly"] += 1
                            break # Found anomaly, no need to check other temps in this column
                            
                pressure_tags = col_data.get("pressure", [])
                if len(pressure_tags) >= 2:
                    top_pressure = row_to_insert.get(pressure_tags[-1])
                    bottom_pressure = row_to_insert.get(pressure_tags[0])
                    if top_pressure is not None and bottom_pressure is not None:
                        if top_pressure > bottom_pressure:
                            flags["is_pressure_anomaly"] = True
                            anomaly_counts["is_pressure_anomaly"] += 1

            # Boiler Anomaly Check
            boiler_temp = row_to_insert.get("TI215")
            if boiler_temp is not None and not (325 <= boiler_temp <= 340):
                flags["is_boiler_anomaly"] = True
                anomaly_counts["is_boiler_anomaly"] += 1
            
            # Prepare the row for insertion
            final_row_values = [minute_data["DateAndTime"]] + [row_to_insert.get(tag) for _, tag in tag_data]
            final_row_values.append(flags["is_faulty_sensor"])
            final_row_values.append(flags["is_temp_anomaly"])
            final_row_values.append(flags["is_pressure_anomaly"])
            final_row_values.append(flags["is_process_excursion"]) # This flag isn't used in this script but is kept for consistency.
            final_row_values.append(flags["is_flow_level_anomaly"]) # This flag isn't used but is kept for consistency.
            final_row_values.append(flags["is_stuck_sensor"])
            final_row_values.append(flags["is_plant_shutdown"])
            final_row_values.append(flags["is_boiler_anomaly"])
            final_row_values.append(imputed_with)

            cleaned_data_to_insert.append(final_row_values)
        
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

        # --- Report Generation ---
        faulty_instruments_list = []
        for tag, count in faulty_readings_count.items():
            total_readings = total_readings_count[tag]
            if total_readings > 0:
                percentage = (count / total_readings) * 100
                faulty_instruments_list.append({
                    "Instrument Name": tag,
                    "Total Faulty Readings": count,
                    "Total Readings": total_readings,
                    "Faulty Readings (%)": f"{percentage:.2f}%"
                })
        
        summary_report = {
            "anomaly_counts": anomaly_counts,
            "faulty_instruments": faulty_instruments_list
        }
        
        generate_excel_report(summary_report, detailed_log, start_timestamp, end_timestamp)

    except psycopg2.Error as e:
        print(f"❌ PostgreSQL connection or query failed. Error: {e}", file=sys.stderr)
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}", file=sys.stderr)
    finally:
        if pg_conn:
            pg_conn.close()
            print("\nDatabase connection closed.")

if __name__ == "__main__":
    start_dt = datetime.strptime(START_DATE, '%Y-%m-%d %H:%M:%S')
    end_dt = datetime.strptime(END_DATE, '%Y-%m-%d %H:%M:%S')
    process_scada_data_in_range(start_dt, end_dt)