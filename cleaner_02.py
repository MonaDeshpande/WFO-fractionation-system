import psycopg2
import sys
import csv
import pandas as pd
from datetime import datetime, timedelta
import xlsxwriter
import numpy as np

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
PG_CLEANED_TABLE = "data_cleaning_with_report"
TAGS_CSV_FILE = "TAG_INDEX_FINAL.csv"

# --- USER INPUT ---
START_DATE = "2025-08-08 00:00:40"
END_DATE = "2025-08-15 00:00:00"

# Faulty value constant
FAULTY_VALUE = 32767

# Anomaly detection thresholds
STUCK_SENSOR_DURATION = timedelta(minutes=60)
# A temperature spike is a change of more than 20 degC in one minute
TEMP_SPIKE_THRESHOLD = 20
# A flow is considered low if it's below this value (e.g., in kg/h or m3/h)
FLOW_ANOMALY_THRESHOLD = 0.5
# A level is considered stable if its change over an hour is below this threshold
LEVEL_STABLE_THRESHOLD = 0.5

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
    "FI": ["FT08", "FT09", "FT10"] # Assuming these are flow indicators
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
    WHERE "DateAndTime" < %s AND "{tag_name}" IS NOT NULL AND "{tag_name}" != %s
    ORDER BY "DateAndTime" DESC
    LIMIT 1;
    """
    pg_cursor.execute(query, (end_date, FAULTY_VALUE))
    result = pg_cursor.fetchone()
    return result[0] if result else None

def get_historical_data(pg_cursor, tag_name, start_date, duration=STUCK_SENSOR_DURATION):
    """Fetches historical data for a given tag to check for stuck sensor or spikes."""
    historical_start = start_date - duration
    query = f"""
    SELECT "DateAndTime", "{tag_name}"
    FROM "{PG_RAW_TABLE}" r
    JOIN "{PG_MAPPING_TABLE}" m ON r."TagIndex" = m."TagIndex"
    WHERE m."TagName" = %s AND r."DateAndTime" BETWEEN %s AND %s
    ORDER BY r."DateAndTime" ASC;
    """
    pg_cursor.execute(query, (tag_name, historical_start, start_date))
    return pg_cursor.fetchall()

def is_stuck_sensor(data, duration=STUCK_SENSOR_DURATION):
    """Checks if a sensor value has been constant for a defined duration."""
    df = pd.DataFrame(data, columns=['DateAndTime', 'Value'])
    if df.empty or len(df) < 5: # Need a minimum number of data points
        return False
    
    # Check if the number of unique values is very low
    if df['Value'].nunique() <= 1:
        return True
    return False

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

        df_raw = pd.DataFrame(raw_data, columns=['DateAndTime', 'TagName', 'Val'])
        
        # --- FIX: Convert 'Val' column to numeric before pivoting ---
        df_raw['Val'] = pd.to_numeric(df_raw['Val'], errors='coerce')
        
        df_pivot = df_raw.pivot_table(index='DateAndTime', columns='TagName', values='Val', aggfunc='mean')
        df_pivot.reset_index(inplace=True)
        df_pivot.columns.name = None
        df_pivot.sort_values(by='DateAndTime', inplace=True)
        df_pivot.set_index('DateAndTime', inplace=True)

        print(f"Processing {len(df_pivot)} minutes of new data...")
        
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
        faulty_readings_count = {tag[1]: 0 for tag in tag_data}
        detailed_log = []
        
        # Add placeholder columns for flags and imputation method
        for flag in anomaly_counts.keys():
            df_pivot[flag] = False
        df_pivot['imputed_with'] = None
        
        # Pre-process faulty values for better imputation
        for tag in df_pivot.columns:
            if tag in [t[1] for t in tag_data]: # Ensure it's a valid tag
                is_faulty = (df_pivot[tag] == FAULTY_VALUE)
                df_pivot.loc[is_faulty, 'is_faulty_sensor'] = True
                anomaly_counts["is_faulty_sensor"] += is_faulty.sum()
                faulty_readings_count[tag] += is_faulty.sum()
                
                # Use linear interpolation for consecutive faulty values
                faulty_groups = is_faulty.astype(int).diff().ne(0).cumsum()
                for group, group_data in is_faulty.groupby(faulty_groups):
                    if group_data.all() and len(group_data) > 1:
                        start_time = group_data.index[0]
                        end_time = group_data.index[-1]
                        
                        # Find last good value before the group
                        last_good = df_pivot.loc[df_pivot.index < start_time, tag].iloc[-1] if any(df_pivot.index < start_time) else None
                        # Find first good value after the group
                        next_good = df_pivot.loc[df_pivot.index > end_time, tag].iloc[0] if any(df_pivot.index > end_time) else None
                        
                        if last_good is not None and next_good is not None:
                            df_pivot.loc[group_data.index, tag] = np.linspace(last_good, next_good, len(group_data))
                            df_pivot.loc[group_data.index, 'imputed_with'] = 'Linear Interpolation'
                            log_entry = {
                                "DateAndTime": start_time,
                                "Instrument": tag,
                                "Faulty Value": FAULTY_VALUE,
                                "Replaced With": f"Interpolated values between {last_good:.2f} and {next_good:.2f}",
                                "Reason": "Consecutive faulty readings. Replaced with Linear Interpolation."
                            }
                            detailed_log.append(log_entry)
                        else: # Fallback to LKG if interpolation not possible
                            df_pivot.loc[group_data.index, tag] = last_good
                            df_pivot.loc[group_data.index, 'imputed_with'] = 'LKG'
                            log_entry = {
                                "DateAndTime": start_time,
                                "Instrument": tag,
                                "Faulty Value": FAULTY_VALUE,
                                "Replaced With": last_good,
                                "Reason": "Faulty sensor reading (32767). Replaced with Last Known Good (LKG) value."
                            }
                            detailed_log.append(log_entry)
                    elif group_data.all() and len(group_data) == 1: # single faulty value
                        df_pivot.loc[group_data.index, tag] = get_last_known_good_value(pg_cursor, tag, group_data.index[0])
                        df_pivot.loc[group_data.index, 'imputed_with'] = 'LKG'
                        log_entry = {
                            "DateAndTime": group_data.index[0],
                            "Instrument": tag,
                            "Faulty Value": FAULTY_VALUE,
                            "Replaced With": df_pivot.loc[group_data.index[0], tag],
                            "Reason": "Faulty sensor reading (32767). Replaced with Last Known Good (LKG) value."
                        }
                        detailed_log.append(log_entry)

        # --- Process-Based Logic on the cleaned data ---
        for minute_ts, row in df_pivot.iterrows():
            # Plant Shutdown Check
            fi_vals = [row[f] for f in TAG_TYPES.get("FI", []) if f in row]
            if fi_vals and all(v is not None and v < FLOW_ANOMALY_THRESHOLD for v in fi_vals):
                df_pivot.loc[minute_ts, "is_plant_shutdown"] = True
                anomaly_counts["is_plant_shutdown"] += 1
                
            # Distillation Column Checks
            for col_name, col_data in DISTILLATION_COLUMNS.items():
                temp_tags = [t for t in col_data.get("temperature", []) if t in row]
                for i in range(len(temp_tags) - 1):
                    current_temp = row.get(temp_tags[i])
                    next_temp = row.get(temp_tags[i+1])
                    if current_temp is not None and next_temp is not None:
                        if next_temp > current_temp: # Monotonicity check
                            df_pivot.loc[minute_ts, "is_temp_anomaly"] = True
                            anomaly_counts["is_temp_anomaly"] += 1
                            log_entry = {
                                "DateAndTime": minute_ts,
                                "Instrument": f"{temp_tags[i+1]}",
                                "Faulty Value": "N/A",
                                "Replaced With": "N/A",
                                "Reason": f"Temperature profile anomaly ({next_temp} > {current_temp}) in {col_name}."
                            }
                            detailed_log.append(log_entry)
                            
                pressure_tags = [t for t in col_data.get("pressure", []) if t in row]
                if len(pressure_tags) >= 2:
                    top_pressure = row.get(pressure_tags[-1])
                    bottom_pressure = row.get(pressure_tags[0])
                    if top_pressure is not None and bottom_pressure is not None:
                        if top_pressure > bottom_pressure:
                            df_pivot.loc[minute_ts, "is_pressure_anomaly"] = True
                            anomaly_counts["is_pressure_anomaly"] += 1
                            log_entry = {
                                "DateAndTime": minute_ts,
                                "Instrument": f"Pressure Anomaly in {col_name}",
                                "Faulty Value": "N/A",
                                "Replaced With": "N/A",
                                "Reason": f"Top pressure ({top_pressure}) > Bottom pressure ({bottom_pressure})."
                            }
                            detailed_log.append(log_entry)

            # Boiler Anomaly Check
            boiler_temp = row.get("TI215")
            if boiler_temp is not None and not (325 <= boiler_temp <= 340):
                df_pivot.loc[minute_ts, "is_boiler_anomaly"] = True
                anomaly_counts["is_boiler_anomaly"] += 1
                log_entry = {
                    "DateAndTime": minute_ts,
                    "Instrument": "TI215",
                    "Faulty Value": "N/A",
                    "Replaced With": "N/A",
                    "Reason": f"Boiler temperature ({boiler_temp}) is outside normal range (325-340 degC)."
                }
                detailed_log.append(log_entry)
                
            # Process Excursion & Stuck Sensor (Requires historical context)
            for tag_name in df_pivot.columns:
                if tag_name in TAG_TYPES.get("TI", []) and tag_name not in ["TI215"]:
                    # Check for sudden spikes
                    if minute_ts > start_timestamp:
                        prev_value = df_pivot.loc[df_pivot.index < minute_ts, tag_name].iloc[-1] if any(df_pivot.index < minute_ts) else None
                        current_value = row.get(tag_name)
                        if prev_value is not None and current_value is not None:
                            if abs(current_value - prev_value) > TEMP_SPIKE_THRESHOLD:
                                df_pivot.loc[minute_ts, 'is_process_excursion'] = True
                                anomaly_counts['is_process_excursion'] += 1
                                log_entry = {
                                    "DateAndTime": minute_ts,
                                    "Instrument": tag_name,
                                    "Faulty Value": "N/A",
                                    "Replaced With": "N/A",
                                    "Reason": f"Sudden temperature spike detected. Change: {abs(current_value - prev_value):.2f} degC."
                                }
                                detailed_log.append(log_entry)
            
            # Flow/Level Anomaly (Simplified check based on provided logic)
            if 'LI03' in row and 'FT06' in row and row['LI03'] is not None and row['FT06'] is not None:
                # Assuming FT06 is feed flow and LI03 is column level
                if row['LI03'] > 80 and row['FT06'] < FLOW_ANOMALY_THRESHOLD:
                    df_pivot.loc[minute_ts, 'is_flow_level_anomaly'] = True
                    anomaly_counts['is_flow_level_anomaly'] += 1
                    log_entry = {
                        "DateAndTime": minute_ts,
                        "Instrument": "LI03/FT06",
                        "Faulty Value": "N/A",
                        "Replaced With": "N/A",
                        "Reason": f"Potential line choking: high level ({row['LI03']:.2f}%) with low feed flow ({row['FT06']:.2f} kg/h)."
                    }
                    detailed_log.append(log_entry)
        
        # Stuck sensor check on a per-instrument basis over the whole period
        for tag_name in [t[1] for t in tag_data]:
            if tag_name in df_pivot.columns:
                historical_data = get_historical_data(pg_cursor, tag_name, start_timestamp)
                if is_stuck_sensor(historical_data, duration=STUCK_SENSOR_DURATION):
                    df_pivot['is_stuck_sensor'] = True
                    anomaly_counts['is_stuck_sensor'] = len(df_pivot)
                    log_entry = {
                        "DateAndTime": start_timestamp,
                        "Instrument": tag_name,
                        "Faulty Value": "N/A",
                        "Replaced With": "N/A",
                        "Reason": f"Stuck sensor: Value for {tag_name} has been constant for at least {STUCK_SENSOR_DURATION.total_seconds() / 60} minutes."
                    }
                    detailed_log.append(log_entry)
                    
        df_pivot.reset_index(inplace=True)
        
        # Insert cleaned data into the table
        if not df_pivot.empty:
            columns_list = ['"DateAndTime"'] + [f'"{col}"' for col in df_pivot.columns if col != 'DateAndTime']
            columns_str = ", ".join(columns_list)
            
            # The values to insert will be a list of tuples
            values_to_insert = [tuple(row) for row in df_pivot.itertuples(index=False)]
            
            insert_query = f"""
            INSERT INTO "{PG_CLEANED_TABLE}" ({columns_str})
            VALUES ({', '.join(['%s'] * len(columns_list))})
            ON CONFLICT ("DateAndTime") DO NOTHING;
            """
            
            pg_cursor.executemany(insert_query, values_to_insert)
            pg_conn.commit()
            print(f"✅ Successfully inserted {len(values_to_insert)} new rows into {PG_CLEANED_TABLE}.")
        else:
            print("No new data to insert.")

        # --- Report Generation ---
        total_readings = {tag: len(df_raw[df_raw['TagName'] == tag]) for tag in df_raw['TagName'].unique()}
        faulty_instruments_list = []
        for tag, count in faulty_readings_count.items():
            if tag in total_readings and total_readings[tag] > 0:
                percentage = (count / total_readings[tag]) * 100
                faulty_instruments_list.append({
                    "Instrument Name": tag,
                    "Total Faulty Readings": count,
                    "Total Readings": total_readings[tag],
                    "Faulty Readings (%)": percentage
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