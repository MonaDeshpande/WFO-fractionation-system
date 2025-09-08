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
PG_RAW_TABLE = "wide_scada_data"
PG_MAPPING_TABLE = "tag_mapping"
PG_CLEANED_TABLE = "data_cleaning_with_report"
TAGS_CSV_FILE = "TAG_INDEX_FINAL.csv"

# --- USER INPUT ---
START_DATE = "2025-09-03 00:00:40"
END_DATE = "2025-09-08 11:15:00"

# Faulty value constant
FAULTY_VALUE = 32767
# Thresholds for anomaly detection
FLOW_ANOMALY_THRESHOLD = 0.5
STUCK_SENSOR_DURATION = timedelta(minutes=60)
TI_SPIKE_THRESHOLD = 20  # 20°C
SMOOTHING_WINDOW_SIZE = 3 # in minutes

# Distillation column tag sequences
DISTILLATION_COLUMNS = {
    "Column_1": {
        "temperature": ["TI61", "TI62", "TI63", "TI64", "TI65"],
        "pressure": {"PTT": "PI61", "PTB": "PI62"}
    },
    "Column_2": {
        "temperature": ["TI03", "TI04", "TI05", "TI06"],
        "pressure": {"PTT": "PI03", "PTB": "PI04"}
    },
    "Column_3": {
        "temperature": ["TI13", "TI14", "TI15", "TI16", "TI17", "TI18", "TI19", "TI20", "TI21", "TI22", "TI23", "TI24"],
        "pressure": {"PTT": "PI13", "PTB": "PI24"}
    },
    "Column_4": {
        "temperature": ["TI31", "TI32", "TI33", "TI34", "TI35", "TI36", "TI37", "TI38", "TI39"],
        "pressure": {"PTT": "PI31", "PTB": "PI39"}
    }
}

# Tag types for multi-sensor logic
TAG_TYPES = {
    "TI": ["TI61", "TI62", "TI63", "TI64", "TI65", "TI03", "TI04", "TI05", "TI06", "TI13", "TI14", "TI15", "TI16", "TI17", "TI18", "TI19", "TI20", "TI21", "TI22", "TI23", "TI24", "TI31", "TI32", "TI33", "TI34", "TI35", "TI36", "TI37", "TI38", "TI39", "TI215"],
    "PI": ["PI61", "PI62", "PI03", "PI04", "PI13", "PI24", "PI31", "PI39"],
    "LI": ["LI61", "LI62", "LI03", "LI04"],
    "FI": ["FI08", "FI09", "FI10", "FT08", "FT09", "FT10"] # Added FT tags for clarity
}

# Example operating ranges (adjust these based on your process data)
OPERATING_RANGES = {
    "TI61": (100, 150),
    "TI215": (325, 340),
    # Add other ranges here
}

# --------------------------------------------------------------------------------------------------

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
        '"imputed_with" VARCHAR(50) DEFAULT NULL',
        '"notes" TEXT'
    ]
    
    # Drop and recreate the cleaned table to avoid schema issues
    pg_cursor.execute(f"DROP TABLE IF EXISTS \"{PG_CLEANED_TABLE}\" CASCADE;")
    
    create_cleaned_table_query = f"""
    CREATE TABLE "{PG_CLEANED_TABLE}" (
        "DateAndTime" TIMESTAMP PRIMARY KEY,
        {", ".join(columns)},
        {", ".join(flag_columns)}
    );
    """
    pg_cursor.execute(create_cleaned_table_query)
    pg_cursor.connection.commit()
    print(f"✅ Tables verified/created: {PG_MAPPING_TABLE}, {PG_CLEANED_TABLE}")

# --------------------------------------------------------------------------------------------------

def generate_excel_report(summary_data, detail_log, start_dt, end_dt):
    """Generates an Excel report with summary and detailed logs."""
    report_filename = f"SCADA_Report_{start_dt.strftime('%Y%m%d')}_to_{end_dt.strftime('%Y%m%d')}.xlsx"
    print(f"\n--- Generating Excel Report: {report_filename} ---")
    
    writer = pd.ExcelWriter(report_filename, engine='xlsxwriter')
    workbook = writer.book

    summary_sheet = workbook.add_worksheet('Summary')
    summary_sheet.write('A1', 'SCADA Data Analysis Report')
    summary_sheet.write('A2', f"Period: {start_dt.strftime('%Y-%m-%d %H:%M:%S')} to {end_dt.strftime('%Y-%m-%d %H:%M:%S')}")
    
    summary_sheet.write('A4', 'Anomaly Summary')
    anomaly_summary = pd.DataFrame(summary_data['anomaly_counts'], index=['Total Count']).T
    anomaly_summary.to_excel(writer, sheet_name='Summary', startrow=5, startcol=0)
    
    summary_sheet.write('A10', 'Top 5 Most Faulty Instruments by Percentage')
    faulty_instruments_df = pd.DataFrame(summary_data['faulty_instruments'])
    # Ensure sorting works by converting the string percentage to a float
    faulty_instruments_df['Faulty Readings (%)'] = faulty_instruments_df['Faulty Readings (%)'].str.rstrip('%').astype(float)
    faulty_instruments_df = faulty_instruments_df.sort_values(by='Faulty Readings (%)', ascending=False).head(5)
    faulty_instruments_df.to_excel(writer, sheet_name='Summary', startrow=11, startcol=0, index=False)

    detail_df = pd.DataFrame(detail_log)
    if not detail_df.empty:
        detail_df.to_excel(writer, sheet_name='Detailed Log', index=False)

    writer.close()
    print(f"✅ Report saved to {report_filename}")

# --------------------------------------------------------------------------------------------------

def process_scada_data_in_range(start_timestamp, end_timestamp):
    """Connects to PostgreSQL, processes raw SCADA data, and generates a report."""
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
        
        # Get the list of tag names to select
        all_tags = [t[1] for t in tag_data]
        columns_to_select = ', '.join([f'"{tag}"' for tag in all_tags])
        
        # --- Fetch Wide-Format Data into Pandas DataFrame ---
        print("--- Fetching wide-format data from the database ---")
        fetch_wide_data_query = f"""
        SELECT
            "DateAndTime",
            {columns_to_select}
        FROM
            "{PG_RAW_TABLE}"
        WHERE
            "DateAndTime" BETWEEN %s AND %s
        ORDER BY
            "DateAndTime" ASC;
        """
        raw_data_df = pd.read_sql_query(fetch_wide_data_query, pg_conn, params=(start_timestamp, end_timestamp))
        
        if raw_data_df.empty:
            print("No data found in the specified range.")
            return
            
        # The fetched DataFrame is already in the correct wide format.
        # Set the DateAndTime as the index and resample.
        processed_df = raw_data_df.set_index('DateAndTime').resample('1T').mean().reset_index()
        processed_df.index = processed_df['DateAndTime']
        
        print(f"Processing {len(processed_df)} minutes of new data...")
        
        # Initialize flags and a notes column
        flag_cols = [
            'is_faulty_sensor', 'is_temp_anomaly', 'is_pressure_anomaly',
            'is_process_excursion', 'is_flow_level_anomaly', 'is_stuck_sensor',
            'is_plant_shutdown', 'is_boiler_anomaly'
        ]
        for col in flag_cols:
            processed_df[col] = False
        processed_df['imputed_with'] = None
        processed_df['notes'] = ''

        detailed_log = []
        anomaly_counts = {col: 0 for col in flag_cols}
        
        # --- Rule 1: Faulty Sensor Value Check and Process Excursion ---
        print("--- Applying Rule 1: Faulty Sensor Value Check ---")
        ti_tags = TAG_TYPES["TI"]
        
        for tag in all_tags:
            if tag in processed_df.columns:
                # Condition 1: Direct faulty value (32767)
                faulty_mask = (processed_df[tag] == FAULTY_VALUE)
                processed_df.loc[faulty_mask, 'is_faulty_sensor'] = True
                processed_df.loc[faulty_mask, 'notes'] += f'Faulty reading ({FAULTY_VALUE}). '
                anomaly_counts['is_faulty_sensor'] += faulty_mask.sum()
                
                # Condition 2: Sudden temperature spike
                if tag in ti_tags:
                    diff = processed_df[tag].diff().abs()
                    spike_mask = (diff > TI_SPIKE_THRESHOLD) & (processed_df[tag].shift(-1).abs() < processed_df[tag].abs())
                    processed_df.loc[spike_mask, 'is_faulty_sensor'] = True
                    processed_df.loc[spike_mask, 'notes'] += 'Sudden temp spike detected. '
                    anomaly_counts['is_faulty_sensor'] += spike_mask.sum()

                # Condition 3: High-value process excursion
                if tag in OPERATING_RANGES:
                    min_val, max_val = OPERATING_RANGES[tag]
                    excursion_mask = (processed_df[tag] > max_val) & (processed_df[tag] != FAULTY_VALUE)
                    processed_df.loc[excursion_mask, 'is_process_excursion'] = True
                    processed_df.loc[excursion_mask, 'notes'] += 'High-value process excursion. '
                    anomaly_counts['is_process_excursion'] += excursion_mask.sum()
        
        # --- Handle Negative Flow Values ---
        fi_tags = TAG_TYPES["FI"]
        for tag in fi_tags:
            if tag in processed_df.columns:
                negative_flow_mask = processed_df[tag] < 0
                if negative_flow_mask.any():
                    mode_val = processed_df[tag][processed_df[tag] >= 0].mode()
                    if not mode_val.empty:
                        replacement_value = mode_val[0]
                        processed_df.loc[negative_flow_mask, tag] = replacement_value
                        processed_df.loc[negative_flow_mask, 'imputed_with'] = 'Most Frequent Value'
                        processed_df.loc[negative_flow_mask, 'notes'] += 'Negative flow replaced. '
                        print(f"✅ Negative flow values for {tag} replaced with most frequent value: {replacement_value}")

        # --- Rule 2: Process-Based Logic (Temperature and Pressure) ---
        print("--- Applying Rule 2: Process-Based Logic ---")
        for col_name, col_data in DISTILLATION_COLUMNS.items():
            # Temperature Profile Check
            temp_tags = col_data.get("temperature", [])
            for i in range(len(temp_tags) - 1):
                current_tag = temp_tags[i]
                next_tag = temp_tags[i+1]
                if current_tag in processed_df.columns and next_tag in processed_df.columns:
                    anomaly_mask = (processed_df[current_tag] < processed_df[next_tag]) & (processed_df[current_tag].notna()) & (processed_df[next_tag].notna())
                    if anomaly_mask.any():
                        processed_df.loc[anomaly_mask, 'is_temp_anomaly'] = True
                        processed_df.loc[anomaly_mask, 'notes'] += f'Temp profile anomaly between {current_tag} and {next_tag} in {col_name}. '
                        anomaly_counts['is_temp_anomaly'] += anomaly_mask.sum()
            
            # Pressure Profile Check
            pressure_tags = col_data.get("pressure", {})
            if "PTT" in pressure_tags and "PTB" in pressure_tags:
                ptt = pressure_tags["PTT"]
                ptb = pressure_tags["PTB"]
                if ptt in processed_df.columns and ptb in processed_df.columns:
                    anomaly_mask = (processed_df[ptt] > processed_df[ptb]) & (processed_df[ptt].notna()) & (processed_df[ptb].notna())
                    if anomaly_mask.any():
                        processed_df.loc[anomaly_mask, 'is_pressure_anomaly'] = True
                        processed_df.loc[anomaly_mask, 'notes'] += f'Pressure profile anomaly in {col_name}. '
                        anomaly_counts['is_pressure_anomaly'] += anomaly_mask.sum()
                        
        # --- Rule 3: Multi-Sensor Correlation Logic ---
        print("--- Applying Rule 3: Multi-Sensor Correlation ---")
        li_tags = TAG_TYPES["LI"]
        
        # Flow/Level Discrepancy (Line Choking) - Assumes LI/FT tags are related by number, e.g., LI61 -> FT61
        for li_tag in li_tags:
            fi_tag = li_tag.replace('LI', 'FT')
            if li_tag in processed_df.columns and fi_tag in processed_df.columns:
                level_increasing = processed_df[li_tag].diff() > 0.1
                flow_low = processed_df[fi_tag] <= FLOW_ANOMALY_THRESHOLD
                anomaly_mask = level_increasing & flow_low
                if anomaly_mask.any():
                    processed_df.loc[anomaly_mask, 'is_flow_level_anomaly'] = True
                    processed_df.loc[anomaly_mask, 'notes'] += f'Flow/Level anomaly (choking) at {li_tag}. '
                    anomaly_counts['is_flow_level_anomaly'] += anomaly_mask.sum()
        
        # Stuck Sensor (using rolling standard deviation)
        for tag in all_tags:
            if tag in processed_df.columns:
                rolling_std = processed_df[tag].rolling(window='60T', min_periods=2).std()
                stuck_mask = (rolling_std == 0) & (processed_df[tag].notna())
                if stuck_mask.any():
                    processed_df.loc[stuck_mask, 'is_stuck_sensor'] = True
                    processed_df.loc[stuck_mask, 'notes'] += f'Stuck sensor detected on {tag}. '
                    anomaly_counts['is_stuck_sensor'] += stuck_mask.sum()

        # --- Rule 4: Event-Triggered Checks ---
        print("--- Applying Rule 4: Event-Triggered Checks ---")
        # Plant Shutdown Event
        ft_shutdown_tags = ["FT08", "FT09", "FT10"]
        if all(tag in processed_df.columns for tag in ft_shutdown_tags):
            all_ft_zero = (processed_df[ft_shutdown_tags] <= FLOW_ANOMALY_THRESHOLD).all(axis=1)
            if all_ft_zero.any():
                processed_df.loc[all_ft_zero, 'is_plant_shutdown'] = True
                processed_df.loc[all_ft_zero, 'notes'] += 'Plant Shutdown Event. '
                anomaly_counts['is_plant_shutdown'] += all_ft_zero.sum()

        # Boiler Issue
        if 'TI215' in processed_df.columns:
            boiler_mask = (processed_df['TI215'] < 325) | (processed_df['TI215'] > 340)
            if boiler_mask.any():
                processed_df.loc[boiler_mask, 'is_boiler_anomaly'] = True
                processed_df.loc[boiler_mask, 'notes'] += 'Boiler-Related Anomaly. '
                anomaly_counts['is_boiler_anomaly'] += boiler_mask.sum()

        # --- Rule 5: Handling and Imputing Faulty Values ---
        print("--- Applying Rule 5: Handling and Imputing ---")
        
        impute_mask = processed_df['is_faulty_sensor'] | processed_df['is_stuck_sensor']

        for tag in all_tags:
            if tag in processed_df.columns:
                is_faulty = processed_df['is_faulty_sensor'] & processed_df[tag].isnull()
                if is_faulty.any():
                    processed_df[tag] = processed_df[tag].interpolate(method='linear', limit_direction='both')
                    processed_df.loc[is_faulty & processed_df[tag].notna(), 'imputed_with'] = 'Linear Interpolation'

        lkg_mask = processed_df['is_faulty_sensor'] & processed_df['imputed_with'].isnull()
        for tag in all_tags:
            if tag in processed_df.columns:
                processed_df.loc[lkg_mask, tag] = processed_df[tag].shift(1)
                processed_df.loc[lkg_mask, 'imputed_with'] = 'LKG'

        # --- Basic Data Smoothing ---
        print("--- Applying basic data smoothing ---")
        smoothing_tags = TAG_TYPES["TI"] + TAG_TYPES["PI"]
        for tag in smoothing_tags:
            if tag in processed_df.columns:
                smoothed_column_name = f"{tag}_smoothed"
                processed_df[smoothed_column_name] = processed_df[tag].rolling(
                    window=f'{SMOOTHING_WINDOW_SIZE}T',
                    min_periods=1,
                    center=True
                ).mean()
                
        # --- Prepare for insertion into the database ---
        all_columns = ['DateAndTime'] + all_tags + flag_cols + ['imputed_with', 'notes']
        processed_df_final = processed_df.reset_index(drop=True)[all_columns]
        processed_df_final = processed_df_final.replace({np.nan: None})
        
        # Log to the database
        print("--- Inserting cleaned data into the database ---")
        pg_cursor.executemany(f"""
            INSERT INTO "{PG_CLEANED_TABLE}" ({', '.join([f'"{c}"' for c in all_columns])})
            VALUES ({', '.join(['%s'] * len(all_columns))})
            ON CONFLICT ("DateAndTime") DO UPDATE SET
                {', '.join([f'"{c}" = EXCLUDED."{c}"' for c in all_tags])},
                {', '.join([f'"{c}" = EXCLUDED."{c}"' for c in flag_cols])},
                "imputed_with" = EXCLUDED."imputed_with",
                "notes" = EXCLUDED."notes"
            """, [tuple(row) for row in processed_df_final.values])

        pg_conn.commit()
        print(f"✅ Successfully inserted/updated {len(processed_df_final)} rows into {PG_CLEANED_TABLE}.")
        
        # --- Report Generation ---
        print("\n--- Generating Report Data ---")
        faulty_instruments_list = []
        for tag in all_tags:
            if tag in processed_df.columns:
                total_readings = len(processed_df)
                
                # Use a combined boolean mask to get a single, non-overlapping count
                faulty_mask = (processed_df[tag] == FAULTY_VALUE)
                
                # Check for stuck sensors on this specific tag
                rolling_std = processed_df[tag].rolling(window='60T', min_periods=2).std()
                stuck_mask = (rolling_std == 0) & (processed_df[tag].notna())
                faulty_mask = faulty_mask | stuck_mask
                
                # Check for temp spikes on this specific tag
                if tag in TAG_TYPES["TI"]:
                    diff = processed_df[tag].diff().abs()
                    spike_mask = (diff > TI_SPIKE_THRESHOLD) & (processed_df[tag].shift(-1).abs() < processed_df[tag].abs())
                    faulty_mask = faulty_mask | spike_mask

                faulty_count = faulty_mask.sum()

                if total_readings > 0:
                    percentage = (faulty_count / total_readings) * 100
                    faulty_instruments_list.append({
                        "Instrument Name": tag,
                        "Total Faulty Readings": int(faulty_count),
                        "Total Readings": total_readings,
                        "Faulty Readings (%)": f"{percentage:.2f}%"
                    })
        
        faulty_instruments_list = sorted(faulty_instruments_list, key=lambda x: float(x['Faulty Readings (%)'].strip('%')), reverse=True)

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

# --------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    start_dt = datetime.strptime(START_DATE, '%Y-%m-%d %H:%M:%S')
    end_dt = datetime.strptime(END_DATE, '%Y-%m-%d %H:%M:%S')
    process_scada_data_in_range(start_dt, end_dt)