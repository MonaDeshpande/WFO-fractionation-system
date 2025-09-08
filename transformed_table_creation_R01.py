import psycopg2
import sys
import csv
import time
from datetime import datetime

# ==============================================================================
# CONFIGURATION
# ==============================================================================
SQL_SERVER_NAME = r"WFOPC3"
SQL_DB_NAME = "Report1database"
SQL_TABLE_NAME = "dbo.FloatTable"
PG_HOST = "localhost"
PG_PORT = "5432"
PG_USER = "postgres"
PG_PASSWORD = "ADMIN"  # <-- IMPORTANT: Add your PostgreSQL password here
PG_DB_NAME = "scada_data_analysis"
PG_RAW_TABLE = "raw_data"
PG_MAPPING_TABLE = "tag_mapping"
PG_TRANSFORMED_TABLE = "wide_scada_data"
TAGS_CSV_FILE = "TAG_INDEX_FINAL.csv"

# --- USER INPUT ---
# Placeholder: Change this to your desired start date for the initial run.
# The format must be 'YYYY-MM-DD HH:MM:SS'. After the first run,
# the script will automatically continue from the last processed time.
START_DATE = "2024-09-03 00:00:00"

# ==============================================================================
# Main Logic
# ==============================================================================
def get_tag_data(pg_cursor):
    """Reads tag data from the CSV and handles database mapping."""
    tag_data = []
    try:
        with open(TAGS_CSV_FILE, 'r') as f:
            reader = csv.reader(f)
            next(reader)  # Skip the header row
            for row in reader:
                tag_data.append((int(row[0]), row[1]))
        print(f"📁 Found {len(tag_data)} tags in {TAGS_CSV_FILE}.")
    except FileNotFoundError:
        print(f"❌ Error: {TAGS_CSV_FILE} not found. Please ensure the file exists.")
        return None
        
    return tag_data

def create_mapping_table(pg_cursor, pg_conn, tag_data):
    """Creates the tag mapping table and populates it."""
    print(f"\n--- Creating mapping table '{PG_MAPPING_TABLE}' if it doesn't exist ---")
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
    pg_conn.commit()
    print(f"✅ Successfully inserted/updated {pg_cursor.rowcount} tags.")

def create_transformed_table(pg_cursor, pg_conn, tag_data):
    """
    Creates the transformed table, dropping it first if it exists to ensure
    the schema is always up-to-date with the tag list.
    """
    print(f"\n--- Creating table '{PG_TRANSFORMED_TABLE}' ---")
    
    # Drop and recreate the table to ensure the schema is always up-to-date
    print(f"⚠️ Dropping and recreating '{PG_TRANSFORMED_TABLE}' to ensure schema is up-to-date...")
    pg_cursor.execute(f"""DROP TABLE IF EXISTS "{PG_TRANSFORMED_TABLE}" CASCADE;""")
    pg_conn.commit()

    columns_definitions = [f'"{tag}" DOUBLE PRECISION' for _, tag in tag_data]
    columns_definitions.append('"Unmapped_TagIndex" INTEGER')
    columns_definitions.append('"Unmapped_Value" DOUBLE PRECISION')
    
    columns_str = ",\n".join(columns_definitions)
    
    create_table_query = f"""
    CREATE TABLE "{PG_TRANSFORMED_TABLE}" (
        "DateAndTime" TIMESTAMP PRIMARY KEY,
        {columns_str}
    );
    """
    pg_cursor.execute(create_table_query)
    pg_conn.commit()
    print(f"✅ Table '{PG_TRANSFORMED_TABLE}' successfully recreated with the correct schema.")


def process_scada_data():
    """
    Main function to connect to PostgreSQL and process data.
    """
    pg_conn = None
    try:
        print("Connecting to PostgreSQL...")
        pg_conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD, dbname=PG_DB_NAME)
        pg_cursor = pg_conn.cursor()
        print("✅ Successfully connected to PostgreSQL.")

        # Read tags from CSV and create/update the mapping table
        tag_data = get_tag_data(pg_cursor)
        if tag_data is None:
            return
            
        create_mapping_table(pg_cursor, pg_conn, tag_data)
        create_transformed_table(pg_cursor, pg_conn, tag_data)

        # Find the last processed timestamp
        get_last_timestamp_query = f"""
        SELECT MAX("DateAndTime") FROM "{PG_TRANSFORMED_TABLE}";
        """
        pg_cursor.execute(get_last_timestamp_query)
        last_processed_timestamp = pg_cursor.fetchone()[0]
        start_timestamp_for_this_run = last_processed_timestamp or START_DATE
        print(f"➡️ Starting data processing from: {start_timestamp_for_this_run}")

        # Dynamically generate and execute the pivot query
        print(f"\n--- Inserting dynamic pivoted data into '{PG_TRANSFORMED_TABLE}' ---")
        
        pivot_cases = [f"""MAX(CASE WHEN "TagName" = '{tag}' THEN "Val"::DOUBLE PRECISION END) AS "{tag}" """ for _, tag in tag_data]
        pivot_cases_str = ",\n".join(pivot_cases)

        insert_data_query = f"""
        INSERT INTO "{PG_TRANSFORMED_TABLE}"
        WITH MappedData AS (
            SELECT
                date_trunc('minute', r."DateAndTime") AS "DateAndTime",
                COALESCE(m."TagName", 'Unmapped') AS "TagName",
                r."Val",
                r."TagIndex"
            FROM
                "{PG_RAW_TABLE}" AS r
            LEFT JOIN
                "{PG_MAPPING_TABLE}" AS m ON r."TagIndex"::INTEGER = m."TagIndex"
            WHERE
                r."DateAndTime" > %s
        )
        SELECT
            "DateAndTime",
            {pivot_cases_str},
            MAX(CASE WHEN "TagName" = 'Unmapped' THEN "TagIndex"::INTEGER END) AS "Unmapped_TagIndex",
            MAX(CASE WHEN "TagName" = 'Unmapped' THEN "Val"::DOUBLE PRECISION END) AS "Unmapped_Value"
        FROM
            MappedData
        GROUP BY
            "DateAndTime"
        ORDER BY
            "DateAndTime" ASC
        ON CONFLICT ("DateAndTime") DO NOTHING;
        """
        
        pg_cursor.execute(insert_data_query, (start_timestamp_for_this_run,))
        rows_inserted = pg_cursor.rowcount
        pg_conn.commit()
        print(f"✅ Successfully inserted {rows_inserted} new rows into '{PG_TRANSFORMED_TABLE}'.")

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