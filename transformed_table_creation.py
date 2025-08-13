import psycopg2
import sys
import csv

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
PG_TRANSFORMED_TABLE = "wide_scada_data"
TAGS_CSV_FILE = "tags.csv"

def refresh_minute_data_table():
    """
    Connects to PostgreSQL, ensures tables exist, and populates it with
    minute-aggregated data. It assumes the table is either new or has been
    cleared by a separate process.
    """
    pg_conn = None
    try:
        print("Connecting to PostgreSQL to refresh database objects...")
        pg_conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD, dbname=PG_DB_NAME)
        pg_cursor = pg_conn.cursor()
        print("✅ Successfully connected to PostgreSQL.")

        # --- Step 1: Create the Tag Mapping Table if it doesn't exist ---
        print(f"\n--- Creating mapping table '{PG_MAPPING_TABLE}' if it doesn't exist ---")
        create_mapping_table_query = f"""
        CREATE TABLE IF NOT EXISTS "{PG_MAPPING_TABLE}" (
            "TagIndex" INTEGER PRIMARY KEY,
            "TagName" VARCHAR(255) UNIQUE
        );
        """
        pg_cursor.execute(create_mapping_table_query)
        pg_conn.commit()
        print(f"✅ Mapping table '{PG_MAPPING_TABLE}' created or verified.")

        # --- Step 2: Read tags from CSV and insert/update into the mapping table ---
        print(f"\n--- Reading tags from '{TAGS_CSV_FILE}' and inserting into mapping table ---")
        tag_data = []
        try:
            with open(TAGS_CSV_FILE, 'r') as f:
                reader = csv.reader(f)
                next(reader)  # Skip the header row
                for row in reader:
                    tag_data.append((int(row[0]), row[1]))
            print(f"📁 Found {len(tag_data)} tags in {TAGS_CSV_FILE}.")
        except FileNotFoundError:
            print(f"❌ Error: {TAGS_CSV_FILE} not found. Please create the file with 'TagIndex' and 'TagName' columns.")
            return

        insert_mapping_query = f"""
        INSERT INTO "{PG_MAPPING_TABLE}" ("TagIndex", "TagName")
        VALUES (%s, %s)
        ON CONFLICT ("TagIndex") DO UPDATE SET "TagName" = EXCLUDED."TagName";
        """
        pg_cursor.executemany(insert_mapping_query, tag_data)
        pg_conn.commit()
        print(f"✅ Successfully inserted/updated {pg_cursor.rowcount} tags.")
        
        # --- Step 3: Create the transformed table if it doesn't exist ---
        # The column definitions are created dynamically from the CSV
        print(f"\n--- Creating table '{PG_TRANSFORMED_TABLE}' if it doesn't exist ---")
        columns = ", ".join([f'"{tag}" DOUBLE PRECISION' for _, tag in tag_data])
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS "{PG_TRANSFORMED_TABLE}" (
            "DateAndTime" TIMESTAMP,
            {columns},
            "Unmapped_TagIndex" INTEGER,
            "Unmapped_Value" DOUBLE PRECISION
        );
        """
        pg_cursor.execute(create_table_query)
        pg_conn.commit()
        print(f"✅ Table '{PG_TRANSFORMED_TABLE}' verified or created with columns from '{TAGS_CSV_FILE}'.")

        # --- Step 4: Dynamically generate and insert the PIVOTED DATA ---
        print(f"\n--- Inserting dynamic pivoted data into '{PG_TRANSFORMED_TABLE}' ---")
        
        # Build the dynamic CASE statements for the mapped tags
        pivot_cases = [f"""MAX(CASE WHEN "TagName" = '{tag}' THEN "Val" END) AS "{tag}" """ for _, tag in tag_data]
        pivot_cases_str = ",\n             ".join(pivot_cases)

        insert_data_query = f"""
        INSERT INTO "{PG_TRANSFORMED_TABLE}"
        WITH MappedData AS (
            SELECT
                -- Truncate the timestamp to the minute for aggregation
                date_trunc('minute', r."DateAndTime") AS "DateAndTime",
                COALESCE(m."TagName", 'Unmapped') AS "TagName",
                r."Val",
                r."TagIndex"
            FROM
                "{PG_RAW_TABLE}" AS r
            LEFT JOIN
                "{PG_MAPPING_TABLE}" AS m ON r."TagIndex" = m."TagIndex"
        )
        SELECT
            "DateAndTime",
            {pivot_cases_str},
            -- This part handles the unmapped tags
            MAX(CASE WHEN "TagName" = 'Unmapped' THEN "TagIndex" END) AS "Unmapped_TagIndex",
            MAX(CASE WHEN "TagName" = 'Unmapped' THEN "Val" END) AS "Unmapped_Value"
        FROM
            MappedData
        GROUP BY
            -- Group by the truncated minute timestamp
            "DateAndTime"
        ORDER BY
            "DateAndTime" DESC;
        """
        
        pg_cursor.execute(insert_data_query)
        pg_conn.commit()
        print(f"✅ Data successfully inserted into '{PG_TRANSFORMED_TABLE}'.")

    except psycopg2.Error as e:
        print(f"❌ PostgreSQL connection or query failed. Error: {e}", file=sys.stderr)
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}", file=sys.stderr)
    finally:
        if pg_conn:
            pg_conn.close()
        print("\nScript finished.")

if __name__ == "__main__":
    refresh_minute_data_table()
