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
PG_TRANSFORMED_TABLE = "wide_scada_data"  # This is now a physical table
TAGS_CSV_FILE = "tags.csv"

def create_and_populate_physical_table():
    """
    Connects to PostgreSQL, creates the tag mapping table, populates it from a CSV,
    and then creates a new physical table with the pivoted data.
    """
    pg_conn = None
    try:
        print("Connecting to PostgreSQL to create and populate database objects...")
        pg_conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD, dbname=PG_DB_NAME)
        pg_cursor = pg_conn.cursor()
        print("✅ Successfully connected to PostgreSQL.")

        # --- Step 1: Create the Tag Mapping Table ---
        print(f"\n--- Creating mapping table '{PG_MAPPING_TABLE}' ---")
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
        
        # --- Step 3: Drop the existing transformed table if it exists ---
        print(f"\n--- Dropping existing table '{PG_TRANSFORMED_TABLE}' if it exists ---")
        drop_table_query = f"""
        DROP TABLE IF EXISTS "{PG_TRANSFORMED_TABLE}";
        """
        pg_cursor.execute(drop_table_query)
        pg_conn.commit()
        print(f"✅ Table '{PG_TRANSFORMED_TABLE}' dropped if it existed.")

        # --- Step 4: Dynamically generate and create the PIVOTED TABLE ---
        print(f"\n--- Creating dynamic transformed TABLE '{PG_TRANSFORMED_TABLE}' ---")
        
        # Build the dynamic CASE statements for the mapped tags
        pivot_cases = [f"""MAX(CASE WHEN "TagName" = '{tag}' THEN "Val" END) AS "{tag}" """ for tag_index, tag in tag_data]
        pivot_cases_str = ",\n             ".join(pivot_cases)

        create_table_query = f"""
        CREATE TABLE "{PG_TRANSFORMED_TABLE}" AS
        WITH MappedData AS (
            SELECT
                r."DateAndTime",
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
            "DateAndTime"
        ORDER BY
            "DateAndTime" DESC;
        """
        
        pg_cursor.execute(create_table_query)
        pg_conn.commit()
        print(f"✅ Physical table '{PG_TRANSFORMED_TABLE}' created successfully with {len(tag_data)} mapped columns.")

    except psycopg2.Error as e:
        print(f"❌ PostgreSQL connection or query failed. Error: {e}", file=sys.stderr)
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}", file=sys.stderr)
    finally:
        if pg_conn:
            pg_conn.close()
        print("\nScript finished.")

if __name__ == "__main__":
    create_and_populate_physical_table()