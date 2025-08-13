import psycopg2
import sys

# ==============================================================================
# CONFIGURATION
# ==============================================================================
PG_HOST = "localhost"
PG_PORT = "5432"
PG_USER = "postgres"
PG_PASSWORD = "ADMIN"  # <-- Add your PostgreSQL password here
PG_DB_NAME = "scada_data_analysis"
PG_RAW_TABLE = "raw_data"
PG_MAPPING_TABLE = "tag_mapping"
PG_TRANSFORMED_TABLE = "wide_scada_data"

def append_new_data_to_wide_table():
    """
    Connects to PostgreSQL and appends new, transformed data to the wide table.
    """
    pg_conn = None
    try:
        print("Connecting to PostgreSQL to append new data...")
        pg_conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD, dbname=PG_DB_NAME)
        pg_cursor = pg_conn.cursor()
        print("✅ Successfully connected to PostgreSQL.")

        # --- Step 1: Get the last timestamp from the wide table ---
        print("\n--- Getting last timestamp from wide table ---")
        get_last_timestamp_query = f"""
        SELECT MAX("DateAndTime") FROM "{PG_TRANSFORMED_TABLE}";
        """
        pg_cursor.execute(get_last_timestamp_query)
        last_timestamp = pg_cursor.fetchone()[0]
        
        if last_timestamp is None:
            # If the wide table is empty, we process all data
            print("❗ Wide table is empty. Processing all available data.")
            last_timestamp = "1970-01-01 00:00:00"  # A very old timestamp
        else:
            print(f"✅ Found last processed timestamp: {last_timestamp}")
        
        # --- Step 2: Get the list of mapped tags from the mapping table ---
        print("\n--- Getting mapped tags to build dynamic query ---")
        get_mapped_tags_query = f"""
        SELECT "TagIndex", "TagName" FROM "{PG_MAPPING_TABLE}";
        """
        pg_cursor.execute(get_mapped_tags_query)
        tag_data = pg_cursor.fetchall()

        if not tag_data:
            print("❌ Error: No tags found in the mapping table. Cannot proceed.")
            return

        # --- Step 3: Dynamically generate and execute the INSERT query ---
        print(f"\n--- Appending new transformed data to '{PG_TRANSFORMED_TABLE}' ---")
        
        # Build the dynamic CASE statements for the mapped tags
        pivot_cases = [f"""MAX(CASE WHEN "TagName" = '{tag}' THEN "Val" END) AS "{tag}" """ for tag_index, tag in tag_data]
        pivot_cases_str = ",\n             ".join(pivot_cases)

        insert_new_data_query = f"""
        WITH NewRawData AS (
            SELECT
                r."DateAndTime",
                COALESCE(m."TagName", 'Unmapped') AS "TagName",
                r."Val",
                r."TagIndex"
            FROM
                "{PG_RAW_TABLE}" AS r
            LEFT JOIN
                "{PG_MAPPING_TABLE}" AS m ON r."TagIndex" = m."TagIndex"
            WHERE
                r."DateAndTime" > '{last_timestamp}'
        )
        INSERT INTO "{PG_TRANSFORMED_TABLE}" ("DateAndTime", {", ".join([f'"{tag}"' for tag_index, tag in tag_data])}, "Unmapped_TagIndex", "Unmapped_Value")
        SELECT
            "DateAndTime",
            {pivot_cases_str},
            MAX(CASE WHEN "TagName" = 'Unmapped' THEN "TagIndex" END) AS "Unmapped_TagIndex",
            MAX(CASE WHEN "TagName" = 'Unmapped' THEN "Val" END) AS "Unmapped_Value"
        FROM
            NewRawData
        GROUP BY
            "DateAndTime"
        ORDER BY
            "DateAndTime" DESC;
        """
        
        pg_cursor.execute(insert_new_data_query)
        pg_conn.commit()
        print(f"✅ Successfully appended {pg_cursor.rowcount} new rows to '{PG_TRANSFORMED_TABLE}'.")

    except psycopg2.Error as e:
        print(f"❌ PostgreSQL connection or query failed. Error: {e}", file=sys.stderr)
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}", file=sys.stderr)
    finally:
        if pg_conn:
            pg_conn.close()
        print("\nScript finished.")

if __name__ == "__main__":
    append_new_data_to_wide_table()