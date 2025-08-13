import psycopg2
import sys

# ==============================================================================
# CONFIGURATION
# ==============================================================================
PG_HOST = "localhost"
PG_PORT = "5432"
PG_USER = "postgres"
PG_PASSWORD = "ADMIN"  # Add your PostgreSQL password here
PG_DB_NAME = "scada_data"
PG_RAW_TABLE = "scada_data_streamlined"
PG_MAPPING_TABLE = "tag_mapping"
PG_TRANSFORMED_VIEW = "wide_scada_data"

def create_database_objects():
    """
    Connects to PostgreSQL and creates the necessary tables and view.
    """
    pg_conn = None
    try:
        print("Connecting to PostgreSQL to create database objects...")
        pg_conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD, dbname=PG_DB_NAME)
        pg_cursor = pg_conn.cursor()
        print("✅ Successfully connected to PostgreSQL.")

        # --- Step 1: Create the Raw Data Table ---
        print(f"\n--- Creating raw data table '{PG_RAW_TABLE}' ---")
        create_raw_table_query = f"""
        CREATE TABLE IF NOT EXISTS "{PG_RAW_TABLE}" (
            "DateAndTime" TIMESTAMP,
            "TagIndex" INTEGER,
            "Val" FLOAT
        );
        """
        pg_cursor.execute(create_raw_table_query)
        print(f"✅ Raw data table '{PG_RAW_TABLE}' created or verified.")

        # --- Step 2: Create the Tag Mapping Table ---
        print(f"\n--- Creating mapping table '{PG_MAPPING_TABLE}' ---")
        create_mapping_table_query = f"""
        CREATE TABLE IF NOT EXISTS "{PG_MAPPING_TABLE}" (
            "TagIndex" INTEGER PRIMARY KEY,
            "TagName" VARCHAR(255) UNIQUE
        );
        """
        pg_cursor.execute(create_mapping_table_query)
        print(f"✅ Mapping table '{PG_MAPPING_TABLE}' created or verified.")

        # The view will be created dynamically by the second script
        pg_conn.commit()
        print("\nDatabase objects successfully created or verified.")

    except psycopg2.Error as e:
        print(f"❌ PostgreSQL connection or query failed. Error: {e}", file=sys.stderr)
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}", file=sys.stderr)
    finally:
        if pg_conn:
            pg_conn.close()
        print("\nScript finished.")

if __name__ == "__main__":
    create_database_objects()