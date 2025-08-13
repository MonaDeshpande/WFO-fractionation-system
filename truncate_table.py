import psycopg2
import sys

# ==============================================================================
# CONFIGURATION
# ==============================================================================
PG_HOST = "localhost"
PG_PORT = "5432"
PG_USER = "postgres"
PG_PASSWORD = "ADMIN"  # <-- IMPORTANT: Add your PostgreSQL password here
PG_DB_NAME = "scada_data_analysis"
PG_TRANSFORMED_TABLE = "wide_scada_data"

def truncate_transformed_table():
    """
    Connects to PostgreSQL and truncates the transformed data table.
    This operation removes all rows but keeps the table's structure.
    """
    pg_conn = None
    try:
        print(f"Connecting to PostgreSQL to truncate table '{PG_TRANSFORMED_TABLE}'...")
        pg_conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD, dbname=PG_DB_NAME)
        pg_cursor = pg_conn.cursor()
        print("✅ Successfully connected to PostgreSQL.")

        # --- Truncate the Transformed Table ---
        print(f"\n--- Truncating table '{PG_TRANSFORMED_TABLE}' ---")
        truncate_table_query = f"""
        TRUNCATE TABLE "{PG_TRANSFORMED_TABLE}";
        """
        pg_cursor.execute(truncate_table_query)
        pg_conn.commit()
        print(f"✅ All data has been removed from '{PG_TRANSFORMED_TABLE}'.")

    except psycopg2.Error as e:
        print(f"❌ PostgreSQL connection or query failed. Error: {e}", file=sys.stderr)
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}", file=sys.stderr)
    finally:
        if pg_conn:
            pg_conn.close()
        print("\nScript finished.")

if __name__ == "__main__":
    # Add a confirmation prompt to prevent accidental data deletion
    confirmation = input(f"Are you sure you want to clear all data from '{PG_TRANSFORMED_TABLE}'? (yes/no): ")
    if confirmation.lower() == 'yes':
        truncate_transformed_table()
    else:
        print("Operation cancelled.")
