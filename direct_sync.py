import psycopg2
import pyodbc
import time
import sys

# ==============================================================================
# CONFIGURATION
# Fill in your connection details here
# ==============================================================================
SQL_SERVER_NAME = r"DESKTOP-DG1Q26L\SQLEXPRESS"
SQL_DB_NAME = "JSCPL"
SQL_TABLE_NAME = "dbo.FloatTable"
PG_HOST = "localhost"
PG_PORT = "5432"
PG_USER = "postgres"
PG_PASSWORD = "ADMIN" # <-- IMPORTANT: Add your PostgreSQL password here
PG_DB_NAME = "scada_data624"
PG_TABLE_NAME = "scada_data_streamlined624"

# ==============================================================================
# Main Sync Logic
# This script will run forever and sync data every 60 seconds.
# ==============================================================================
def run_sync():
    """
    Main function to continuously sync data from SQL Server to PostgreSQL.
    """
    sql_conn, pg_conn = None, None

    try:
        print("Starting continuous data sync...")
        while True:
            try:
                # --- Step 1: Connect to SQL Server ---
                conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SQL_SERVER_NAME};DATABASE={SQL_DB_NAME};Trusted_Connection=yes;'
                print(f"Connecting to SQL Server at {SQL_SERVER_NAME}...")
                sql_conn = pyodbc.connect(conn_str)
                sql_cursor = sql_conn.cursor()
                print("✅ Successfully connected to SQL Server.")

                # --- Step 2: Connect to PostgreSQL ---
                print(f"Connecting to PostgreSQL database {PG_DB_NAME}...")
                pg_conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD, dbname=PG_DB_NAME)
                pg_cursor = pg_conn.cursor()
                print("✅ Successfully connected to PostgreSQL.")

                # --- Step 3: Find the latest timestamp in PostgreSQL ---
                pg_cursor.execute(f'SELECT "DateAndTime" FROM "{PG_TABLE_NAME}" ORDER BY "DateAndTime" DESC LIMIT 1;')
                result = pg_cursor.fetchone()
                latest_timestamp_pg = result[0] if result else None
                print(f"Latest timestamp in PostgreSQL: {latest_timestamp_pg}")

                # --- Step 4: Fetch new data from SQL Server ---
                if latest_timestamp_pg is None:
                    sql_query = f'SELECT "DateAndTime", "TagIndex", "Val" FROM {SQL_TABLE_NAME} ORDER BY "DateAndTime" ASC;'
                    print("⚠️ PostgreSQL table is empty. Fetching all data from SQL Server.")
                    sql_cursor.execute(sql_query)
                else:
                    sql_query = f'SELECT "DateAndTime", "TagIndex", "Val" FROM {SQL_TABLE_NAME} WHERE "DateAndTime" > ? ORDER BY "DateAndTime" ASC;'
                    print(f"ℹ️ Fetching new data from SQL Server since {latest_timestamp_pg}.")
                    sql_cursor.execute(sql_query, latest_timestamp_pg)

                rows = sql_cursor.fetchall()
                print(f"📁 Fetched {len(rows)} new row(s) from SQL Server.")

                # --- Step 5: Insert new data into PostgreSQL ---
                if rows:
                    insert_query = f"""
                    INSERT INTO "{PG_TABLE_NAME}" ("DateAndTime", "TagIndex", "Val")
                    VALUES (%s, %s, %s)
                    ON CONFLICT ("DateAndTime", "TagIndex") DO NOTHING;
                    """
                    # We will now explicitly cast the 'Val' column to a string to avoid data type mismatch
                    rows_for_insert = [(row[0], row[1], str(row[2])) for row in rows]
                    
                    # Using executemany which is more reliable than execute_values if 'extras' is missing
                    pg_cursor.executemany(insert_query, rows_for_insert)
                    pg_conn.commit()
                    print(f"✅ Successfully inserted {pg_cursor.rowcount} row(s) into PostgreSQL.")
                else:
                    print("💤 No new data found. Waiting...")

            except pyodbc.Error as e:
                print(f"❌ SQL Server connection failed. Error: {e}", file=sys.stderr)
            except psycopg2.Error as e:
                print(f"❌ PostgreSQL connection failed. Error: {e}", file=sys.stderr)
            except Exception as e:
                print(f"❌ An unexpected error occurred: {e}", file=sys.stderr)
            finally:
                if sql_conn: sql_conn.close()
                if pg_conn: pg_conn.close()

            print("--- Waiting for 60 seconds before next sync cycle... ---")
            time.sleep(60)

    except KeyboardInterrupt:
        print("\nSync process stopped by user.")
        
if __name__ == "__main__":
    run_sync()

