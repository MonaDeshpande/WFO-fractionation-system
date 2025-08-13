import psycopg2
import psycopg2.extensions
import pyodbc
import time
import sys
from datetime import datetime

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
PG_PASSWORD = "admin"  # <-- IMPORTANT: Add your PostgreSQL password here
PG_DB_NAME = "scada_data624"
PG_TABLE_NAME = "scada_data_streamlined624"

# --- New: Add your desired start date here in 'YYYY-MM-DD' format ---
# This will only be used if the PostgreSQL table is empty.
START_DATE = "2024-01-01" 
# ==============================================================================

def create_database_if_not_exists(pg_conn_no_db, db_name):
    """
    Creates a PostgreSQL database if it does not already exist.
    """
    old_isolation_level = pg_conn_no_db.isolation_level
    pg_conn_no_db.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = pg_conn_no_db.cursor()
    
    try:
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")
        exists = cursor.fetchone()
        if not exists:
            print(f"Database '{db_name}' not found. Creating it now...")
            cursor.execute(f"CREATE DATABASE {db_name};")
            print(f"✅ Database '{db_name}' created successfully.")
        else:
            print(f"Database '{db_name}' already exists.")
    except psycopg2.Error as e:
        print(f"❌ Failed to check or create database. Error: {e}", file=sys.stderr)
        raise
    finally:
        cursor.close()
        pg_conn_no_db.set_isolation_level(old_isolation_level)

def create_table_if_not_exists(pg_conn, table_name):
    """
    Creates the required table in the PostgreSQL database if it does not exist.
    """
    cursor = pg_conn.cursor()
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS "{table_name}" (
        "DateAndTime" TIMESTAMP NOT NULL,
        "TagIndex" INTEGER NOT NULL,
        "Val" TEXT,
        PRIMARY KEY ("DateAndTime", "TagIndex")
    );
    """
    try:
        cursor.execute(create_table_query)
        pg_conn.commit()
        print(f"✅ Table '{table_name}' checked/created successfully.")
    except psycopg2.Error as e:
        print(f"❌ Failed to create table. Error: {e}", file=sys.stderr)
        raise
    finally:
        cursor.close()

# ==============================================================================
# Main Sync Logic
# ==============================================================================
def run_sync():
    """
    Main function to continuously sync data from SQL Server to PostgreSQL.
    """
    sql_conn, pg_conn = None, None

    try:
        print("Starting continuous data sync...")
        
        # --- Step 0: Ensure PostgreSQL database and table exist ---
        print("--- Setting up PostgreSQL database and table... ---")
        try:
            # Connect to default 'postgres' database to create the new one
            pg_conn_no_db = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD, dbname='postgres')
            create_database_if_not_exists(pg_conn_no_db, PG_DB_NAME)
            pg_conn_no_db.close()
            
            # Now connect to the target database and create the table
            pg_conn_temp = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD, dbname=PG_DB_NAME)
            create_table_if_not_exists(pg_conn_temp, PG_TABLE_NAME)
            pg_conn_temp.close()
        except psycopg2.Error as e:
            print(f"❌ Initial setup failed. Error: {e}", file=sys.stderr)
            sys.exit(1)
            
        print("--- PostgreSQL setup complete. Entering sync loop... ---")

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

                # --- Step 3: Find the latest timestamp in PostgreSQL or use START_DATE ---
                pg_cursor.execute(f'SELECT "DateAndTime" FROM "{PG_TABLE_NAME}" ORDER BY "DateAndTime" DESC LIMIT 1;')
                result = pg_cursor.fetchone()
                latest_timestamp_pg = result[0] if result else None
                
                if latest_timestamp_pg is None:
                    # Use the user-defined START_DATE if the table is empty
                    latest_timestamp_pg = datetime.strptime(START_DATE, '%Y-%m-%d')
                    sql_query = f'SELECT "DateAndTime", "TagIndex", "Val" FROM {SQL_TABLE_NAME} WHERE "DateAndTime" >= ? ORDER BY "DateAndTime" ASC;'
                    print(f"⚠️ PostgreSQL table is empty. Fetching all data from SQL Server since {START_DATE}.")
                    sql_cursor.execute(sql_query, latest_timestamp_pg)
                else:
                    sql_query = f'SELECT "DateAndTime", "TagIndex", "Val" FROM {SQL_TABLE_NAME} WHERE "DateAndTime" > ? ORDER BY "DateAndTime" ASC;'
                    print(f"ℹ️ Fetching new data from SQL Server since {latest_timestamp_pg}.")
                    sql_cursor.execute(sql_query, latest_timestamp_pg)

                rows = sql_cursor.fetchall()
                print(f"📁 Fetched {len(rows)} new row(s) from SQL Server.")

                # --- Step 4: Insert new data into PostgreSQL ---
                if rows:
                    insert_query = f"""
                    INSERT INTO "{PG_TABLE_NAME}" ("DateAndTime", "TagIndex", "Val")
                    VALUES (%s, %s, %s)
                    ON CONFLICT ("DateAndTime", "TagIndex") DO NOTHING;
                    """
                    # We will now explicitly cast the 'Val' column to a string to avoid data type mismatch
                    rows_for_insert = [(row[0], row[1], str(row[2])) for row in rows]
                    
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