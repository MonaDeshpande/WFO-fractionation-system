import streamlit as st
import psycopg2
import psycopg2.extras # This is the crucial new import for bulk inserts
import os
import pandas as pd
import threading
import time
import sys
import pyodbc
import re
from psycopg2.errors import DuplicateDatabase

# ==============================================================================
#  _           _           _      _
# | |__   ___| |_ _  _ __ _| |_ ___| |__
# | '_ \ / _ \ __| | | |/ _` | __/ __| '_ \
# | |_) |  __/ |_| |_| | (_| | |_\__ \ | | |
# |_.__/ \___|\__|\__,_|\__,_|\__|___/_| |_|
#
# Edit these values to configure your application
# ==============================================================================
CONFIG = {
    "SQL_SERVER_NAME": r"DESKTOP-DG1Q26L\SQLEXPRESS",
    "SQL_DB_NAME": "JSCPL",
    "SQL_TABLE_NAME": "dbo.FloatTable",
    "PG_HOST": "localhost",
    "PG_PORT": "5432",
    "PG_USER": "postgres",
    "PG_DB_NAME": "scada_data",
    "PG_TABLE_NAME": "scada_data"
}

# The dictionary of TagIndex numbers to their friendly names.
# The sync process will IGNORE any tags from the SQL data that are not in this list.
TAG_MAPPING = {
    251: "TI-31", 253: "TI-32", 254: "TI-33", 255: "TI-35", 256: "TI-35-A",
    257: "TI-36", 258: "TI-37", 259: "TI-38", 260: "TI-39", 261: "TI-40",
    270: "TI-41", 271: "TI-42", 273: "TI-43", 274: "TI-44", 275: "TI-45",
    272: "TI-42A", 279: "TI-54", 199: "TI-107", 201: "TI-109", 296: "TI-73A",
    297: "TI-73B", 280: "TI-55", 154: "PTT-03", 149: "PTB-03", 122: "LT-O5",
    123: "LT-06", 28: "FT-01", 46: "FT-07", 63: "FT-10", 37: "FT-04",
    225: "Tag-225", 226: "Tag-226", 227: "Tag-227", 228: "Tag-228",
    229: "Tag-229", 230: "Tag-230", 231: "Tag-231", 232: "Tag-232",
    233: "Tag-233", 234: "Tag-234", 235: "Tag-235"
}

# ==============================================================================
#  _  _           _
# | || | __ _ _ __ | | __
# | || |/ _` | '_ \| |/ /
# |__  | (_| | | | |  <
#   |_|\__,_|_| |_|_|\_\
#
#  Streamlit App Layout and Logic
# ==============================================================================

# Global State for Streamlit session
if 'sync_running' not in st.session_state:
    st.session_state.sync_running = False
if 'sync_thread' not in st.session_state:
    st.session_state.sync_thread = None

# Streamlit Page Config
st.set_page_config(page_title="SCADA SQL to PostgreSQL Sync", layout="centered")
st.title("🔄 SCADA SQL to PostgreSQL Sync Tool")
st.markdown("---")

# ---------------------------
# Sidebar Input Form - Displaying Config
# ---------------------------
st.sidebar.header("🔧 Sync Settings")
with st.sidebar.form("connection_form"):
    st.subheader("SQL Server Details")
    sql_server = st.text_input("SQL Server Name", value=CONFIG["SQL_SERVER_NAME"], help="The server where your SCADA data is located.")
    sql_db_name = st.text_input("SQL Server Database Name", value=CONFIG["SQL_DB_NAME"], help="The name of the database that contains your SCADA data.")
    sql_table_name = st.text_input("SQL Server Data Table Name", value=CONFIG["SQL_TABLE_NAME"], help="The name of the table that contains your SCADA data.")

    st.subheader("PostgreSQL Details")
    host = st.text_input("Host", value=CONFIG["PG_HOST"], help="The hostname of your PostgreSQL server.")
    port = st.text_input("Port", value=CONFIG["PG_PORT"], help="The port for your PostgreSQL server (default is 5432).")
    user = st.text_input("Username", value=CONFIG["PG_USER"], help="The username for PostgreSQL access.")
    password = st.text_input("Password", type="password", help="The password for the PostgreSQL user.")
    db_name = st.text_input("PostgreSQL Database Name", value=CONFIG["PG_DB_NAME"], help="The name of the database to create or use.")
    pg_table_name = st.text_input("Target Table Name", value=CONFIG["PG_TABLE_NAME"], help="The name of the table to create or use for storing the data.")

    submitted = st.form_submit_button("✅ Save Settings and Initialize")

# ---------------------------
# DB Utility Functions
# ---------------------------
def _is_valid_db_name(name):
    """Checks if a string is a valid PostgreSQL database name."""
    return bool(re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name))

def create_database_if_not_exists(host, port, user, password, db_name):
    """Handles the creation of the database gracefully."""
    if not _is_valid_db_name(db_name):
        st.error(f"❌ Invalid PostgreSQL database name: `{db_name}`. Please use a simple name with only letters, numbers, and underscores (e.g., `scada_db`).")
        return False
    
    conn = None
    try:
        conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname="postgres")
        conn.autocommit = True
        cursor = conn.cursor()
        # Use quotes around the database name to handle potential case-sensitivity issues
        create_query = f'CREATE DATABASE "{db_name}"'
        st.info(f"ℹ️ Attempting to create PostgreSQL database `{db_name}`...")
        cursor.execute(create_query)
        st.success(f"✅ PostgreSQL database `{db_name}` created successfully.")
        return True
    
    except DuplicateDatabase:
        st.info(f"ℹ️ PostgreSQL database `{db_name}` already exists. Skipping creation.")
        return True
    except psycopg2.OperationalError as e:
        st.error(f"❌ Cannot connect to PostgreSQL to create the database. Please check your **Host, Port, Username, and Password**. Error: {e}")
        return False
    except Exception as e:
        st.error(f"❌ An unexpected error occurred while creating the PostgreSQL database. Error: {e}")
        return False
    finally:
        if conn:
            conn.close()

def create_pivoted_table_if_not_exists(host, port, user, password, db_name, table_name):
    """Connects to the specific database and creates the target table if it doesn't exist."""
    conn = None
    try:
        conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=db_name)
        cursor = conn.cursor()
        # Create columns with double quotes to handle non-standard characters and preserve case
        columns = ",\n".join([f'"{tag}" FLOAT' for tag in TAG_MAPPING.values()])
        create_query = f"""
        CREATE TABLE IF NOT EXISTS "{table_name}" (
            "DateAndTime" TIMESTAMP PRIMARY KEY,
            {columns}
        );
        """
        st.info(f"ℹ️ Attempting to create table `{table_name}`...")
        cursor.execute(create_query)
        conn.commit()
        st.success(f"✅ Table `{table_name}` verified/created successfully.")
        return True
    except psycopg2.OperationalError as e:
        st.error(f"❌ Could not connect to database `{db_name}` to create the table. Please verify the database exists and your credentials are correct. Error: {e}")
        return False
    except Exception as e:
        st.error(f"❌ An error occurred during table creation. Error: {e}")
        return False
    finally:
        if conn:
            cursor.close()
            conn.close()


# ---------------------------
# Sync Function (Diagnostic Mode)
# ---------------------------
def sync_continuously(config, tag_mapping, password):
    """The main continuous sync loop, now with bulk insertion."""
    while st.session_state.sync_running:
        sql_conn, pg_conn = None, None
        try:
            # Step 1: Connect to SQL Server
            conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={config["SQL_SERVER_NAME"]};DATABASE={config["SQL_DB_NAME"]};Trusted_Connection=yes;'
            st.info("ℹ️ Connecting to SQL Server...")
            sql_conn = pyodbc.connect(conn_str)
            st.success(f"✅ Connected to SQL Server at `{config['SQL_SERVER_NAME']}`.")
            sql_cursor = sql_conn.cursor()

            # Step 2: Determine the synchronization start timestamp
            st.info("ℹ️ Connecting to PostgreSQL to get the latest timestamp...")
            pg_conn = psycopg2.connect(host=config['PG_HOST'], port=config['PG_PORT'], user=config['PG_USER'], password=password, dbname=config['PG_DB_NAME'])
            pg_cursor = pg_conn.cursor()
            
            # Use double quotes for the table name for robustness
            pg_cursor.execute(f'SELECT MAX("DateAndTime") FROM "{config["PG_TABLE_NAME"]}";')
            latest_timestamp_pg = pg_cursor.fetchone()[0]

            sync_start_timestamp = None
            if latest_timestamp_pg is None:
                st.info("⚠️ Destination table is empty. Fetching the latest timestamp from SQL Server for the initial sync.")
                sql_cursor.execute(f"SELECT MAX(DateAndTime) FROM {config['SQL_TABLE_NAME']};")
                latest_timestamp_sql = sql_cursor.fetchone()[0]
                sync_start_timestamp = latest_timestamp_sql
            else:
                st.info(f"ℹ️ Latest timestamp in PostgreSQL is: `{latest_timestamp_pg}`. Syncing data newer than this.")
                sync_start_timestamp = latest_timestamp_pg

            # Step 3: Query SQL Server for new data
            sql_query = f"""
            SELECT DateAndTime, TagIndex, Val
            FROM {config['SQL_TABLE_NAME']}
            WHERE DateAndTime > ?
            ORDER BY DateAndTime ASC;
            """
            st.info(f"ℹ️ Fetching new data from SQL Server (rows newer than `{sync_start_timestamp}`).")
            sql_cursor.execute(sql_query, sync_start_timestamp)
            rows = sql_cursor.fetchall()
            st.info(f"📁 Fetched {len(rows)} rows from SQL Server. Now filtering for relevant tags.")

            if not rows:
                st.info("📁 No new data found in SQL Server. Waiting for new data...")
            else:
                # Step 4: Process and Filter the new data
                df = pd.DataFrame(rows, columns=["DateAndTime", "TagIndex", "Val"])
                df["TAG"] = df["TagIndex"].map(tag_mapping)
                df.dropna(subset=["TAG"], inplace=True)
                
                rows_after_filter = len(df)
                st.info(f"ℹ️ Filtered down to {rows_after_filter} rows after applying tag mapping. Now pivoting the data.")

                if rows_after_filter == 0:
                    st.warning("⚠️ No data found with matching tags to insert. All fetched rows were ignored.")
                else:
                    # Pivot the data to a wide format
                    pivot_df = df.pivot_table(index="DateAndTime", columns="TAG", values="Val", aggfunc='first').reset_index()

                    # Step 5: Bulk insert new data into PostgreSQL
                    st.info(f"ℹ️ Inserting {len(pivot_df)} new row(s) into PostgreSQL.")
                    
                    # Prepare data for bulk insert
                    columns = ['"DateAndTime"'] + [f'"{col}"' for col in pivot_df.columns if col != 'DateAndTime']
                    values_to_insert = [tuple(row) for row in pivot_df.to_numpy()]
                    
                    # Use execute_values for efficient and safe bulk insertion
                    insert_query = f"INSERT INTO \"{config['PG_TABLE_NAME']}\" ({', '.join(columns)}) VALUES %s ON CONFLICT (\"DateAndTime\") DO NOTHING;"
                    
                    psycopg2.extras.execute_values(
                        pg_cursor,
                        insert_query,
                        values_to_insert,
                        template=None,
                        page_size=100
                    )
                    
                    pg_conn.commit()
                    st.success(f"✅ Synced {pg_cursor.rowcount} new row(s) from SQL Server to PostgreSQL.")
        
        except pyodbc.Error as e:
            st.error(f"❌ SQL Server connection failed. Check your server name, database, and network connectivity. Error: {e}")
            st.session_state.sync_running = False
        except psycopg2.OperationalError as e:
            st.error(f"❌ PostgreSQL connection failed. Check your credentials and that the service is running. Error: {e}")
            st.session_state.sync_running = False
        except Exception as e:
            st.error(f"❌ A general error occurred: {e}. Stopping sync.")
            st.session_state.sync_running = False
        finally:
            if sql_conn: sql_conn.close()
            if pg_conn: pg_conn.close()

        if st.session_state.sync_running:
            st.info("💤 Waiting for 60 seconds before the next sync cycle...")
            time.sleep(60)

# ==============================================================================
# Main App Flow
# ==============================================================================
# --- Dependency Check ---
try:
    import pyodbc
    import psycopg2
except ImportError:
    st.error("❌ The required Python libraries ('pyodbc' or 'psycopg2') are not installed. Please run `pip install pyodbc psycopg2-binary` from your terminal.")
    st.stop()

# --- DB Setup ---
if submitted:
    st.session_state.pg_password = password
    if create_database_if_not_exists(host, port, user, st.session_state.pg_password, db_name):
        create_pivoted_table_if_not_exists(host, port, user, st.session_state.pg_password, db_name, pg_table_name)
    # Re-fetch config from the form for the main part of the app
    CONFIG['SQL_SERVER_NAME'] = sql_server
    CONFIG['SQL_DB_NAME'] = sql_db_name
    CONFIG['SQL_TABLE_NAME'] = sql_table_name
    CONFIG['PG_HOST'] = host
    CONFIG['PG_PORT'] = port
    CONFIG['PG_USER'] = user
    CONFIG['PG_DB_NAME'] = db_name
    CONFIG['PG_TABLE_NAME'] = pg_table_name
    

# --- Data Preview ---
st.header("🔍 SQL Server Data Preview")
st.info("ℹ️ This is a diagnostic preview to confirm the connection and data are correct.")
try:
    sql_conn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={CONFIG["SQL_SERVER_NAME"]};DATABASE={CONFIG["SQL_DB_NAME"]};Trusted_Connection=yes;')
    sql_query = f"SELECT TOP 500 DateAndTime, TagIndex, Val FROM {CONFIG['SQL_TABLE_NAME']} ORDER BY DateAndTime DESC;"
    preview_df = pd.read_sql(sql_query, sql_conn)
    st.dataframe(preview_df)
    sql_conn.close()
    st.info("✅ Preview successfully fetched. The data above shows the most recent 500 rows from your SQL Server.")
except pyodbc.Error as e:
    st.warning(f"⚠️ Could not connect to SQL Server for preview. Check your connection details in the code and your network. Error: {e}")
except Exception as e:
    st.warning(f"⚠️ An unexpected error occurred while fetching data for preview. Error: {e}")

# --- Sync Controls ---
if st.button("🚀 Start Sync") and not st.session_state.sync_running:
    if not 'pg_password' in st.session_state:
        st.error("❌ Please enter your settings and click 'Save Settings' before starting the sync.")
    else:
        st.session_state.sync_running = True
        st.session_state.sync_thread = threading.Thread(target=sync_continuously, args=(CONFIG, TAG_MAPPING, st.session_state.pg_password))
        st.session_state.sync_thread.start()
        st.info("⏳ Sync started...")

if st.button("🛑 Stop Sync") and st.session_state.sync_running:
    st.session_state.sync_running = False
    st.warning("🛑 Sync stopped.")
