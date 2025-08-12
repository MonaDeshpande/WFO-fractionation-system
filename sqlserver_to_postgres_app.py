import streamlit as st
import psycopg2
import psycopg2.extras
import os
import pandas as pd
import threading
import time
import sys
import pyodbc
import re
from psycopg2.errors import DuplicateDatabase

# ==============================================================================
# CORRECT SESSION STATE INITIALIZATION
# This is the most important part to fix your error.
# The `if ... not in st.session_state` check ensures these variables
# always exist before the app tries to use them.
# ==============================================================================
if 'sync_running' not in st.session_state:
    st.session_state.sync_running = False
if 'sync_thread' not in st.session_state:
    st.session_state.sync_thread = None
if 'sync_stop_event' not in st.session_state:
    st.session_state.sync_stop_event = None
if 'pg_password' not in st.session_state:
    st.session_state.pg_password = None
if 'config' not in st.session_state:
    st.session_state.config = {
        "SQL_SERVER_NAME": r"DESKTOP-DG1Q26L\SQLEXPRESS",
        "SQL_DB_NAME": "JSCPL",
        "SQL_TABLE_NAME": "dbo.FloatTable",
        "PG_HOST": "localhost",
        "PG_PORT": "5432",
        "PG_USER": "postgres",
        "PG_DB_NAME": "scada_data",
        "PG_TABLE_NAME": "scada_data_streamlined"
    }

# ==============================================================================
#       _               _           _   _
#  | |__   ___| |_ _  _ __ _| |_ ___| |__
#  | '_ \ / _ \ __| | | |/ _` | __/ __| '_ \
#  | |_) |  __/ |_| |_| | (_| | |_\__ \ | | |
#  |_.__/ \___|\__|\__,_|\__,_|\__|___/_| |_|
#
# Edit these values to configure your application
# ==============================================================================
# CONFIG is now handled by session state
# A copy of the initial config is still here for reference
DEFAULT_CONFIG = {
    "SQL_SERVER_NAME": r"DESKTOP-DG1Q26L\SQLEXPRESS",
    "SQL_DB_NAME": "JSCPL",
    "SQL_TABLE_NAME": "dbo.FloatTable",
    "PG_HOST": "localhost",
    "PG_PORT": "5432",
    "PG_USER": "postgres",
    "PG_DB_NAME": "scada_data",
    "PG_TABLE_NAME": "scada_data_streamlined"
}

# ==============================================================================
#       _  _         _
#  | || | __ _ _ __ | | __
#  | || |/ _` | '_ \| |/ /
#  |__  | (_| | | | |  <
#       |_|\__,_|_| |_|_|\_\
#
#   Streamlit App Layout and Logic
# ==============================================================================

# Streamlit Page Config
st.set_page_config(page_title="SCADA SQL to PostgreSQL Sync", layout="wide")
st.title("🔄 SCADA SQL to PostgreSQL Sync Tool (Streamlined)")
st.markdown("---")

# ---------------------------
# Sidebar Input Form - Displaying Config
# ---------------------------
st.sidebar.header("🔧 Sync Settings")
with st.sidebar.form("connection_form"):
    st.subheader("SQL Server Details")
    sql_server = st.text_input("SQL Server Name", value=st.session_state.config["SQL_SERVER_NAME"], help="The server where your SCADA data is located.")
    sql_db_name = st.text_input("SQL Server Database Name", value=st.session_state.config["SQL_DB_NAME"], help="The name of the database that contains your SCADA data.")
    sql_table_name = st.text_input("SQL Server Data Table Name", value=st.session_state.config["SQL_TABLE_NAME"], help="The name of the table that contains your SCADA data.")

    st.subheader("PostgreSQL Details")
    host = st.text_input("Host", value=st.session_state.config["PG_HOST"], help="The hostname of your PostgreSQL server.")
    port = st.text_input("Port", value=st.session_state.config["PG_PORT"], help="The port for your PostgreSQL server (default is 5432).")
    user = st.text_input("Username", value=st.session_state.config["PG_USER"], help="The username for PostgreSQL access.")
    password = st.text_input("Password", type="password", help="The password for the PostgreSQL user.")
    db_name = st.text_input("PostgreSQL Database Name", value=st.session_state.config["PG_DB_NAME"], help="The name of the database to create or use.")
    pg_table_name = st.text_input("Target Table Name", value=st.session_state.config["PG_TABLE_NAME"], help="The name of the table to create or use for storing the data.")

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

def create_target_table_if_not_exists(host, port, user, password, db_name, table_name):
    """
    Connects to the specific database and creates a target table
    with only the requested columns.
    """
    conn = None
    try:
        conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=db_name)
        cursor = conn.cursor()

        create_query = f"""
        CREATE TABLE IF NOT EXISTS "{table_name}" (
            "DateAndTime" TIMESTAMP NOT NULL,
            "TagIndex" INTEGER NOT NULL,
            "Val" FLOAT,
            PRIMARY KEY ("DateAndTime", "TagIndex") -- Composite key for uniqueness
        );
        """
        st.info(f"ℹ️ Attempting to create streamlined data table `{table_name}`...")
        cursor.execute(create_query)
        conn.commit()
        st.success(f"✅ Streamlined data table `{table_name}` verified/created successfully.")
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
# Correct Sync Function (THREAD-SAFE and Robust)
# ---------------------------
def sync_continuously_correct(config, password, stop_event):
    """The main continuous sync loop, now thread-safe and more robust."""
    while not stop_event.is_set():
        sql_conn, pg_conn = None, None
        try:
            # Step 1: Connect to SQL Server
            conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={config["SQL_SERVER_NAME"]};DATABASE={config["SQL_DB_NAME"]};Trusted_Connection=yes;'
            st.toast("ℹ️ Connecting to SQL Server...")
            sql_conn = pyodbc.connect(conn_str)
            st.toast(f"✅ Connected to SQL Server at `{config['SQL_SERVER_NAME']}`.")
            sql_cursor = sql_conn.cursor()

            # Step 2: Determine the synchronization start timestamp
            st.toast("ℹ️ Connecting to PostgreSQL to get the latest timestamp...")
            pg_conn = psycopg2.connect(host=config['PG_HOST'], port=config['PG_PORT'], user=config['PG_USER'], password=password, dbname=config['PG_DB_NAME'])
            pg_cursor = pg_conn.cursor()

            pg_cursor.execute(f'SELECT "DateAndTime", "TagIndex" FROM "{config["PG_TABLE_NAME"]}" ORDER BY "DateAndTime" DESC LIMIT 1;')
            result = pg_cursor.fetchone()
            latest_timestamp_pg = result[0] if result else None
            
            if latest_timestamp_pg is None:
                sql_query = f"""
                SELECT "DateAndTime", "TagIndex", "Val"
                FROM {config['SQL_TABLE_NAME']}
                ORDER BY "DateAndTime" ASC;
                """
                st.toast("⚠️ Destination table is empty. Performing an initial sync.")
                sql_cursor.execute(sql_query)
                rows = sql_cursor.fetchall()
            else:
                sql_query = f"""
                SELECT "DateAndTime", "TagIndex", "Val"
                FROM {config['SQL_TABLE_NAME']}
                WHERE "DateAndTime" > ?
                ORDER BY "DateAndTime" ASC;
                """
                st.toast(f"ℹ️ Latest timestamp in PostgreSQL is: `{latest_timestamp_pg}`. Syncing data newer than this.")
                sql_cursor.execute(sql_query, latest_timestamp_pg)
                rows = sql_cursor.fetchall()

            st.toast(f"📁 Fetched {len(rows)} rows from SQL Server.")

            if not rows:
                st.toast("📁 No new data found in SQL Server. Waiting...")
            else:
                insert_query = f"""
                INSERT INTO "{config['PG_TABLE_NAME']}"
                ("DateAndTime", "TagIndex", "Val")
                VALUES (%s, %s, %s)
                ON CONFLICT ON CONSTRAINT {config['PG_TABLE_NAME']}_pkey DO NOTHING;
                """
                
                st.toast(f"ℹ️ Inserting {len(rows)} new row(s) into PostgreSQL.")
                
                psycopg2.extras.execute_values(
                    pg_cursor,
                    insert_query,
                    rows,
                    page_size=100
                )
                pg_conn.commit()
                st.toast(f"✅ Synced {len(rows)} new row(s) from SQL Server to PostgreSQL.")

        except pyodbc.Error as e:
            st.toast(f"❌ SQL Server connection failed. Error: {e}")
            stop_event.set()
        except psycopg2.OperationalError as e:
            st.toast(f"❌ PostgreSQL connection failed. Error: {e}")
            stop_event.set()
        except Exception as e:
            st.toast(f"❌ A general error occurred: {e}. Stopping sync.")
            stop_event.set()
        finally:
            if sql_conn: sql_conn.close()
            if pg_conn: pg_conn.close()

        if not stop_event.is_set():
            st.toast("💤 Waiting for 60 seconds before the next sync cycle...")
            stop_event.wait(60)

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
    st.session_state.config['SQL_SERVER_NAME'] = sql_server
    st.session_state.config['SQL_DB_NAME'] = sql_db_name
    st.session_state.config['SQL_TABLE_NAME'] = sql_table_name
    st.session_state.config['PG_HOST'] = host
    st.session_state.config['PG_PORT'] = port
    st.session_state.config['PG_USER'] = user
    st.session_state.config['PG_DB_NAME'] = db_name
    st.session_state.config['PG_TABLE_NAME'] = pg_table_name

    if create_database_if_not_exists(host, port, user, st.session_state.pg_password, db_name):
        create_target_table_if_not_exists(host, port, user, st.session_state.pg_password, db_name, st.session_state.config['PG_TABLE_NAME'])

# --- Data Preview Layout ---
col1, col2 = st.columns(2)

# --- SQL Server Data Preview ---
with col1:
    st.header("🔍 SQL Server Data Preview")
    st.info("ℹ️ Most recent 500 rows from your SQL Server.")
    try:
        sql_conn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={st.session_state.config["SQL_SERVER_NAME"]};DATABASE={st.session_state.config["SQL_DB_NAME"]};Trusted_Connection=yes;')
        sql_query = f"SELECT TOP 500 DateAndTime, TagIndex, Val FROM {st.session_state.config['SQL_TABLE_NAME']} ORDER BY DateAndTime DESC;"
        preview_df = pd.read_sql(sql_query, sql_conn)
        st.dataframe(preview_df)
        sql_conn.close()
        st.success("✅ SQL Server preview successfully fetched.")
    except pyodbc.Error as e:
        st.warning(f"⚠️ Could not connect to SQL Server for preview. Error: {e}")
    except Exception as e:
        st.warning(f"⚠️ An unexpected error occurred while fetching SQL Server preview. Error: {e}")

# --- PostgreSQL Data Preview ---
with col2:
    st.header("📋 PostgreSQL Data Preview")
    st.info("ℹ️ Most recent 500 rows from your PostgreSQL database.")
    try:
        if st.session_state.pg_password is not None:
            pg_conn = psycopg2.connect(
                host=st.session_state.config['PG_HOST'],
                port=st.session_state.config['PG_PORT'],
                user=st.session_state.config['PG_USER'],
                password=st.session_state.pg_password,
                dbname=st.session_state.config['PG_DB_NAME']
            )
            pg_query = f'SELECT "DateAndTime", "TagIndex", "Val" FROM "{st.session_state.config["PG_TABLE_NAME"]}" ORDER BY "DateAndTime" DESC LIMIT 500;'
            preview_df_pg = pd.read_sql(pg_query, pg_conn)
            st.dataframe(preview_df_pg)
            pg_conn.close()
            st.success("✅ PostgreSQL preview successfully fetched.")
        else:
            st.info("Please save your settings to enable the PostgreSQL preview.")
    except psycopg2.OperationalError as e:
        st.warning(f"⚠️ Could not connect to PostgreSQL for preview. Check your settings or if the database/table exists. Error: {e}")
    except Exception as e:
        st.warning(f"⚠️ An unexpected error occurred while fetching PostgreSQL preview. Error: {e}")

st.markdown("---")

# --- Sync Controls ---
if st.session_state.sync_running:
    if st.button("🛑 Stop Sync"):
        st.session_state.sync_running = False
        if st.session_state.sync_stop_event:
            st.session_state.sync_stop_event.set()
        st.warning("🛑 Sync stopping...")
else:
    if st.button("🚀 Start Sync"):
        if 'pg_password' not in st.session_state or st.session_state.pg_password is None:
            st.error("❌ Please enter your settings and click 'Save Settings' before starting the sync.")
        else:
            st.session_state.sync_running = True
            st.session_state.sync_stop_event = threading.Event()
            st.session_state.sync_thread = threading.Thread(target=sync_continuously_correct, args=(st.session_state.config, st.session_state.pg_password, st.session_state.sync_stop_event))
            st.session_state.sync_thread.start()
            st.info("⏳ Sync started...")
