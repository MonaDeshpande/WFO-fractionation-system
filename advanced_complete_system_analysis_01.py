# -*- coding: utf-8 -*-
"""
Advanced Distillation Analysis for Naphthalene Recovery Plant
- Pulls SCADA data from PostgreSQL (same connection as your script)
- Reads lab results CSV (purity_lab_result.csv)
- Builds a Word report with:
  * Material balance & recovery efficiency (C-03)
  * Energy proxy KPI (reboiler/boil-up)
  * Packing temperature gradient health
  * Purity compliance & risk (if lab has a timestamp series)
  * SPC/control charts (+/-3σ) and anomaly flags
  * Correlation matrix of key drivers
  * Packing temperature heatmaps over time
  * ΔP trend & flooding tendency proxy
  * Baseline (previous period) benchmarking
- Exports KPI table to Excel

Requirements: psycopg2, pandas, numpy, matplotlib, python-docx, openpyxl (for Excel)
"""

import os
import math
import logging
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns # Added for correlation matrix
from sklearn.cluster import KMeans # Added for anomaly detection
from sklearn.preprocessing import StandardScaler # Added for scaling
from statsmodels.tsa.arima.model import ARIMA # Added for time series analysis

from docx import Document
from docx.shared import Inches, Cm
from docx.enum.text import WD_ALIGN_PARAGRAPH
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows

# --------------- CONFIG & SETUP -----------------------------------------------

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
def log_and_print(message, level='info'):
    """Logs a message and prints it to the console."""
    if level == 'info':
        logging.info(message)
    elif level == 'warning':
        logging.warning(message)
    elif level == 'error':
        logging.error(message)
    print(message)

OUT_DIR = "reports_out"
os.makedirs(OUT_DIR, exist_ok=True)

# Rolling window target in MINUTES; automatically converted to points
ROLL_WINDOW_MIN = 120

# Latent heats (very rough proxies)
HVAP_NAPHTHALENE_KJ_PER_KG = 430.0
LATENT_STEAM_KJ_PER_KG = 2100.0

# Column tag map (extend as needed)
COLUMN_ANALYSIS = {
    'C-00': {
        'purpose': 'Remove maximum moisture from the feed.',
        'tags': {
            'feed': 'FI-01',
            'top_flow': 'FI-61',     # water distillate
            'bottom_flow': 'FI-62',
            'top_temp': 'TI-01',
            'dp': 'PI-00-DP'
        }
    },
    'C-01': {
        'purpose': 'Produce bottom Anthracene Oil (< 2% naphthalene).',
        'tags': {
            'reflux_flow': 'FT-08',
            'top_flow': 'FT-02',
            'feed_temp': 'TI-02',
            'packing_temps': ['TI-03','TI-04','TI-05','TI-06'],
            'dp': 'PI-01-DP',
            'pressure': 'PI-01'
        },
        'spec': {'sample':'C-01-B','product':'Naphthalene','max_pct':2.0}
    },
    'C-02': {
        'purpose': 'Produce top Light Oil (< 15% naphthalene).',
        'tags': {
            'reflux_flow': 'FT-09',
            'top_flow': 'FT-03',
            'feed_temp': 'TI-11',
            'packing_temps': ['TI-13','TI-14','TI-15','TI-16','TI-17','TI-18','TI-19','TI-20','TI-21','TI-22','TI-23','TI-24','TI-25'],
            'dp': 'PI-02-DP',
            'pressure': 'PI-02'
        },
        'spec': {'sample':'C-02-T','product':'Naphthalene','max_pct':15.0}
    },
    'C-03': {
        'purpose': 'Recover naphthalene at top; bottom wash oil < 2% naphthalene.',
        'tags': {
            'reflux_flow': 'FT-10',
            'top_flow': 'FT-04',
            'feed_temp': 'TI-30',
            'packing_temps': ['TI-31','TI-32','TI-33','TI-34','TI-35','TI-36','TI-37','TI-38','TI-39','TI-40'],
            'dp': 'PI-03-DP',
            'pressure': 'PI-03',
            'reboiler_steam_flow': 'FI-STEAM-03',
            'condensate_return': 'FI-COND-03',
            'reboiler_temp_A': 'TI-72A',
            'reboiler_temp_B': 'TI-215'
        },
        'spec_top': {'sample':'C-03-T','product':'Naphthalene','min_pct':90.0},
        'spec_bottom': {'sample':'C-03-B','product':'Naphthalene','max_pct':2.0}
    }
}

# --------------- UTILS --------------------------------------------------------

def get_data_from_db(start_time_str, end_time_str, table_name):
    """
    Fetches SCADA data from a PostgreSQL database for a given time range and table.
    
    Args:
        start_time_str (str): The start of the time range (e.g., 'YYYY-MM-DD HH:MM:SS').
        end_time_str (str): The end of the time range (e.g., 'YYYY-MM-DD HH:MM:SS').
        table_name (str): The name of the database table to query.
        
    Returns:
        pd.DataFrame: A DataFrame containing the fetched data, or an empty DataFrame on failure.
    """
    
    # !! IMPORTANT: Replace these with your actual database credentials and details
    DB_HOST = 'localhost'
    DB_NAME = 'scada_data_analysis'
    DB_USER = 'postgres'
    DB_PASS = 'ADMIN'
    DB_PORT = '5432'
    
    conn = None
    try:
        log_and_print("Connecting to the database...")
        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS, port=DB_PORT)
        log_and_print("Database connection successful.")
        
        query = f"SELECT * FROM \"{table_name}\" WHERE \"DateAndTime\" BETWEEN '{start_time_str}' AND '{end_time_str}' ORDER BY \"DateAndTime\";"
        
        log_and_print(f"Executing query: {query}")
        df = pd.read_sql_query(query, conn)
        
        if df.empty:
            log_and_print("Warning: No data found in the specified time range for this table.", 'warning')
        else:
            df = ensure_datetime(df)
            log_and_print(f"Successfully fetched {df.shape[0]} rows.")
            
        return df
        
    except psycopg2.OperationalError as e:
        log_and_print(f"Database connection failed: {e}", 'error')
        return pd.DataFrame()
    except Exception as e:
        log_and_print(f"An error occurred while fetching data: {e}", 'error')
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()
            log_and_print("Database connection closed.")


def have_cols(df, cols):
    """Checks if a DataFrame has all specified columns."""
    return all((c in df.columns) for c in cols if c)

def lab_value(lab_df, sample, product, default=np.nan):
    """Retrieves a single value from the lab results DataFrame."""
    if lab_df.empty:
        return default
    try:
        m = lab_df[(lab_df['Sample']==sample) & (lab_df['Product']==product)]
        return float(m['Value'].iloc[0]) if not m.empty else default
    except Exception as e:
        log_and_print(f"Warning: Could not get lab value for {sample} - {product}. Error: {e}", 'warning')
        return default

def guess_sampling_seconds(df):
    """Guesses the sampling interval of the data."""
    if 'datetime' not in df.columns or df.shape[0] < 2:
        return 60.0
    s = df['datetime'].sort_values().diff().dt.total_seconds().dropna()
    med = float(s.median()) if not s.empty else 60.0
    return max(1.0, med)

def points_for_minutes(df, minutes, min_points=10):
    """Converts a time window in minutes to a number of data points."""
    sec = guess_sampling_seconds(df)
    pts = int((minutes*60.0)/sec)
    return max(min_points, pts)

def compute_recovery_efficiency(df, lab_df, feed_flow_tag, top_flow_tag,
                                 feed_sample='P-01', top_sample='C-03-T',
                                 product='Naphthalene'):
    """Calculates the recovery efficiency of a product."""
    if not have_cols(df, [feed_flow_tag, top_flow_tag]):
        return np.nan, np.nan, np.nan
    
    feed_flow = pd.to_numeric(df[feed_flow_tag], errors='coerce').mean()
    top_flow = pd.to_numeric(df[top_flow_tag], errors='coerce').mean()
    feed_pct = lab_value(lab_df, feed_sample, product)
    top_pct = lab_value(lab_df, top_sample, product)
    
    if any(pd.isna(x) for x in [feed_flow, top_flow, feed_pct, top_pct]) or feed_flow <= 0 or feed_pct <= 0:
        log_and_print("Warning: Insufficient data for recovery efficiency calculation.", 'warning')
        return np.nan, np.nan, np.nan
    
    feed_nap = feed_flow * (feed_pct / 100.0)
    top_nap = top_flow * (top_pct / 100.0)
    
    return (top_nap / feed_nap) * 100.0, feed_nap, top_nap

def packing_temp_gradient_score(df, packing_tags):
    """Calculates mean and std of temperature gradients across packing."""
    packing = [t for t in (packing_tags or []) if t in df.columns]
    if len(packing) < 2:
        return np.nan, np.nan
    packing_means = [pd.to_numeric(df[t], errors='coerce').mean() for t in packing]
    if any(pd.isna(x) for x in packing_means):
        return np.nan, np.nan
    grads = np.diff(packing_means)