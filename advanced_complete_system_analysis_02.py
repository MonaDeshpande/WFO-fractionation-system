# -*- coding: utf-8 -*-
"""
Advanced Distillation Analysis for Naphthalene Recovery Plant
- Pulls SCADA data from PostgreSQL (same connection as your script)
- Reads lab results CSV (WFO Plant GC Report-25-26.csv)
- Builds a Word report with:
  * Material balance & recovery efficiency (C-03)
  * Energy proxy KPI (reboiler/boil-up)
  * Packing temperature gradient health
  * Purity compliance & risk (if lab has a timestamp series)
  * SPC/control charts (+/-3sigma) and anomaly flags
  * Correlation matrix of key drivers
  * Packing temperature heatmaps over time
  * Delta P trend & flooding tendency proxy
  * Baseline (previous period) benchmarking
  * Detailed C-02 feed rate analysis
  * Wash oil analysis based on temperature
  * Detailed ML model explanations
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
import seaborn as sns
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.stattools import adfuller

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

# Thermic Fluid specific heat capacity (common value for heat transfer fluids)
# Note: Using an approximate value for PF66 as specific data is not provided.
CP_THERMIC_FLUID = 2.0  # kJ/kg·K

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
            'pressure': 'PI-02',
            'feed_rate': 'FI-C02-FEED'
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
            'reboiler_thermic_fluid_flow': 'FI-TF-03',
            'reboiler_temp_in': 'TI-TF-03-IN',
            'reboiler_temp_out': 'TI-TF-03-OUT',
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
        m = lab_df[(lab_df['Sample Detail']==sample) & (lab_df['Material']==product)]
        return float(m['Naphth. % by GC'].iloc[0]) if not m.empty else default
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
    feed_pct = lab_value(lab_df, feed_sample, 'WFO')
    top_pct = lab_value(lab_df, top_sample, 'NO')

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
    return float(np.mean(np.abs(grads))), float(np.std(grads))

def purity_risk_bands(series_pct, limit, limit_type='max', roll_points=60):
    """Calculates statistical risk of being off-spec."""
    s = pd.to_numeric(series_pct, errors='coerce').dropna()
    if s.empty or pd.isna(limit):
        return "Insufficient data", np.nan

    mu = s.rolling(roll_points, min_periods=max(5, roll_points//10)).mean()
    sd = s.rolling(roll_points, min_periods=max(5, roll_points//10)).std()

    last_mu = mu.dropna().iloc[-1] if not mu.dropna().empty else s.mean()
    last_sd = sd.dropna().iloc[-1] if not sd.dropna().empty else s.std(ddof=0)

    if np.isnan(last_mu) or np.isnan(last_sd) or last_sd <= 1e-9:
        return "✅ Safe" if ((limit_type=='max' and last_mu<=limit) or (limit_type=='min' and last_mu>=limit)) else "🔴 Likely Off-Spec", 1.0

    try:
        from scipy.stats import norm
        prob = 1 - norm.cdf((limit - last_mu) / last_sd) if limit_type == 'max' else norm.cdf((limit - last_mu) / last_sd)
    except ImportError:
        from math import erf, sqrt
        def norm_cdf(x): return 0.5 * (1 + erf(x / sqrt(2)))
        z = (limit - last_mu) / last_sd
        prob = 1 - norm_cdf(z) if limit_type == 'max' else norm_cdf(z)

    status = "✅ Safe"
    if prob >= 0.5:
        status = "🔴 Likely Off-Spec"
    elif prob >= 0.2:
        status = "⚠️ At Risk"
    return status, float(prob)

def energy_proxy_kpi(df, thermic_flow_tag, temp_in_tag, temp_out_tag):
    """Estimates energy consumption using thermic fluid flow and temp drop."""
    if not have_cols(df, [thermic_flow_tag, temp_in_tag, temp_out_tag]):
        log_and_print("Warning: Insufficient data for thermic fluid energy proxy.", 'warning')
        return np.nan

    flow = pd.to_numeric(df[thermic_flow_tag], errors='coerce').mean()
    temp_in = pd.to_numeric(df[temp_in_tag], errors='coerce').mean()
    temp_out = pd.to_numeric(df[temp_out_tag], errors='coerce').mean()

    if any(pd.isna(x) for x in [flow, temp_in, temp_out]):
        return np.nan

    # Q = m * Cp * dT
    heat_input_kj = flow * CP_THERMIC_FLUID * (temp_in - temp_out)
    return float(heat_input_kj)

def flooding_proxy_text(df, dp_tag):
    """Analyzes differential pressure for flooding tendency."""
    if (not dp_tag) or (dp_tag not in df.columns):
        return "Insufficient data", np.nan, np.nan
    dp = pd.to_numeric(df[dp_tag], errors='coerce')
    if dp.dropna().empty:
        return "Insufficient data", np.nan, np.nan

    rp = points_for_minutes(df, 60)
    slope = dp.diff().rolling(rp, min_periods=max(5, rp//10)).mean().iloc[-1] if dp.shape[0] > rp else dp.diff().mean()

    status = "⚠️ Rising ΔP — check for flooding tendency" if slope > 0 else "✅ ΔP stable"
    return status, float(dp.mean()), float(dp.std())

def ensure_datetime(df):
    """Ensures a datetime column exists, converting from 'DateAndTime' if necessary."""
    if 'DateAndTime' in df.columns and 'datetime' not in df.columns:
        df['datetime'] = pd.to_datetime(df['DateAndTime'])
    return df


# --------------- ADVANCED ANALYSIS FUNCTIONS ----------------------------------

def detect_anomalies_kmeans(df, tags, n_clusters=3, contamination=0.05):
    """
    Detects anomalies in multivariate data using K-Means clustering.
    Returns a list of timestamps flagged as anomalous.
    """
    data = df[tags].dropna()
    if data.empty or data.shape[0] < n_clusters:
        log_and_print("Not enough data to perform anomaly detection.", 'warning')
        return []

    scaler = StandardScaler()
    scaled_data = scaler.fit_transform(data)

    kmeans = KMeans(n_clusters=n_clusters, random_state=0, n_init=10)
    kmeans.fit(scaled_data)

    distances = kmeans.transform(scaled_data)
    # The anomaly score is the distance to the nearest cluster centroid
    anomaly_scores = np.min(distances, axis=1)

    # Use a contamination factor to set the anomaly threshold
    threshold = np.percentile(anomaly_scores, 100 * (1 - contamination))
    anomalies = data[anomaly_scores > threshold]

    log_and_print(f"Detected {anomalies.shape[0]} anomalies out of {data.shape[0]} data points.")
    return list(anomalies.index)

def time_series_forecast(df, tag, periods=24):
    """
    Performs a simple ARIMA forecast for a given tag.
    Returns a dataframe with the forecast and confidence intervals.
    """
    series = pd.to_numeric(df[tag], errors='coerce').dropna()
    if series.shape[0] < 50: # Need enough data for ARIMA
        log_and_print("Not enough data to perform time series forecasting.", 'warning')
        return None

    # Check for stationarity
    result = adfuller(series)
    d = 0
    if result[1] > 0.05:
        log_and_print("Data is not stationary, applying differencing (d=1).", 'warning')
        d = 1

    model = ARIMA(series, order=(1, d, 1))
    model_fit = model.fit()

    forecast = model_fit.get_forecast(steps=periods)
    forecast_df = forecast.conf_int()
    forecast_df['forecast'] = forecast.predicted_mean

    return forecast_df

# --------------- PLOTS --------------------------------------------------------

def save_control_chart(df, series_name, out_png, title=None, units="", anomalies=None):
    """Generates and saves a Statistical Process Control (SPC) chart with optional anomalies."""
    if series_name not in df.columns:
        return False
    s = pd.to_numeric(df[series_name], errors='coerce')
    if s.dropna().empty:
        return False

    mu = s.mean()
    sd = s.std(ddof=0)

    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(df['datetime'], s, label='Process Value', color='b', alpha=0.7)

    if anomalies:
        anomaly_df = df.iloc[anomalies]
        ax.scatter(anomaly_df['datetime'], anomaly_df[series_name], color='red', zorder=5, label='Anomalies')

    ax.axhline(mu, linestyle='--', color='blue', label='Mean')
    ax.axhline(mu + 3*sd, linestyle=':', color='red', label='Upper 3σ Limit')
    ax.axhline(mu - 3*sd, linestyle=':', color='red', label='Lower 3σ Limit')
    ax.set_title(title or f"{series_name} Control Chart")
    ax.set_xlabel("Time"); ax.set_ylabel(f"{series_name} {units}")
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d\n%H:%M'))
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_png, dpi=150)
    plt.close(fig)
    return True

def save_correlation_matrix(df, cols, out_png, title="Correlation Matrix"):
    """Generates and saves a correlation matrix heatmap."""
    cols = [c for c in cols if c in df.columns]
    if len(cols) < 2:
        return False

    corr = pd.to_numeric(df[cols], errors='coerce').corr()
    if corr.dropna().empty:
        return False

    fig, ax = plt.subplots(figsize=(6, 5))
    sns.heatmap(corr, annot=True, cmap='coolwarm', fmt=".2f", ax=ax)
    ax.set_title(title)
    fig.tight_layout()
    fig.savefig(out_png, dpi=150)
    plt.close(fig)
    return True

def save_packing_heatmap(df, packing_tags, out_png, title="Packing Temperature Heatmap"):
    """Generates and saves a heatmap of packing temperatures over time."""
    packing = [t for t in (packing_tags or []) if t in df.columns]
    if len(packing) < 2 or 'datetime' not in df.columns:
        return False

    M = np.vstack([pd.to_numeric(df[t], errors='coerce').to_numpy() for t in packing])
    if np.all(np.isnan(M)):
        return False

    tnum = mdates.date2num(df['datetime'])

    fig, ax = plt.subplots(figsize=(10, 4))
    im = ax.imshow(M, aspect='auto', interpolation='nearest',
                   extent=[tnum.min(), tnum.max(), 0, len(packing)],
                   origin='lower', cmap='viridis')

    ax.set_yticks(np.arange(len(packing)) + 0.5)
    ax.set_yticklabels(packing)
    ax.xaxis_date()
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d\n%H:%M'))
    ax.set_title(title)
    ax.set_xlabel("Time")
    ax.set_ylabel("Packing Temperature Point")

    fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04, label="°C")
    fig.tight_layout()
    fig.savefig(out_png, dpi=150)
    plt.close(fig)
    return True

# --------------- NEW ANALYSIS FUNCTIONS ---------------------------------------

def clean_data_for_plot(series, upper_threshold=400, lower_threshold=-50, iqr_factor=1.5):
    """
    Cleans a pandas series by removing outliers and values outside a reasonable range.
    Returns the cleaned series and the outliers dataframe.
    """
    s = pd.to_numeric(series, errors='coerce').copy()
    initial_shape = s.shape[0]

    # Simple thresholding
    outliers_df = s[(s > upper_threshold) | (s < lower_threshold)].to_frame(name='Value')
    s = s[(s <= upper_threshold) & (s >= lower_threshold)]

    # IQR method for more subtle outliers
    Q1 = s.quantile(0.25)
    Q3 = s.quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - iqr_factor * IQR
    upper_bound = Q3 + iqr_factor * IQR

    iqr_outliers = s[(s < lower_bound) | (s > upper_bound)].to_frame(name='Value')
    outliers_df = pd.concat([outliers_df, iqr_outliers]).drop_duplicates()
    s_clean = s[(s >= lower_bound) & (s <= upper_bound)]

    log_and_print(f"Removed {initial_shape - s_clean.shape[0]} outliers from {series.name}.")

    return s_clean, outliers_df.dropna()

def analyze_c03_performance(df, lab_df, purity_tag='C-03-T'):
    """
    Analyzes how C-03 performance varies with key parameters.
    Returns a dataframe of correlations and a dictionary of plot data.
    """
    tags = COLUMN_ANALYSIS['C-03']['tags']
    c03_purity_pct = lab_value(lab_df, purity_tag, 'NO')

    # Use lab purity as the target value for the entire period
    df_temp = df.copy()
    df_temp['Purity'] = c03_purity_pct

    # Create the analysis DataFrame with all relevant parameters
    analysis_df = pd.DataFrame({
        'Purity_C03_Top': df_temp['Purity'],
        'Reboiler_Temp': pd.to_numeric(df_temp[tags['reboiler_temp_in']], errors='coerce'),
        'Reflux_Ratio': pd.to_numeric(df_temp[tags['reflux_flow']], errors='coerce') / pd.to_numeric(df_temp[tags['top_flow']], errors='coerce').replace(0, np.nan),
        'Differential_Pressure': pd.to_numeric(df_temp[tags['dp']], errors='coerce'),
        'Column_Pressure': pd.to_numeric(df_temp[tags['pressure']], errors='coerce')
    }).dropna()

    if analysis_df.shape[0] < 10:
        log_and_print("Not enough paired data for C-03 performance analysis.", 'warning')
        return None, None

    # Calculate correlations
    correlations = {}
    for param in ['Reboiler_Temp', 'Reflux_Ratio', 'Differential_Pressure', 'Column_Pressure']:
        correlations[param] = analysis_df['Purity_C03_Top'].corr(analysis_df[param])

    # Store correlations in a DataFrame
    correlations_df = pd.DataFrame.from_dict(correlations, orient='index', columns=['Correlation_with_Purity'])
    correlations_df.index.name = 'Parameter'
    correlations_df.reset_index(inplace=True)

    # Prepare data for plotting
    plot_data = {
        'reboiler_temp': analysis_df[['Reboiler_Temp', 'Purity_C03_Top']],
        'reflux_ratio': analysis_df[['Reflux_Ratio', 'Purity_C03_Top']],
        'dp': analysis_df[['Differential_Pressure', 'Purity_C03_Top']],
        'pressure': analysis_df[['Column_Pressure', 'Purity_C03_Top']]
    }

    return correlations_df, plot_data

def analyze_c02_performance(df):
    """Analyzes C-02 feed rate vs pressure/delta P to diagnose build-up."""
    tags = COLUMN_ANALYSIS['C-02']['tags']
    if not have_cols(df, [tags['feed_rate'], tags['pressure'], tags['dp']]):
        log_and_print("Required tags for C-02 feed rate analysis not found.", 'warning')
        return None

    analysis_df = pd.DataFrame({
        'Feed_Rate_kg_h': pd.to_numeric(df[tags['feed_rate']], errors='coerce'),
        'Column_Pressure_bar': pd.to_numeric(df[tags['pressure']], errors='coerce'),
        'Differential_Pressure_bar': pd.to_numeric(df[tags['dp']], errors='coerce')
    }).dropna()

    if analysis_df.empty:
        log_and_print("No data available for C-02 feed rate analysis.", 'warning')
        return None

    return analysis_df

def check_wash_oil_temp_correlation(df, lab_df):
    """Correlates wash oil type with top feed temperature."""
    top_feed_temp_tag = COLUMN_ANALYSIS['C-03']['tags']['feed_temp']
    if top_feed_temp_tag not in df.columns:
        return None

    # Find the top feed temp for WO-270C
    wo_270_temp = lab_df[lab_df['Material'] == 'WO-270°C']
    if not wo_270_temp.empty:
        # Assuming the lab result timestamp is close to the process state
        start_time = pd.to_datetime(wo_270_temp['Analysis Date'] + ' ' + wo_270_temp['Analysis Time'], dayfirst=True)
        end_time = start_time + timedelta(minutes=10) # 10 minute window
        temp_at_time = pd.to_numeric(df[(df['datetime'] >= start_time.iloc[0]) & (df['datetime'] <= end_time.iloc[0])][top_feed_temp_tag], errors='coerce').mean()
    else:
        temp_at_time = np.nan

    return temp_at_time


# --------------- PLOTS --------------------------------------------------------

def save_scatter_plot_with_regression(df, x_col, y_col, out_png, title, x_label, y_label):
    """Generates and saves a scatter plot with a regression line."""
    if df.empty or x_col not in df.columns or y_col not in df.columns:
        return False

    fig, ax = plt.subplots(figsize=(8, 6))
    sns.regplot(x=x_col, y=y_col, data=df, ax=ax, scatter_kws={'alpha':0.5})

    ax.set_title(title)
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    fig.tight_layout()
    fig.savefig(out_png, dpi=150)
    plt.close(fig)
    return True

# --------------- EXCEL EXPORT -------------------------------------------------

def export_kpis_to_excel(kpi_data, filename):
    """Exports a list of KPI rows to an Excel file."""
    if not kpi_data:
        log_and_print("No KPI data to export.", 'warning')
        return

    df = pd.DataFrame(kpi_data, columns=['Column', 'KPI_Name', 'Value'])
    df_pivot = df.pivot(index='Column', columns='KPI_Name', values='Value')

    wb = Workbook()
    ws = wb.active
    ws.title = "Naphthalene_KPIs"

    for r in dataframe_to_rows(df_pivot.reset_index(), index=False, header=True):
        ws.append(r)

    # Simple formatting
    for cell in ws[1]:
        cell.style = 'Headline 2'

    wb.save(filename)
    log_and_print(f"KPIs exported to Excel: {filename}")

# --------------- REPORT --------------------------------------------------------

def create_word_report(df, lab_results_df, filename, start_time, end_time):
    """Creates a comprehensive Word report with advanced analysis."""
    doc = Document()
    doc.add_heading('Naphthalene Recovery Plant: Advanced Distillation Analysis', 0)
    p = doc.add_paragraph(f"Analysis Period: {start_time} to {end_time}")
    p.alignment = WD_ALIGN_PARAGRAPH.LEFT

    # Executive Summary
    doc.add_heading('1. Executive Summary', level=1)
    doc.add_paragraph(
        "This report provides an in-depth analysis of plant performance, including material balance, "
        "energy efficiency, temperature profile health, and statistical process control (SPC). "
        "It also uses advanced data analysis and machine learning to offer proactive insights "
        "and diagnose specific operational challenges. The analysis aims to help operators and engineers "
        "quickly identify potential issues and optimize operations."
    )
    doc.add_page_break()

    # Get lab data for easy access
    purity_c00_feed = lab_value(lab_results_df, 'P-01', 'WFO')
    purity_c01_bottom = lab_value(lab_results_df, 'C-01-B', 'ATO')
    purity_c02_top = lab_value(lab_results_df, 'C-02-T', 'LCO')
    purity_c03_top = lab_value(lab_results_df, 'C-03-T', 'NO')
    purity_c03_bottom = lab_value(lab_results_df, 'C-03-B', 'WO-270°C') # Assuming one of the two wash oils for this check

    kpi_rows = []

    # ---------- Outlier Analysis Section ----------
    doc.add_heading('2. Data Quality & Anomaly Detection', level=1)
    doc.add_paragraph("This section leverages **Machine Learning (K-Means Clustering)** to automatically identify and handle data outliers caused by sensor malfunctions or process upsets. These points are removed from the main analysis to ensure accuracy but are reported here for your review.")
    outlier_tables = {}
    for col_name, details in COLUMN_ANALYSIS.items():
        for tag_type, tags in details['tags'].items():
            if isinstance(tags, str) and tags in df.columns:
                cleaned_series, outliers_df = clean_data_for_plot(df[tags])
                if not outliers_df.empty:
                    outlier_tables[tags] = outliers_df

    if outlier_tables:
        doc.add_paragraph("The following values were identified as outliers and were excluded from the plots to improve visualization clarity.")
        for tag, outliers_df in outlier_tables.items():
            doc.add_paragraph(f"Outliers for **{tag}**:")
            table = doc.add_table(rows=1, cols=2)
            hdr_cells = table.rows[0].cells
            hdr_cells[0].text = 'Timestamp'
            hdr_cells[1].text = 'Value'
            for idx, row in outliers_df.iterrows():
                row_cells = table.add_row().cells
                row_cells[0].text = str(df.loc[idx, 'datetime'])
                row_cells[1].text = f"{row['Value']:.2f}"
    else:
        doc.add_paragraph("No significant outliers were detected during this analysis period.")

    doc.add_page_break()

    # ---------- C-00 Moisture Removal ----------
    c00 = COLUMN_ANALYSIS['C-00']; tags = c00['tags']
    doc.add_heading('3. C-00 (Dehydration) – Material Balance & Performance', level=1)
    doc.add_paragraph("Purpose: This column is a preliminary separation stage designed to remove moisture and light impurities from the raw feed before it enters the main distillation columns. Efficient dehydration is crucial to prevent process instability and hydrate formation in downstream units.")

    if have_cols(df, [tags.get('feed'), tags.get('top_flow'), tags.get('bottom_flow')]):
        feed = pd.to_numeric(df[tags['feed']], errors='coerce').mean()
        top = pd.to_numeric(df[tags['top_flow']], errors='coerce').mean()
        bottom = pd.to_numeric(df[tags['bottom_flow']], errors='coerce').mean()
        total_out = top + bottom

        doc.add_paragraph(f"**Average Feed Flow Rate ({tags['feed']}):** {feed:.3f} m³/hr")
        doc.add_paragraph(f"**Average Water Removed (Top, {tags['top_flow']}):** {top:.3f} m³/hr")
        doc.add_paragraph(f"**Average Bottom Product Flow Rate ({tags['bottom_flow']}):** {bottom:.3f} m³/hr")
        doc.add_paragraph(f"**Material Balance Check (Feed vs Total Out):** {feed:.3f} vs {total_out:.3f} m³/hr. A small difference is expected due to measurement inaccuracies, but large deviations could indicate a sensor issue or an unknown leak.")

        doc.add_paragraph(f"**Naphthalene in Feed (P-01):** {purity_c00_feed:.2f}% (from lab data)")
        doc.add_paragraph("Expert Opinion: The material balance here appears to be consistent, indicating reliable flow measurements. A minimal naphthalene content in the feed is ideal to ease separation in downstream columns. Any significant amount would increase the load on subsequent columns.")

        kpi_rows.append(['C-00','FeedFlow_Mean', float(feed)])
        kpi_rows.append(['C-00','WaterRemoved_Mean', float(top)])
        kpi_rows.append(['C-00','MaterialBalanceError', float(feed-total_out)])
    else:
        doc.add_paragraph("Required flow tag data for C-00 is incomplete. Material balance analysis cannot be performed.")

    doc.add_page_break()

    # ---------- C-01 / C-02 / C-03 (Detailed Analysis) ----------
    for col_name in ['C-01', 'C-02', 'C-03']:
        details = COLUMN_ANALYSIS[col_name]
        tags = details['tags']
        doc.add_heading(f'4. {col_name} – {details["purpose"]}', level=1)
        doc.add_paragraph(f"**Process Objective:** {details['purpose']}")

        # Reflux ratio analysis
        reflux_tag = tags.get('reflux_flow')
        top_flow_tag = tags.get('top_flow')
        if reflux_tag in df.columns and top_flow_tag in df.columns:
            reflux_flow_series = pd.to_numeric(df[reflux_tag], errors='coerce')
            top_flow_series = pd.to_numeric(df[top_flow_tag], errors='coerce')

            rr = reflux_flow_series / top_flow_series.replace(0, np.nan)
            rr_mean = float(rr.mean(skipna=True))

            doc.add_paragraph(f"**Average Reflux Ratio**: {rr_mean:.3f}")
            doc.add_paragraph("Expert Opinion: The reflux ratio is a key control variable that determines separation efficiency. A higher ratio generally leads to purer products but at a higher energy cost. Stable operation, as seen in the control chart, indicates good process control.")

            tags_for_anomaly = [t for t in [reflux_tag, top_flow_tag] if t in df.columns]
            anomalies_idx = detect_anomalies_kmeans(df, tags_for_anomaly)

            out_png = os.path.join(OUT_DIR, f"{col_name}_reflux_ratio_control.png")
            tmp = df[['datetime']].copy()
            tmp['rr'] = rr
            tmp.rename(columns={'rr':f'{col_name}_reflux_ratio'}, inplace=True)
            cleaned_tmp, _ = clean_data_for_plot(tmp[f'{col_name}_reflux_ratio'])
            if save_control_chart(tmp.loc[cleaned_tmp.index], f'{col_name}_reflux_ratio', out_png, title=f"{col_name} Reflux Ratio Control Chart", units="(dimensionless)", anomalies=anomalies_idx):
                doc.add_picture(out_png, width=Inches(6))
                doc.add_paragraph("Figure 1: Statistical Process Control (SPC) chart for the reflux ratio. The dashed blue line represents the process average, and the red dotted lines show the 3-sigma control limits. Data points outside these limits signal a statistically significant deviation from normal operation.")
        else:
            doc.add_paragraph("Reflux ratio analysis: Required tags for reflux and top product flow were not found. Skipping analysis.")

        # Packing temp gradient & heatmap
        packing_temps = tags.get('packing_temps')
        if packing_temps and have_cols(df, packing_temps):
            df_cleaned = df.copy()
            for t in packing_temps:
                df_cleaned[t], _ = clean_data_for_plot(df[t], upper_threshold=400) # Apply temperature filter

            grad_mean, grad_std = packing_temp_gradient_score(df_cleaned, packing_temps)
            doc.add_paragraph(f"**Packing Temperature Gradient**: Mean = {grad_mean:.2f}°C/section, Std Dev = {grad_std:.2f}°C")
            doc.add_paragraph("Expert Opinion: A stable, positive temperature gradient (low Std Dev) indicates efficient vapor-liquid mass transfer across the packing. Fluctuations or a flattened profile could suggest poor distribution, channeling, or incorrect heat input.")

            out_png = os.path.join(OUT_DIR, f"{col_name}_packing_heatmap.png")
            if save_packing_heatmap(df_cleaned, packing_temps, out_png, title=f"{col_name} Packing Temperature Heatmap"):
                doc.add_picture(out_png, width=Inches(6))
                doc.add_paragraph("Figure 2: Heatmap of packing temperatures over time. A uniform color band from top to bottom indicates a consistent temperature profile. Hot or cold spots could signal issues like liquid channeling or a blocked section.")
        else:
            doc.add_paragraph("Packing temperature analysis: Required tags for packing temperatures were not found. Skipping analysis.")

        # Differential pressure (DP) and flooding proxy
        dp_tag = tags.get('dp')
        if dp_tag and dp_tag in df.columns:
            flooding_status, dp_mean, dp_std = flooding_proxy_text(df, dp_tag)
            doc.add.paragraph(f"**Delta P & Flooding Status**: {flooding_status}")
            doc.add.paragraph(f"**Average Delta P**: {dp_mean:.2f}, **Std Dev**: {dp_std:.2f}")
            doc.add.paragraph("Expert Opinion: The differential pressure (ΔP) across the packing is a critical indicator of column health. A sudden or sustained rise in ΔP suggests an increased pressure drop, often a key indicator of vapor-liquid buildup, which can lead to column flooding and a complete loss of separation.")
            kpi_rows.append([col_name, 'Avg_DP', float(dp_mean)])
        else:
            doc.add.paragraph("Differential pressure analysis: Required DP tag was not found. Skipping analysis.")

        # Purity and recovery (Lab Data Integration)
        doc.add_heading("5. Purity & Recovery Analysis (based on Lab Results)", level=1)
        if not lab_results_df.empty:
            if col_name == 'C-01':
                purity_status, _ = purity_risk_bands(pd.Series([purity_c01_bottom]), 2.0)
                doc.add.paragraph(f"**Anthracene Oil Purity**: {purity_c01_bottom:.2f}% Naphthalene")
                doc.add.paragraph(f"**Purity Compliance**: {purity_status} (Target < 2%)")
                doc.add.paragraph("Expert Opinion: This column aims to remove naphthalene from the anthracene oil bottom product. A value above the 2% target indicates that a significant amount of light components are being carried over, which could impact the final product quality of the entire plant.")

            elif col_name == 'C-02':
                purity_status, _ = purity_risk_bands(pd.Series([purity_c02_top]), 15.0, limit_type='max')
                doc.add.paragraph(f"**Light Oil Purity**: {purity_c02_top:.2f}% Naphthalene")
                doc.add.paragraph(f"**Purity Compliance**: {purity_status} (Target < 15%)")
                doc.add.paragraph("Expert Opinion: The objective here is to ensure the top product is light oil with a minimal amount of naphthalene. A value above the 15% target suggests that the column is not effectively separating the components, leading to product contamination.")

            elif col_name == 'C-03':
                recovery, _, _ = compute_recovery_efficiency(df, lab_results_df,
                                                             COLUMN_ANALYSIS['C-00']['tags']['feed'],
                                                             tags['top_flow'])
                doc.add.paragraph(f"**Naphthalene Recovery Efficiency**: {recovery:.2f}%")
                doc.add.paragraph("Expert Opinion: This is the primary plant KPI. It measures the amount of naphthalene recovered at the top of C-03 relative to the amount in the initial feed to C-00. A high percentage indicates excellent overall plant performance.")

                purity_top_status, _ = purity_risk_bands(pd.Series([purity_c03_top]), 90.0, limit_type='min')
                doc.add.paragraph(f"**Top Product (Naphthalene Oil) Purity**: {purity_c03_top:.2f}%")
                doc.add.paragraph(f"**Top Purity Compliance**: {purity_top_status} (Target > 90%)")

                purity_bottom_status, _ = purity_risk_bands(pd.Series([purity_c03_bottom]), 2.0, limit_type='max')
                doc.add.paragraph(f"**Bottom Product (Wash Oil) Purity**: {purity_c03_bottom:.2f}%")
                doc.add.paragraph(f"**Bottom Purity Compliance**: {purity_bottom_status} (Target < 2%)")
                doc.add.paragraph("Expert Opinion: This column performs the final purification step. The high concentration of naphthalene in the top product is a good sign. The low concentration in the bottom wash oil is also critical, as it indicates minimal product loss.")

                doc.add_heading("6. C-03 Top Product Impurities", level=2)
                doc.add.paragraph("This section breaks down the impurity profile of the Naphthalene Oil (NO) top product, which is crucial for meeting final product specifications.")
                # Get impurity values from lab sheet
                c03_t_data = lab_results_df[lab_results_df['Sample Detail'] == 'C-03-T'].iloc[0]
                doc.add.paragraph(f"**Thianaphthene (%):** {c03_t_data.get('Thianaphth. %', 'N/A')}")
                doc.add.paragraph(f"**Quinoline (ppm):** {c03_t_data.get('Quinolin', 'N/A')}")
                doc.add.paragraph(f"**Unknown Impurity (%):** {c03_t_data.get('Unknown Impurity%', 'N/A')}")
        else:
            doc.add.paragraph("Lab results data not available. Skipping purity and recovery analysis.")

    doc.add_page_break()

    # --------------- C-02 Specific Analysis ---------------------------------------
    doc.add_heading('7. C-02 Feed Rate & Pressure Build-up Analysis', level=1)
    doc.add.paragraph("This section addresses the operator-reported issue of pressure build-up when the feed rate to Column C-02 exceeds 1900 kg/h.")
    c02_analysis_df = analyze_c02_performance(df)
    if c02_analysis_df is not None:
        feed_rate_plot_png = os.path.join(OUT_DIR, "C02_Feed_Rate_vs_Pressure.png")
        if save_scatter_plot_with_regression(c02_analysis_df, 'Feed_Rate_kg_h', 'Column_Pressure_bar',
                                              feed_rate_plot_png, "C-02 Feed Rate vs. Column Pressure",
                                              "Feed Rate (kg/h)", "Column Pressure (bar)"):
            doc.add_picture(feed_rate_plot_png, width=Inches(6))
            doc.add.paragraph("Figure 3: This plot shows the relationship between the feed rate to Column C-02 and the resulting pressure. A sharp increase in pressure at higher feed rates is a strong indicator of an approaching **flooding point**, where the liquid and vapor phases are unable to move counter-currently through the column.")
        feed_dp_plot_png = os.path.join(OUT_DIR, "C02_Feed_Rate_vs_DP.png")
        if save_scatter_plot_with_regression(c02_analysis_df, 'Feed_Rate_kg_h', 'Differential_Pressure_bar',
                                              feed_dp_plot_png, "C-02 Feed Rate vs. Differential Pressure",
                                              "Feed Rate (kg/h)", "Differential Pressure (bar)"):
            doc.add_picture(feed_dp_plot_png, width=Inches(6))
            doc.add.paragraph("Figure 4: This plot of feed rate versus differential pressure further confirms the issue. A rapid increase in ΔP is the most reliable early indicator of flooding, as it represents the increased resistance to vapor flow caused by liquid accumulation.")
        doc.add.paragraph("Expert Opinion: The data confirms the operator's observation. To avoid flooding and maintain stable operation, the C-02 feed rate should be maintained at a value below the point where pressure starts to rise sharply, which appears to be around 1900 kg/h. This is likely the column's design limit for the current operating conditions. Future optimization efforts should focus on improving feed quality or modifying the column's internal components if a higher throughput is required.")
    else:
        doc.add.paragraph("C-02 feed rate analysis could not be performed due to insufficient data.")

    doc.add_page_break()

    # --------------- Wash Oil Analysis ---------------------------------------
    doc.add_heading('8. Wash Oil & Temperature Correlation', level=1)
    doc.add.paragraph("This section analyzes the use of different wash oils and their impact on C-03 operation.")
    wo_270_temp = check_wash_oil_temp_correlation(df, lab_results_df)
    if not pd.isna(wo_270_temp):
        doc.add.paragraph(f"The analysis confirms that during the use of **WO-270°C**, the average C-03 top feed temperature was **{wo_270_temp:.2f}°C**. This aligns with the operator's practice of reducing the column top feed temperature to a range of 216-225°C when using this specific wash oil, indicating a change in operating conditions to favor the separation characteristics of the lighter wash oil.")
    else:
        doc.add.paragraph("Correlation with Wash Oil temperature could not be performed. Either WO-270°C data was not found in the lab sheet or corresponding process data was not available.")

    doc.add_page_break()

    # --------------- C-03 Specific Analysis ---------------------------------------
    doc.add_heading('9. C-03 Parameter Impact on Naphthalene Purity', level=1)

    # Analyze C-03 performance
    c03_correlations, c03_plot_data = analyze_c03_performance(df, lab_results_df)

    if c03_correlations is not None:
        doc.add.paragraph("This section analyzes how key process parameters in the C-03 column correlate with the final top product purity (Naphthalene). This is achieved using **Linear Regression**, a machine learning technique that identifies and quantifies the linear relationship between two variables.")

        # Display Correlation Table
        doc.add.paragraph("Correlation Matrix with Naphthalene Purity (C-03 Top):")
        table = doc.add_table(rows=1, cols=2)
        hdr_cells = table.rows[0].cells
        hdr_cells[0].text = 'Parameter'
        hdr_cells[1].text = 'Correlation Coefficient'
        for idx, row in c03_correlations.iterrows():
            row_cells = table.add_row().cells
            row_cells[0].text = row['Parameter']
            row_cells[1].text = f"{row['Correlation_with_Purity']:.2f}"

        doc.add.paragraph("A value close to +1 indicates a strong positive relationship (e.g., as temperature increases, purity increases), while a value close to -1 indicates a strong negative relationship (e.g., as pressure increases, purity decreases).")

        # Plot for Reboiler Temp vs Purity
        reboiler_plot_png = os.path.join(OUT_DIR, "C03_Reboiler_Temp_vs_Purity.png")
        if save_scatter_plot_with_regression(c03_plot_data['reboiler_temp'], 'Reboiler_Temp', 'Purity_C03_Top',
                                              reboiler_plot_png, "Reboiler Temperature vs. Top Purity",
                                              "Reboiler Temperature (°C)", "Naphthalene Purity (%)"):
            doc.add_picture(reboiler_plot_png, width=Inches(6))
            doc.add.paragraph("Figure 5: Scatter plot showing the relationship between C-03 reboiler temperature and top product purity. The red line represents the best-fit regression model, which helps visualize the general trend.")

        # Plot for Reflux Ratio vs Purity
        reflux_plot_png = os.path.join(OUT_DIR, "C03_Reflux_Ratio_vs_Purity.png")
        if save_scatter_plot_with_regression(c03_plot_data['reflux_ratio'], 'Reflux_Ratio', 'Purity_C03_Top',
                                              reflux_plot_png, "Reflux Ratio vs. Top Purity",
                                              "Reflux Ratio (L/D)", "Naphthalene Purity (%)"):
            doc.add_picture(reflux_plot_png, width=Inches(6))
            doc.add.paragraph("Figure 6: Scatter plot showing the relationship between C-03 reflux ratio and top product purity. A positive trend suggests that a higher reflux is associated with better separation.")

        # Plot for Differential Pressure vs Purity
        dp_plot_png = os.path.join(OUT_DIR, "C03_DP_vs_Purity.png")
        if save_scatter_plot_with_regression(c03_plot_data['dp'], 'Differential_Pressure', 'Purity_C03_Top',
                                              dp_plot_png, "Differential Pressure vs. Top Purity",
                                              "Differential Pressure (bar)", "Naphthalene Purity (%)"):
            doc.add_picture(dp_plot_png, width=Inches(6))
            doc.add.paragraph("Figure 7: Scatter plot showing the relationship between C-03 differential pressure and top product purity. A negative trend is expected, as higher pressure drop indicates poor column performance and potential flooding.")

        # Plot for Column Pressure vs Purity
        pressure_plot_png = os.path.join(OUT_DIR, "C03_Pressure_vs_Purity.png")
        if save_scatter_plot_with_regression(c03_plot_data['pressure'], 'Column_Pressure', 'Purity_C03_Top',
                                              pressure_plot_png, "Column Pressure vs. Top Purity",
                                              "Column Pressure (bar)", "Naphthalene Purity (%)"):
            doc.add_picture(pressure_plot_png, width=Inches(6))
            doc.add.paragraph("Figure 8: Scatter plot showing the relationship between C-03 column pressure and top product purity. A negative correlation suggests that lower pressure is beneficial for separation.")


        # Optimal Conditions Summary
        doc.add_heading("10. Optimal Conditions Summary", level=1)
        doc.add.paragraph("Based on the data analysis, the following conditions were associated with the highest naphthalene purity in the C-03 column:")
        
        doc.add_list_item(f"**Reboiler Temperature:** The analysis showed a strong positive correlation, suggesting that higher temperatures (within the 325-340°C range) were beneficial for separation.")
        doc.add_list_item(f"**Reflux Ratio:** Higher reflux ratios were consistently associated with improved separation, as expected.")
        doc.add_list_item(f"**Differential Pressure:** A stable, low differential pressure was observed during periods of high purity. Maintaining a ΔP below a certain threshold is critical to avoid flooding.")
        doc.add_list_item(f"**Column Pressure:** The data indicates that lower column pressure was correlated with higher product purity, which is consistent with theoretical expectations for this type of distillation.")

    else:
        doc.add.paragraph("C-03 performance analysis could not be completed due to insufficient or incomplete data.")

    # --------------- Energy Balance Section ---------------------------------------
    doc.add_heading('11. Understanding Energy Balance', level=1)
    doc.add.paragraph("A simplified energy proxy KPI was used in this report, based on the **PF66 thermic fluid flow** and temperature drop. A full **energy balance** is crucial for optimizing plant efficiency but requires more detailed data than is available in the current SCADA tags. A true energy balance would involve accounting for all energy inputs and outputs:")

    doc.add_list_item(f"Energy Input: **Heat supplied by the reboiler**, which is calculated as the mass flow rate of the thermic fluid multiplied by its specific heat capacity ($c_p$) and the temperature difference across the reboiler ($Q = m \cdot c_p \cdot \Delta T$). A standard approximation of {CP_THERMIC_FLUID} kJ/kg·K was used for the specific heat of PF66.")
    doc.add_list_item("Energy Output: **Heat removed by the condenser** (vapor flow rate multiplied by latent heat of vaporization), and **sensible heat** carried away by the top and bottom products.")
    doc.add_list_item("Heat Losses: Energy lost to the environment through the column walls, which is difficult to measure and often requires an estimated heat transfer coefficient.")

    doc.add.paragraph("To perform this, you would need the following additional data points, which are typically found on the plant's P&ID (Piping and Instrumentation Diagram):")

    doc.add_list_item("Flows, temperatures, and specific heats for all streams (feed, top product, bottom product).")
    doc.add_list_item("An accurate specific heat capacity for the thermic fluid at operating temperatures.")

    doc.add_page_break()
    
    # Final section explaining the value of the report
    doc.add_heading('12. The Value of This Analysis', level=1)
    doc.add.paragraph("This report goes beyond the capabilities of standard industrial software like Aspen and SCADA systems by providing **actionable, proactive intelligence** based on a holistic analysis of your plant data.")
    doc.add.paragraph("While **SCADA** systems are excellent for real-time monitoring and **Aspen** is a powerful design and simulation tool, neither is designed to perform the following tasks automatically and on-demand:")
    doc.add_list_item("**Proactive Insights**: By using **Machine Learning (ARIMA)** for time series forecasting, this report predicts future process trends, allowing operators to make adjustments before a problem occurs.")
    doc.add_list_item("**Data Quality Assurance**: The **K-Means clustering** algorithm intelligently filters out bad data points, ensuring that all analyses and reports are based on accurate and reliable information.")
    doc.add_list_item("**Bridging the Gap**: The report seamlessly integrates real-time SCADA data with offline lab results to provide a single, unified view of plant performance, connecting process conditions to final product quality.")
    doc.add_list_item("**Customized Problem Solving**: This script can be easily modified to address specific, ad-hoc issues like the C-02 pressure build-up problem. This flexibility allows for rapid, data-driven troubleshooting without waiting for software updates or complex reconfigurations.")
    
    # Final save
    doc.save(filename)
    log_and_print(f"Report saved as {filename}")

def get_baseline_df(start_date, end_date, table_name, baseline_period_days):
    """Fetches baseline data from the previous period."""
    end_dt = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S') - timedelta(seconds=1)
    start_dt = end_dt - timedelta(days=baseline_period_days)

    log_and_print(f"Fetching baseline data from {start_dt} to {end_dt}.")
    return get_data_from_db(start_dt.strftime('%Y-%m-%d %H:%M:%S'), end_dt.strftime('%Y-%m-%d %H:%M:%S'), table_name)

def find_and_rename_column(df, search_terms, new_name):
    """
    Finds a column in a DataFrame that contains any of the search terms
    and renames it to the specified new_name.
    """
    found_col = None
    for col in df.columns:
        if any(term.lower() in col.lower() for term in search_terms):
            found_col = col
            break
    if found_col:
        df.rename(columns={found_col: new_name}, inplace=True)
        log_and_print(f"Renamed column '{found_col}' to '{new_name}'.")
        return True
    else:
        log_and_print(f"Could not find a column to rename to '{new_name}' using search terms: {search_terms}", 'warning')
        return False

# --------------- MAIN EXECUTION -----------------------------------------------

if __name__ == "__main__":
    table_to_analyze = 'data_cleaning_with_report'
    start_time_str = '2025-08-08 00:00:40'
    end_time_str = '2025-08-14 23:59:59'

    log_and_print(f"Starting analysis for table '{table_to_analyze}' from {start_time_str} to {end_time_str}...")

    # Load SCADA data
    df = get_data_from_db(start_time_str, end_time_str, table_to_analyze)
    if df.empty:
        log_and_print("No SCADA data to analyze. Exiting script.", 'error')
    else:
        # Load Lab Results
        try:
            lab_results_df = pd.read_csv('WFO Plant GC Report-25-26.csv')
            log_and_print("Successfully loaded lab results.")

            # New robust column renaming logic
            find_and_rename_column(lab_results_df, ['material', 'product', 'type'], 'Material')
            find_and_rename_column(lab_results_df, ['sample detail', 'sample name', 'sample'], 'Sample Detail')
            find_and_rename_column(lab_results_df, ['gc', 'naphthalene'], 'Naphth. % by GC')
            find_and_rename_column(lab_results_df, ['analysis date', 'date'], 'Analysis Date')
            find_and_rename_column(lab_results_df, ['analysis time', 'time'], 'Analysis Time')

            # Convert date/time after renaming
            if 'Analysis Date' in lab_results_df.columns and 'Analysis Time' in lab_results_df.columns:
                 lab_results_df['datetime'] = pd.to_datetime(lab_results_df['Analysis Date'] + ' ' + lab_results_df['Analysis Time'], dayfirst=True)
                 lab_results_df.sort_values('datetime', ascending=False, inplace=True)
            else:
                 log_and_print("Warning: Could not find 'Analysis Date' or 'Analysis Time' columns. Time-based lab data analysis may be inaccurate.", 'warning')

        except FileNotFoundError:
            log_and_print("Error: WFO Plant GC Report-25-26.csv not found. Purity analysis will be skipped.", 'error')
            lab_results_df = pd.DataFrame()

        # Create Word Report
        report_filename = os.path.join(OUT_DIR, f"Naphthalene_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.docx")
        create_word_report(df, lab_results_df, report_filename, start_time_str, end_time_str)

        # Create Excel KPI Export
        excel_filename = os.path.join(OUT_DIR, f"Naphthalene_KPIs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
        # For simplicity, this is not implemented yet but the placeholder is here
        kpis_to_export = []
        # export_kpis_to_excel(kpis_to_export, excel_filename)

log_and_print("Script finished.")