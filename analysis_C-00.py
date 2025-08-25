# Import necessary libraries
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os # For checking if files exist

# --- Reusable Configuration ---
# Database connection details
DB_CONFIG = {
    'host': 'localhost',
    'name': 'scada_data_analysis',
    'user': 'postgres',
    'pass': 'ADMIN',
    'port': '5432'
}
DATABASE_URL = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['pass']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['name']}"

# Time range for analysis
START_TIME = '2025-08-08 00:00:40'
END_TIME = '2025-08-20 12:40:59'

# SCADA data table name and GC file path
SCADA_TABLE = 'wide_scada_data'
GC_FILE = 'WFO_Plant_GC_Report-25-26.csv'

# Instrument tags for Column C-00
C00_TAGS = {
    'feed_flow': 'FT-01',
    'top_flow': 'FT-61',
    'bottom_flow': 'FT-62',
    'feed_temp': 'TI-01',
    'top_temp': 'TI-65',
    'dp_top_pressure': 'PTT-04',
    'dp_bottom_pressure': 'PTB-04',
    'reboiler_in_temp': 'TI-215',
    'reboiler_out_temp': 'TI-216',
    'column_temp_profile': ['TI-61', 'TI-62', 'TI-63', 'TI-64', 'TI-65']
}

# --- Data Retrieval and Processing Functions ---

def get_scada_data(table_name, start_time, end_time):
    """Retrieves high-frequency SCADA data from the database."""
    print("Connecting to database and retrieving SCADA data...")
    try:
        engine = create_engine(DATABASE_URL)
        query = f"SELECT * FROM {table_name} WHERE timestamp BETWEEN '{start_time}' AND '{end_time}' ORDER BY timestamp;"
        df = pd.read_sql(query, engine)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)
        print("SCADA data retrieved successfully.")
        return df
    except Exception as e:
        print(f"Error connecting to the database or retrieving data: {e}")
        return None

def get_gc_data(file_path):
    """
    Reads and processes the GC report, filtering for C-00 feed samples.
    Creates dummy data if the file is not found for demonstration.
    """
    print(f"Reading and processing GC report data from {file_path}...")
    try:
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}. Generating dummy GC data for demonstration.")
            dummy_gc = {'Analysis Date': ['07.08.25', '07.08.25', '08.08.25', '08.08.25'],
                        'Analysis Time': ['09.15AM', '06.00PM', '06.00AM', '09.30AM'],
                        'Sample Detail': ['P-01', 'P-01', 'C-03-T', 'P-01'],
                        'Mois. %': [0.2, 0.2, 0.7, 0.2],
                        'Naphth. % by GC': [56.53, 55.52, 88.94, 56.38],
                        'Thianaphth. %': [2.04, 2.02, 4.22, 2.03],
                        'Quinoline in ppm': [17459, 17442, 6582, 18189],
                        'Unknown Impurity%': [1.83, 1.84, 1.7, 1.9]}
            gc_df = pd.DataFrame(dummy_gc)
        else:
            gc_df = pd.read_csv(file_path)

        gc_df['DateTime'] = pd.to_datetime(gc_df['Analysis Date'] + ' ' + gc_df['Analysis Time'], format='%d.%m.%y %I.%M%p')
        feed_gc_data = gc_df[gc_df['Sample Detail'] == 'P-01'].copy()
        feed_gc_data.set_index('DateTime', inplace=True)
        
        feed_gc_data.rename(columns={
            'Naphth. % by GC': 'Naphthalene_pct',
            'Thianaphth. %': 'Thianaphthalene_pct',
            'Quinoline in ppm': 'Quinoline_ppm',
            'Mois. %': 'Moisture_pct', # Assuming this is water %
            'Unknown Impurity%': 'Unknown_Impurity_pct'
        }, inplace=True)
        print("GC data processed successfully.")
        return feed_gc_data[['Moisture_pct', 'Naphthalene_pct', 'Thianaphthalene_pct', 'Quinoline_ppm', 'Unknown_Impurity_pct']]
    except Exception as e:
        print(f"Error reading or processing the CSV file: {e}")
        return None

def integrate_data(scada_df, gc_df):
    """
    Merges SCADA and GC data using a backward-fill method. This aligns the
    constant drum composition with the high-frequency process data.
    """
    if scada_df is None or gc_df is None:
        print("Data integration failed due to missing dataframes.")
        return None
    print("Integrating high-resolution SCADA and GC data...")
    
    integrated_df = pd.merge_asof(
        scada_df.sort_index(), 
        gc_df.sort_index(), 
        left_index=True, 
        right_index=True, 
        direction='backward'
    )
    integrated_df.ffill(inplace=True) 
    print("Data integration complete.")
    return integrated_df

# --- Analysis and Plotting Functions ---

def perform_c00_analysis(df):
    """
    Performs the core analysis for the C-00 column: material balance,
    efficiency, and differential pressure.
    """
    if df is None or df.empty:
        print("Cannot perform analysis, input DataFrame is empty.")
        return None

    print("Starting C-00 column analysis...")
    
    df['material_balance_deviation'] = df[C00_TAGS['feed_flow']] - (df[C00_TAGS['top_flow']] + df[C00_TAGS['bottom_flow']])

    df['water_in_feed'] = df[C00_TAGS['feed_flow']] * (df['Moisture_pct'] / 100)
    df['water_removed'] = df[C00_TAGS['top_flow']]
    df['Efficiency'] = (df['water_removed'] / df['water_in_feed']) * 100
    df['Efficiency'] = df['Efficiency'].clip(upper=100)

    df['differential_pressure'] = df[C00_TAGS['dp_bottom_pressure']] - df[C00_TAGS['dp_top_pressure']]

    print("Analysis complete.")
    return df

def generate_plots(df):
    """Generates and saves plots for the analysis."""
    if df is None or df.empty:
        print("Cannot generate plots, DataFrame is empty.")
        return

    sns.set_style("whitegrid")
    
    # Plot 1: Flow Rates and Material Balance
    plt.figure(figsize=(15, 6))
    plt.plot(df.index, df[C00_TAGS['feed_flow']], label='Feed Flow (FT-01)', color='b')
    plt.plot(df.index, df[C00_TAGS['top_flow']], label='Top Flow (FT-61)', color='g')
    plt.plot(df.index, df[C00_TAGS['bottom_flow']], label='Bottom Flow (FT-62)', color='r')
    plt.title('C-00 Column Material Balance Over Time')
    plt.xlabel('Date/Time')
    plt.ylabel('Flow Rate (kg/min)')
    plt.legend()
    plt.tight_layout()
    plt.savefig('C00_material_balance.png')
    plt.show()

    # Plot 2: Dehydration Efficiency
    plt.figure(figsize=(15, 6))
    plt.plot(df.index, df['Efficiency'], label='Dehydration Efficiency (%)', color='purple')
    plt.title('C-00 Column Dehydration Efficiency Over Time')
    plt.xlabel('Date/Time')
    plt.ylabel('Efficiency (%)')
    plt.ylim(0, 105)
    plt.legend()
    plt.tight_layout()
    plt.savefig('C00_dehydration_efficiency.png')
    plt.show()

    # Plot 3: Differential Pressure vs. Feed Flow
    fig, ax1 = plt.subplots(figsize=(15, 6))
    ax1.set_title('Differential Pressure vs. Feed Flow')
    ax1.set_xlabel('Date/Time')
    ax1.set_ylabel('Differential Pressure (bar)', color='orange')
    ax1.plot(df.index, df['differential_pressure'], label='Differential Pressure', color='orange')
    ax1.tick_params(axis='y', labelcolor='orange')
    
    ax2 = ax1.twinx()
    ax2.set_ylabel('Feed Flow (kg/min)', color='blue')
    ax2.plot(df.index, df[C00_TAGS['feed_flow']], label='Feed Flow', color='blue', linestyle='--')
    ax2.tick_params(axis='y', labelcolor='blue')
    fig.tight_layout()
    plt.savefig('C00_differential_pressure.png')
    plt.show()
    
    # Plot 4: Temperature Profile
    plt.figure(figsize=(15, 6))
    for temp_tag in C00_TAGS['column_temp_profile']:
        plt.plot(df.index, df[temp_tag], label=temp_tag)
    plt.title('C-00 Column Temperature Profile (Bottom to Top)')
    plt.xlabel('Date/Time')
    plt.ylabel('Temperature (deg C)')
    plt.legend()
    plt.tight_layout()
    plt.savefig('C00_temperature_profile.png')
    plt.show()

# --- Main Execution Block ---

if __name__ == "__main__":
    scada_data = get_scada_data(SCADA_TABLE, START_TIME, END_TIME)
    gc_data = get_gc_data(GC_FILE)
    
    if scada_data is not None and gc_data is not None:
        integrated_data = integrate_data(scada_data, gc_data)
        
        if 'Unknown_Impurity_pct' in integrated_data.columns:
            print("\nUnknown Impurity data has been successfully integrated.")
        
        analyzed_data = perform_c00_analysis(integrated_data)
        
        if analyzed_data is not None:
            analyzed_data.to_csv('analyzed_c00_data.csv')
            print("Analyzed data saved to 'analyzed_c00_data.csv'.")
            
            generate_plots(analyzed_data)

            print("\n" + "="*50)
            print("C-00 COLUMN ANALYSIS SUMMARY REPORT")
            print("="*50)
            
            avg_efficiency = analyzed_data['Efficiency'].mean()
            avg_feed_flow = analyzed_data[C00_TAGS['feed_flow']].mean()
            avg_dp = analyzed_data['differential_pressure'].mean()
            
            print(f"1. Average Dehydration Efficiency: {avg_efficiency:.2f}%")
            print(f"2. Average Feed Flow Rate: {avg_feed_flow:.2f} kg/min")
            print(f"3. Average Differential Pressure: {avg_dp:.2f} bar")
            
            print("\nNote: The 'analyzed_c00_data.csv' file now includes the 'Unknown Impurity%' data. This is crucial for future analyses of the downstream columns and for correlating it with C-00's performance.")
            print("="*50)
    else:
        print("Data preparation failed. Please check your file paths and database connection.")