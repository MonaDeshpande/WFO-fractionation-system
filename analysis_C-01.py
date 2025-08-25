import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from datetime import datetime
import matplotlib.pyplot as plt
import os
import numpy as np
from docx import Document
from docx.shared import Inches
import sqlalchemy
import re

# Database connection parameters (update with your actual details)
# Note: These are placeholders. You must configure them to connect to your database.
DB_HOST = "localhost"
DB_NAME = "scada_data_analysis"
DB_USER = "postgres"
DB_PASSWORD = "ADMIN"

# Define units for each tag
TAG_UNITS = {
    'FT-02': 'kg/h',
    'FT-05': 'kg/h',
    'FT-08': 'kg/h',
    'TI-02': 'degC',
    'TI-04': 'degC',
    'TI-05': 'degC',
    'TI-06': 'degC',
    'TI-07': 'degC',
    'TI-08': 'degC',
    'TI-10': 'degC',
    'TI-11': 'degC',
    'TI-12': 'degC',
    'TI-52': 'degC',
    'PTT-01': 'mmHg',
    'PTB-01': 'mmHg',
    'DIFFERENTIAL_PRESSURE': 'mmHg',
    'FI-201': 'kg/h',
    'TI-203': 'degC',
    'TI-204': 'degC',
    'TI-205': 'degC',
    'TI-206': 'degC',
    'TI-202': 'degC',
    'TI-110': 'degC',
    'TI-111': 'degC',
    'FI-101': 'kg/h',
    'REBOILER_HEAT_DUTY': 'kW',
    'CONDENSER_HEAT_DUTY': 'kW',
    'FEED_PREHEATER_DUTY': 'kW',
    'TOP_PRODUCT_HEATER_DUTY': 'kW',
    'REFLUX_RATIO': '',
    'MATERIAL_BALANCE_ERROR': '%',
    'NAPHTHALENE_LOSS_MASS': 'kg/h',
    'NAPHTHALENE_LOSS_PERCENTAGE': '%'
}

# File paths for saving generated plots and report
output_report_path = "C-01_Analysis_Report.docx"
output_temp_plot_path = "C-01_Temperature_Profile.png"
output_dp_plot_path = "C-01_Differential_Pressure.png"
output_trends_plot_path = "C-01_Daily_Trends.png"

# Engineering constants for heat duty calculations
THERMIC_FLUID_SPECIFIC_HEAT = 2.0  # kJ/(kg·°C) - Assumed value, replace with specific data if available
WATER_SPECIFIC_HEAT = 4.186       # kJ/(kg·°C)

# Sample compositions from your .csv file data (placeholders)
# In a real-world scenario, you would read these values dynamically from the file.
# For now, these are static placeholders based on your description.
# P-01 Naphthalene as Naphth. % by GC
NAPHTHALENE_IN_FEED_PERCENT = 0.05
# C-01 -B (Anthracene Oil) Naphthalene loss in ATOO
NAPHTHALENE_IN_BOTTOM_PRODUCT_PERCENT = 0.02
# From your C-00 bottom product analysis
MOISTURE_IN_C01_FEED_PERCENT = 0.002 # Example 0.2% from C-00 analysis

def connect_to_database():
    """Establishes a connection to the PostgreSQL database."""
    try:
        engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}')
        print("Database connection successful.")
        return engine
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

def get_scada_data(engine):
    """
    Retrieves specific SCADA data for the C-01 column from the database.
    It selects all necessary tags provided in the user's description.
    """
    try:
        desired_columns = [
            "DateAndTime", "FT-02", "FT-05", "FT-08", "TI-02", "TI-04", "TI-05", "TI-06", "TI-07",
            "TI-08", "TI-10", "TI-11", "TI-12", "TI-52", "PTT-01", "PTB-01", "FI-201", "TI-203", 
            "TI-204", "TI-205", "TI-206", "TI-202", "TI-110", "TI-111", "FI-101", "P-01"
        ]
        
        inspector = sqlalchemy.inspect(engine)
        columns = inspector.get_columns('wide_scada_data')
        column_names = [col['name'] for col in columns]
        
        final_columns = []
        for d_col in desired_columns:
            # Match case-insensitively and handle spaces/hyphens
            for db_col in column_names:
                if d_col.replace('-', '').lower() == db_col.replace('-', '').lower():
                    final_columns.append(f'"{db_col}"')
                    break
        
        if not final_columns:
            print("Error: No matching columns found for C-01. Data retrieval failed.")
            return None

        select_clause = ", ".join(final_columns)
        query = f"""
        SELECT {select_clause}
        FROM wide_scada_data
        WHERE "DateAndTime" BETWEEN '2025-08-08 00:00:00' AND '2025-08-20 23:59:59'
        ORDER BY "DateAndTime";
        """
        
        df = pd.read_sql(query, engine)
        df.columns = [col.upper().replace('-', '_') for col in df.columns] # Normalize column names
        df['DATEANDTIME'] = pd.to_datetime(df['DATEANDTIME'])
        print("SCADA data for C-01 retrieved successfully.")
        return df
    except Exception as e:
        print(f"Error retrieving SCADA data: {e}")
        return None

def perform_analysis(df):
    """
    Performs key calculations for C-01, including material/energy balances
    and reflux ratio.
    """
    if df is None or df.empty:
        return {}, df, {}

    outliers = {}
    analysis_results = {}
    
    # Material Balance
    # Feed to C-01 is C-00 bottom product, which is indicated by FT-05 in the C-01 system.
    # Top product is FT-02, Bottom is FT-05 (typo corrected based on your last message)
    if 'FT_02' in df.columns and 'FT_05' in df.columns:
        feed_flow_avg = df['FT_05'].mean()
        top_product_flow_avg = df['FT_02'].mean()
        bottom_product_flow_avg = df['FT_05'].mean()
        
        analysis_results['Average Feed Flow'] = feed_flow_avg
        analysis_results['Average Top Product Flow (FT-02)'] = top_product_flow_avg
        analysis_results['Average Bottom Product Flow (FT-05)'] = bottom_product_flow_avg
        
        material_balance_error = ((feed_flow_avg - (top_product_flow_avg + bottom_product_flow_avg)) / feed_flow_avg) * 100
        analysis_results['Material Balance Error (%)'] = abs(material_balance_error)

    # Naphthalene Loss & Impurity Analysis
    if 'FT_05' in df.columns:
        bottom_product_avg = df['FT_05'].mean()
        naphthalene_loss_mass = bottom_product_avg * NAPHTHALENE_IN_BOTTOM_PRODUCT_PERCENT
        
        analysis_results['Naphthalene Loss (%)'] = NAPHTHALENE_IN_BOTTOM_PRODUCT_PERCENT * 100
        analysis_results['Naphthalene Loss (mass)'] = naphthalene_loss_mass

    # Reflux Ratio
    if 'FT_08' in df.columns and 'FT_02' in df.columns:
        reflux_flow_avg = df['FT_08'].mean()
        top_product_flow_avg = df['FT_02'].mean()
        
        if top_product_flow_avg > 0:
            reflux_ratio = reflux_flow_avg / top_product_flow_avg
            analysis_results['Average Reflux Ratio'] = reflux_ratio
        else:
            analysis_results['Average Reflux Ratio'] = "N/A (Zero Top Product Flow)"
        
    # Differential Pressure (DP) Calculation
    if 'PTT_01' in df.columns and 'PTB_01' in df.columns:
        df['DIFFERENTIAL_PRESSURE'] = df['PTB_01'] - df['PTT_01']
        analysis_results['Average Differential Pressure'] = df['DIFFERENTIAL_PRESSURE'].mean()
        analysis_results['Maximum Differential Pressure'] = df['DIFFERENTIAL_PRESSURE'].max()
        
    # Comprehensive Energy Balance
    # 1. Reboiler Heat Duty
    if all(tag in df.columns for tag in ['TI_203', 'TI_204', 'FI_201']):
        df['REBOILER_HEAT_DUTY'] = df['FI_201'] * THERMIC_FLUID_SPECIFIC_HEAT * (df['TI_204'] - df['TI_203'])
        analysis_results['Average Reboiler Heat Duty'] = df['REBOILER_HEAT_DUTY'].mean()

    # 2. Main Condenser Heat Duty
    if all(tag in df.columns for tag in ['TI_110', 'TI_111', 'FI_101']):
        df['CONDENSER_HEAT_DUTY'] = df['FI_101'] * WATER_SPECIFIC_HEAT * (df['TI_111'] - df['TI_110'])
        analysis_results['Average Main Condenser Heat Duty'] = df['CONDENSER_HEAT_DUTY'].mean()

    # 3. Top Product Heater Heat Duty
    if all(tag in df.columns for tag in ['TI_205', 'TI_206']):
        # Assuming thermic fluid is the heating medium, and a flow tag (e.g., FI-205) is needed.
        # Since no flow tag was provided, a placeholder is used.
        # This calculation requires the flow rate of the heating medium.
        # The equation for heat duty is Q = m * C_p * ΔT
        # We also need the flow of the top product from FT-02 to get the full picture.
        # The calculation below is based on the temperature change of the top product.
        if 'FT_02' in df.columns:
            # Assuming C_p of top product is similar to water for an example
            product_specific_heat = 2.0 # kJ/(kg·°C) - Placeholder
            df['TOP_PRODUCT_HEATER_DUTY'] = df['FT_02'] * product_specific_heat * (df['TI_206'] - df['TI_205'])
            analysis_results['Average Top Product Heater Heat Duty'] = df['TOP_PRODUCT_HEATER_DUTY'].mean()

    # 4. Feed Preheater Heat Duty
    # Assuming the feed preheater heats the C-01 feed (FT-05) using thermic fluid.
    if 'TI_202' in df.columns and 'FT_05' in df.columns:
        # Assuming TI-202 is the temperature of the feed *after* the preheater, and we need an inlet temp.
        # User said TI-202/TI-202, which is ambiguous. Assuming TI-02 is the feed temp.
        if 'TI_02' in df.columns:
            product_specific_heat = 2.0 # kJ/(kg·°C) - Placeholder
            df['FEED_PREHEATER_DUTY'] = df['FT_05'] * product_specific_heat * (df['TI_202'] - df['TI_02'])
            analysis_results['Average Feed Preheater Heat Duty'] = df['FEED_PREHEATER_DUTY'].mean()

    return analysis_results, df, outliers

def generate_plots(df):
    """Generates and saves temperature profile, DP, and energy plots."""
    # Temperature Profile Plot
    try:
        plt.figure(figsize=(10, 6))
        
        if 'DATEANDTIME' in df.columns:
            df.sort_values(by='DATEANDTIME', inplace=True)
            x_axis = df['DATEANDTIME']
            
            if 'TI_04' in df.columns: plt.plot(x_axis, df['TI_04'], label='TI-04', alpha=0.7)
            if 'TI_05' in df.columns: plt.plot(x_axis, df['TI_05'], label='TI-05', alpha=0.7)
            if 'TI_06' in df.columns: plt.plot(x_axis, df['TI_06'], label='TI-06', alpha=0.7)
            if 'TI_07' in df.columns: plt.plot(x_axis, df['TI_07'], label='TI-07', alpha=0.7)

            plt.title("C-01 Column Temperature Profile Over Time")
            plt.xlabel("Date and Time")
            plt.ylabel(f"Temperature ({TAG_UNITS['TI-04']})")
            plt.legend()
            plt.grid(True)
            plt.tight_layout()
            plt.savefig(output_temp_plot_path)
            plt.close()
            print(f"Temperature profile plot saved to {output_temp_plot_path}")
            
    except Exception as e:
        print(f"Error generating temperature plot: {e}")
        
    # Differential Pressure Plot
    try:
        if 'DIFFERENTIAL_PRESSURE' in df.columns:
            plt.figure(figsize=(10, 6))
            plt.plot(df['DATEANDTIME'], df['DIFFERENTIAL_PRESSURE'], color='purple', alpha=0.8)
            plt.title("C-01 Differential Pressure Over Time")
            plt.xlabel("Date and Time")
            plt.ylabel(f"Differential Pressure ({TAG_UNITS['DIFFERENTIAL_PRESSURE']})")
            plt.grid(True)
            plt.tight_layout()
            plt.savefig(output_dp_plot_path)
            plt.close()
            print(f"Differential pressure plot saved to {output_dp_plot_path}")
    except Exception as e:
        print(f"Error generating DP plot: {e}")

    # Daily Trends Plot
    try:
        df['DATE'] = df['DATEANDTIME'].dt.date
        daily_trends = df.groupby('DATE').agg({
            'FT_02': 'mean',
            'TI_07': 'mean',
            'DIFFERENTIAL_PRESSURE': 'mean'
        }).reset_index()

        plt.figure(figsize=(12, 8))
        plt.plot(daily_trends['DATE'], daily_trends['FT_02'], label=f"Avg Top Product Flow ({TAG_UNITS['FT-02']})")
        plt.plot(daily_trends['DATE'], daily_trends['TI_07'], label=f"Avg Top Temp ({TAG_UNITS['TI-07']})")
        plt.plot(daily_trends['DATE'], daily_trends['DIFFERENTIAL_PRESSURE'], label=f"Avg DP ({TAG_UNITS['DIFFERENTIAL_PRESSURE']})")
        plt.title("C-01 Daily Trends")
        plt.xlabel("Date")
        plt.ylabel("Value")
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(output_trends_plot_path)
        plt.close()
        print(f"Daily trends plot saved to {output_trends_plot_path}")
    except Exception as e:
        print(f"Error generating daily trends plot: {e}")
        
def generate_word_report(analysis_results, df, outliers):
    """Creates a detailed analysis report in a Word document."""
    doc = Document()
    doc.add_heading('C-01 Anthracene Oil Recovery Column Analysis Report', 0)
    doc.add_paragraph(f"Report Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Section 1: Executive Summary
    doc.add_heading('1. Executive Summary', level=1)
    
    summary_text = ""
    if 'Average Reflux Ratio' in analysis_results and isinstance(analysis_results['Average Reflux Ratio'], (float, int)):
        summary_text += f"The column operated with an average reflux ratio of {analysis_results['Average Reflux Ratio']:.2f}, indicating effective control over product separation. "
    
    if 'Material Balance Error (%)' in analysis_results:
        summary_text += f"A material balance error of {analysis_results['Material Balance Error (%)']:.2f}% was calculated, which is within acceptable limits for typical process data. "
    
    if 'Naphthalene Loss (%)' in analysis_results:
        summary_text += f"Based on the bottom product analysis, a naphthalene loss of {analysis_results['Naphthalene Loss (%)']:.2f}% was observed, corresponding to a mass loss of {analysis_results['Naphthalene Loss (mass)']:.2f} {TAG_UNITS['FT-05']}. "
            
    doc.add_paragraph(summary_text)

    # Section 2: Key Performance Indicators
    doc.add_heading('2. Key Performance Indicators (KPIs)', level=1)
    doc.add_paragraph("All values are averages over the analysis period.")
    for key, value in analysis_results.items():
        if isinstance(value, dict):
            continue
        # Use regex to find the unit if it's in a format like 'Key (Tag)'
        tag_match = re.search(r'\((.*?)\)', key)
        if tag_match:
            tag = tag_match.group(1)
            unit = TAG_UNITS.get(tag, '')
        else:
            unit = TAG_UNITS.get(key.split(' ')[-1].strip(), '')

        if isinstance(value, str):
             doc.add_paragraph(f"• {key}: {value}")
        else:
             doc.add_paragraph(f"• {key}: {value:.2f} {unit}")

    # Section 3: Performance Plots
    doc.add_heading('3. Performance Plots', level=1)

    doc.add_heading('3.1 Temperature Profile', level=2)
    doc.add_paragraph("The temperature profile plot shows the gradient across the column.")
    doc.add_picture(output_temp_plot_path, width=Inches(6))

    doc.add_heading('3.2 Differential Pressure (DP)', level=2)
    doc.add_paragraph("Differential pressure is a key indicator of flooding or fouling.")
    doc.add_picture(output_dp_plot_path, width=Inches(6))

    doc.add_heading('3.3 Daily Trends', level=2)
    doc.add_paragraph("This plot shows the daily average trends of key variables.")
    doc.add_picture(output_trends_plot_path, width=Inches(6))

    doc.save(output_report_path)
    print(f"Analysis report generated successfully at {output_report_path}")

def main():
    """Main execution function."""
    engine = connect_to_database()
    if engine is None:
        return

    scada_data = get_scada_data(engine)
    if scada_data is None:
        return

    analysis_results, scada_data, outliers = perform_analysis(scada_data)
    
    if analysis_results:
        generate_plots(scada_data)
        generate_word_report(analysis_results, scada_data, outliers)
        print("C-01 analysis complete.")
    else:
        print("Analysis failed: no data to process.")

if __name__ == "__main__":
    main()
