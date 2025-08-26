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
DB_HOST = "localhost"
DB_NAME = "scada_data_analysis"
DB_USER = "postgres"
DB_PASSWORD = "ADMIN"

# Define units for each tag
TAG_UNITS = {
    'FT-01': 'kg/h',
    'FT-02': 'kg/h',
    'FT-03': 'kg/h',
    'FT-05': 'kg/h',
    'FT-06': 'kg/h',
    'FT-09': 'kg/h',
    'FT-61': 'kg/h',
    'FT-62': 'kg/h',
    'FI-103': 'kg/h',
    'FI-202': 'kg/h',
    'TI-11': 'degC',
    'TI-13': 'degC',
    'TI-14': 'degC',
    'TI-15': 'degC',
    'TI-16': 'degC',
    'TI-17': 'degC',
    'TI-18': 'degC',
    'TI-19': 'degC',
    'TI-20': 'degC',
    'TI-21': 'degC',
    'TI-22': 'degC',
    'TI-23': 'degC',
    'TI-24': 'degC',
    'TI-25': 'degC',
    'TI-26': 'degC',
    'TI-27': 'degC',
    'TI-28': 'degC',
    'TI-29': 'degC',
    'TI-30': 'degC',
    'TI-72A': 'degC',
    'TI-72B': 'degC',
    'PTT-02': 'mmHg',
    'PTB-02': 'mmHg',
    'DIFFERENTIAL_PRESSURE': 'mmHg',
    'LI-03': '%',
    'REBOILER_HEAT_DUTY': 'kW',
    'CONDENSER_HEAT_DUTY': 'kW',
    'REFLUX_RATIO': '',
    'MATERIAL_BALANCE_ERROR': '%',
    'NAPHTHALENE_LOSS_PERCENTAGE': '%'
}

# File paths for saving generated plots and report
output_report_path = "C-02_Analysis_Report.docx"
output_temp_plot_path = "C-02_Temperature_Profile.png"
output_dp_plot_path = "C-02_Differential_Pressure.png"
output_trends_plot_path = "C-02_Daily_Trends.png"
output_naphthalene_vs_reflux_plot_path = "C-02_Naphthalene_vs_Reflux.png"
output_naphthalene_vs_reboiler_temp_plot_path = "C-02_Naphthalene_vs_Reboiler_Temp.png"
output_feed_vs_dp_plot_path = "C-02_Feed_vs_DP.png"

# Engineering constants for heat duty calculations
THERMIC_FLUID_SPECIFIC_HEAT = 2.0  # kJ/(kg·°C) - Assumed value
WATER_SPECIFIC_HEAT = 4.186        # kJ/(kg·°C)

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
    """Retrieves specific SCADA data for the C-02 column and related streams."""
    try:
        desired_columns = [
            "DateAndTime", "FT-01", "FT-02", "FT-03", "FT-05", "FT-06", "FT-09", "FT-61", "FT-62", "TI-11", "TI-13", "TI-14", "TI-15", "TI-16",
            "TI-17", "TI-18", "TI-19", "TI-20", "TI-21", "TI-22", "TI-23", "TI-24", "TI-25", "TI-26",
            "TI-27", "TI-28", "TI-29", "TI-30", "TI-72A", "TI-72B", "PTT-02", "PTB-02", "LI-03",
            "FI-103", "FI-202"
        ]
        
        start_date = '2025-08-08 00:00:00'
        end_date = '2025-08-20 23:59:59'

        inspector = sqlalchemy.inspect(engine)
        columns = inspector.get_columns('wide_scada_data')
        column_names = [col['name'] for col in columns]
        
        final_columns = []
        for d_col in desired_columns:
            for db_col in column_names:
                if d_col.lower().replace('-', '') == db_col.lower().replace('-', ''):
                    final_columns.append(f'"{db_col}"')
                    break
        
        if not final_columns:
            print("Error: No matching columns found. Data retrieval failed.")
            return None, start_date, end_date

        select_clause = ", ".join(final_columns)
        query = f"""
        SELECT {select_clause}
        FROM wide_scada_data
        WHERE "DateAndTime" BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY "DateAndTime";
        """
        
        df = pd.read_sql(query, engine)
        df.columns = [col.upper().replace('-', '_') for col in df.columns]
        df['DATEANDTIME'] = pd.to_datetime(df['DATEANDTIME'])
        print("SCADA data for process streams retrieved successfully.")
        return df, start_date, end_date
    except Exception as e:
        print(f"Error retrieving SCADA data: {e}")
        return None, None, None

def get_composition_data():
    """
    Simulates reading composition data from a lab analysis report based on user feedback.
    Returns a dictionary of compositions for each component at specific sample points.
    """
    try:
        composition_data = {
            'Naphthalene': {
                'P-01': 0.1,    
                'C-01-B': 0.02, 
                'C-02-T': 0.08,
            },
            'Thianaphthene': {
                'P-01': 0.05,
                'C-01-B': 0.01,
                'C-02-T': 0.04
            },
            'Quinoline': {
                'P-01': 0.03,
                'C-01-B': 0.02,
                'C-02-T': 0.01
            },
            'Unknown Impurity': {
                'P-01': 0.01,
                'C-01-B': 0.005,
                'C-02-T': 0.002
            },
            'Moisture': {
                'P-01': 0.15 
            }
        }
        return composition_data
    except Exception as e:
        print(f"Error simulating composition data: {e}. Using default values.")
        return None

def perform_analysis(df):
    """
    Performs key calculations for the C-02 column and the overall process,
    including staged material balances.
    """
    if df is None or df.empty:
        return {}, df, {}

    analysis_results = {}
    composition_data = get_composition_data()
    
    # Clean and convert data to numeric
    for col in ['FT_01', 'FT_02', 'FT_03', 'FT_05', 'FT_06', 'FT_09', 'FT_61', 'FT_62', 'FI_103', 'FI_202', 'TI_72A', 'TI_72B', 'PTT_02', 'PTB_02', 'TI_26', 'TI_11']:
        if col in df.columns:
            df.loc[:, col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Calculate average flows for the period
    avg_flows = {tag.replace('-', '_').upper(): df[tag.replace('-', '_').upper()].mean() for tag in ['FT-01', 'FT-61', 'FT-62', 'FT-05', 'FT-02', 'FT-03', 'FT-06', 'FT-09']}

    # --- Staged Material Balance Calculations ---
    analysis_results['Component-wise Material Balance'] = {}
    
    # C-00 (Dehydration) Balance
    input_flow_c00 = avg_flows['FT_01']
    output_flow_c00 = avg_flows['FT_62'] + avg_flows['FT_61']
    c00_balance_error = ((input_flow_c00 - output_flow_c00) / input_flow_c00) * 100 if input_flow_c00 > 0 else 0
    analysis_results['C-00 Overall Material Balance Error (%)'] = abs(c00_balance_error)

    # C-01 Naphthalene Loss Calculation (Corrected logic)
    if composition_data and 'Naphthalene' in composition_data:
        # Calculate C-01 feed composition from C-00 feed (P-01) by removing moisture
        c01_feed_comp_naphthalene = composition_data['Naphthalene']['P-01'] / (1 - composition_data['Moisture']['P-01'])
        
        # Calculate naphthalene mass in C-01 feed
        naphthalene_mass_in_c01_feed = avg_flows['FT_62'] * c01_feed_comp_naphthalene

        # Calculate naphthalene mass in C-01 bottom product
        naphthalene_mass_in_c01_bottom = avg_flows['FT_05'] * composition_data['Naphthalene']['C-01-B']
        
        # Calculate Naphthalene loss percentage
        if naphthalene_mass_in_c01_feed > 0:
            naphthalene_loss_percent_c01 = (naphthalene_mass_in_c01_bottom / naphthalene_mass_in_c01_feed) * 100
        else:
            naphthalene_loss_percent_c01 = 0
            
        analysis_results['Naphthalene Loss in C-01 (%)'] = naphthalene_loss_percent_c01
        
        if naphthalene_loss_percent_c01 > 2:
            analysis_results['C-01 Naphthalene Loss Status'] = "ALERT: Naphthalene loss is above the 2% limit."
        else:
            analysis_results['C-01 Naphthalene Loss Status'] = "Naphthalene loss is within acceptable limits."

    # C-02 Material Balance & Other KPIs
    if all(tag in df.columns for tag in ['FT_02', 'FT_03', 'FT_06']):
        feed_flow_avg = avg_flows['FT_02']
        top_product_flow_avg = avg_flows['FT_03']
        bottom_product_flow_avg = avg_flows['FT_06']
        
        if feed_flow_avg > 0:
            material_balance_error = ((feed_flow_avg - (top_product_flow_avg + bottom_product_flow_avg)) / feed_flow_avg) * 100
            analysis_results['C-02 Overall Material Balance Error (%)'] = abs(material_balance_error)

    # Reflux Ratio (C-02)
    if 'FT_09' in df.columns and 'FT_03' in df.columns:
        df['REFLUX_RATIO'] = df['FT_09'] / df['FT_03']
        df.loc[df['FT_03'] == 0, 'REFLUX_RATIO'] = 0
        df['REFLUX_RATIO'] = df['REFLUX_RATIO'].abs()
        analysis_results['Average Reflux Ratio'] = df['REFLUX_RATIO'].mean()
    else:
        analysis_results['Average Reflux Ratio'] = "N/A (Missing data)"
            
    # Differential Pressure (DP) Calculation
    if 'PTT_02' in df.columns and 'PTB_02' in df.columns:
        df['DIFFERENTIAL_PRESSURE'] = df['PTB_02'] - df['PTT_02']
        analysis_results['Average Differential Pressure'] = df['DIFFERENTIAL_PRESSURE'].mean()
        analysis_results['Maximum Differential Pressure'] = df['DIFFERENTIAL_PRESSURE'].max()
        
    # Energy Balance (C-02 specific)
    if all(tag in df.columns for tag in ['TI_72A', 'TI_72B', 'FI_202']):
        df['REBOILER_HEAT_DUTY'] = df['FI_202'] * THERMIC_FLUID_SPECIFIC_HEAT * (df['TI_72B'] - df['TI_72A'])
        analysis_results['Average Reboiler Heat Duty'] = df['REBOILER_HEAT_DUTY'].mean()

    if all(tag in df.columns for tag in ['TI_26', 'TI_11', 'FI_103']):
        df['CONDENSER_HEAT_DUTY'] = df['FI_103'] * WATER_SPECIFIC_HEAT * (df['TI_26'] - df['TI_11'])
        analysis_results['Average Condenser Heat Duty'] = df['CONDENSER_HEAT_DUTY'].mean()

    # Create new columns for plotting based on user request
    if 'FT_03' in df.columns and composition_data and 'Naphthalene' in composition_data and 'C-02-T' in composition_data['Naphthalene']:
        df['NAPHTHALENE_IN_C02_TOP_PROD_MASS'] = df['FT_03'] * composition_data['Naphthalene']['C-02-T']
        analysis_results['Naphthalene in C-02 Top Product (%)'] = composition_data['Naphthalene']['C-02-T'] * 100
    
    return analysis_results, df, {}

def generate_plots(df):
    """Generates and saves temperature profile, DP, and energy plots."""
    
    # Check if necessary columns exist for plotting
    if df is None or df.empty:
        print("Dataframe is empty, cannot generate plots.")
        return

    # Helper function to generate a plot
    def plot_and_save(x_data, y_data, title, xlabel, ylabel, filename, is_scatter=False):
        plt.figure(figsize=(10, 6))
        if is_scatter:
            plt.scatter(x_data, y_data, alpha=0.5)
        else:
            plt.plot(x_data, y_data, alpha=0.7)
        plt.title(title)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(filename)
        plt.close()
        print(f"Plot saved to {filename}")
        
    # Temperature Profile Plot
    if 'DATEANDTIME' in df.columns:
        temp_tags = ['TI_13', 'TI_14', 'TI_15', 'TI_16', 'TI_17', 'TI_18', 'TI_19', 'TI_20', 'TI_21', 'TI_22', 'TI_23', 'TI_24', 'TI_25']
        
        plt.figure(figsize=(10, 6))
        df.sort_values(by='DATEANDTIME', inplace=True)
        x_axis = df['DATEANDTIME']
        
        for tag in temp_tags:
            if tag in df.columns:
                plt.plot(x_axis, df[tag], label=tag.replace('_', '-'), alpha=0.7)
        
        plt.title("C-02 Column Temperature Profile Over Time")
        plt.xlabel("Date and Time")
        plt.ylabel(f"Temperature ({TAG_UNITS['TI-13']})")
        plt.legend(ncol=2)
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(output_temp_plot_path)
        plt.close()
        print(f"Temperature profile plot saved to {output_temp_plot_path}")

    # Differential Pressure Plot
    if 'DIFFERENTIAL_PRESSURE' in df.columns:
        plot_and_save(df['DATEANDTIME'], df['DIFFERENTIAL_PRESSURE'], 
                      "C-02 Differential Pressure Over Time", "Date and Time", 
                      f"Differential Pressure ({TAG_UNITS['DIFFERENTIAL_PRESSURE']})", 
                      output_dp_plot_path)

    # Daily Trends Plot
    df['DATE'] = df['DATEANDTIME'].dt.date
    daily_trends = df.groupby('DATE').agg({
        'FT_03': 'mean',
        'TI_28': 'mean', 
        'DIFFERENTIAL_PRESSURE': 'mean'
    }).reset_index()
    
    plt.figure(figsize=(12, 8))
    plt.plot(daily_trends['DATE'], daily_trends['FT_03'], label=f"Avg Top Product Flow ({TAG_UNITS['FT-03']})")
    plt.plot(daily_trends['DATE'], daily_trends['TI_28'], label=f"Avg Top Product Temp ({TAG_UNITS['TI-28']})")
    plt.plot(daily_trends['DATE'], daily_trends['DIFFERENTIAL_PRESSURE'], label=f"Avg DP ({TAG_UNITS['DIFFERENTIAL_PRESSURE']})")
    plt.title("C-02 Daily Trends")
    plt.xlabel("Date")
    plt.ylabel("Value")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(output_trends_plot_path)
    plt.close()
    print(f"Daily trends plot saved to {output_trends_plot_path}")

    # New Plots requested by user
    # Naphthalene Loss vs. Reflux Ratio (C-02)
    if 'NAPHTHALENE_IN_C02_TOP_PROD_MASS' in df.columns and 'REFLUX_RATIO' in df.columns:
        plot_and_save(df['REFLUX_RATIO'], df['NAPHTHALENE_IN_C02_TOP_PROD_MASS'], 
                      "C-02 Naphthalene Loss vs. Reflux Ratio", 
                      "Reflux Ratio", "Naphthalene Mass in Top Product (kg/h)", 
                      output_naphthalene_vs_reflux_plot_path, is_scatter=True)

    # Naphthalene Loss vs. Reboiler Temperature (C-02)
    if 'NAPHTHALENE_IN_C02_TOP_PROD_MASS' in df.columns and 'TI_72B' in df.columns:
        plot_and_save(df['TI_72B'], df['NAPHTHALENE_IN_C02_TOP_PROD_MASS'], 
                      "C-02 Naphthalene Loss vs. Reboiler Temperature", 
                      f"Reboiler Temperature ({TAG_UNITS['TI-72B']})", "Naphthalene Mass in Top Product (kg/h)", 
                      output_naphthalene_vs_reboiler_temp_plot_path, is_scatter=True)
                      
    # Feed vs. Differential Pressure (C-02)
    if 'FT_02' in df.columns and 'DIFFERENTIAL_PRESSURE' in df.columns:
        plot_and_save(df['FT_02'], df['DIFFERENTIAL_PRESSURE'], 
                      "C-02 Feed vs. Differential Pressure", 
                      f"Feed Flow (FT-02) ({TAG_UNITS['FT-02']})", 
                      f"Differential Pressure ({TAG_UNITS['DIFFERENTIAL_PRESSURE']})", 
                      output_feed_vs_dp_plot_path, is_scatter=True)
        
def generate_word_report(analysis_results, df, outliers, start_date, end_date):
    """Creates a detailed analysis report in a Word document."""
    doc = Document()
    doc.add_heading('C-02 Light Oil Recovery Column Analysis Report', 0)
    doc.add_paragraph(f"Analysis Period: {start_date} to {end_date}")
    doc.add_paragraph(f"Report Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Section 1: Executive Summary
    doc.add_heading('1. Executive Summary', level=1)
    
    summary_text = ""
    if 'Average Reflux Ratio' in analysis_results and isinstance(analysis_results['Average Reflux Ratio'], (float, int)):
        summary_text += f"The column operated with an average reflux ratio of {analysis_results['Average Reflux Ratio']:.2f}, indicating effective control over product separation. "
    
    if 'C-02 Overall Material Balance Error (%)' in analysis_results:
        summary_text += f"The C-02 column had a material balance error of {analysis_results['C-02 Overall Material Balance Error (%)']:.2f}%. "
    
    if 'Naphthalene Loss in C-01 (%)' in analysis_results:
        summary_text += f"The naphthalene loss in the C-01 column was calculated to be {analysis_results['Naphthalene Loss in C-01 (%)']:.2f}%, which is "
        if analysis_results['Naphthalene Loss in C-01 (%)'] > 2:
             summary_text += "above the acceptable limit. "
        else:
             summary_text += "within the acceptable limit. "

    if 'Naphthalene in C-02 Top Product (%)' in analysis_results:
        summary_text += f"Naphthalene concentration in the C-02 top product was found to be {analysis_results['Naphthalene in C-02 Top Product (%)']:.2f}%. "
    
    doc.add_paragraph(summary_text)

    # Section 2: Key Performance Indicators
    doc.add_heading('2. Key Performance Indicators (KPIs)', level=1)
    doc.add_paragraph("All values are averages over the analysis period.")
    for key, value in analysis_results.items():
        if key.startswith(('C-00', 'C-01', 'Component')) or key.endswith('Status'):
            continue
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

    # Section 3: Material Balance Analysis
    doc.add_heading('3. Material Balance Analysis', level=1)
    
    doc.add_heading('3.1 C-00 and C-01 Material Balance', level=2)
    if 'C-00 Overall Material Balance Error (%)' in analysis_results:
        doc.add_paragraph(f"• C-00 Overall Material Balance Error: {analysis_results['C-00 Overall Material Balance Error (%)']:.2f}%")
    if 'Naphthalene Loss in C-01 (%)' in analysis_results:
        doc.add_paragraph(f"• Naphthalene Loss in C-01: {analysis_results['Naphthalene Loss in C-01 (%)']:.2f}%")
    if 'C-01 Naphthalene Loss Status' in analysis_results:
        doc.add_paragraph(f"   - {analysis_results['C-01 Naphthalene Loss Status']}")

    doc.add_heading('3.2 C-02 Material Balance', level=2)
    doc.add_paragraph(f"• C-02 Overall Material Balance Error: {analysis_results.get('C-02 Overall Material Balance Error (%)', 'N/A'):.2f}%")
    
    doc.add_heading('3.3 Component-wise Balance', level=2)
    doc.add_paragraph("This section details the material balance for key components across the C-01 and C-02 system.")
    if 'Component-wise Material Balance' in analysis_results:
        comp_balance = analysis_results['Component-wise Material Balance']
        for comp_key, comp_value in comp_balance.items():
            doc.add_paragraph(f"• {comp_key}: {comp_value:.2f}%")

    # Section 4: Performance Plots
    doc.add_heading('4. Performance Plots', level=1)
    
    # New plots
    doc.add_heading('4.1 Naphthalene in Top Product vs. Reflux Ratio', level=2)
    doc.add_picture(output_naphthalene_vs_reflux_plot_path, width=Inches(6))

    doc.add_heading('4.2 Naphthalene in Top Product vs. Reboiler Temperature', level=2)
    doc.add_picture(output_naphthalene_vs_reboiler_temp_plot_path, width=Inches(6))

    doc.add_heading('4.3 Feed vs. Differential Pressure', level=2)
    doc.add_picture(output_feed_vs_dp_plot_path, width=Inches(6))

    # Existing plots
    doc.add_heading('4.4 Temperature Profile', level=2)
    doc.add_paragraph("The temperature profile plot shows the gradient across the column.")
    doc.add_picture(output_temp_plot_path, width=Inches(6))

    doc.add_heading('4.5 Differential Pressure (DP)', level=2)
    doc.add_paragraph("Differential pressure is a key indicator of flooding or fouling.")
    doc.add_picture(output_dp_plot_path, width=Inches(6))

    doc.add_heading('4.6 Daily Trends', level=2)
    doc.add_paragraph("This plot shows the daily average trends of key variables.")
    doc.add_picture(output_trends_plot_path, width=Inches(6))

    doc.save(output_report_path)
    print(f"Analysis report generated successfully at {output_report_path}")

def main():
    """Main execution function."""
    engine = connect_to_database()
    if engine is None:
        return

    scada_data, start_date, end_date = get_scada_data(engine)
    if scada_data is None:
        return

    analysis_results, scada_data, outliers = perform_analysis(scada_data)
    
    if analysis_results:
        generate_plots(scada_data)
        generate_word_report(analysis_results, scada_data, outliers, start_date, end_date)
        print("C-02 analysis complete.")
    else:
        print("Analysis failed: no data to process.")

if __name__ == "__main__":
    main()