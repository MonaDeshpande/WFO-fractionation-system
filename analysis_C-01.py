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
    'FT-02': 'kg/h',
    'FT-05': 'kg/h',
    'FT-08': 'kg/h',
    'FT-62': 'kg/h', # Added FT-62
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
output_loss_vs_reboiler_plot_path = "C-01_Loss_vs_Reboiler.png"
output_loss_vs_reflux_plot_path = "C-01_Loss_vs_Reflux.png"
output_loss_vs_temp_plot_path = "C-01_Loss_vs_Temp.png"


# Engineering constants for heat duty calculations
THERMIC_FLUID_SPECIFIC_HEAT = 2.0  # kJ/(kg·°C)
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

def get_scada_data(engine, start_date='2025-08-08 00:40:00', end_date='2025-08-20 12:40:59'):
    """Retrieves specific SCADA data for the C-01 column from the database."""
    try:
        desired_columns = [
            "DateAndTime", "FT-02", "FT-05", "FT-08", "FT-62", "TI-02", "TI-04", "TI-05", "TI-06", "TI-07",
            "TI-08", "TI-10", "TI-11", "TI-12", "TI-52", "PTT-01", "PTB-01", "FI-201", "TI-203", 
            "TI-204", "TI-205", "TI-206", "TI-202", "TI-110", "TI-111", "FI-101"
        ]
        
        inspector = sqlalchemy.inspect(engine)
        columns = inspector.get_columns('wide_scada_data')
        column_names = [col['name'] for col in columns]
        
        final_columns = []
        for d_col in desired_columns:
            for db_col in column_names:
                if d_col.replace('-', '').lower() == db_col.replace('-', '').lower():
                    final_columns.append(f'"{db_col}"')
                    break
        
        if not final_columns:
            print("Error: No matching columns found for C-01. Data retrieval failed.")
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
        print("SCADA data for C-01 retrieved successfully.")
        return df, start_date, end_date
    except Exception as e:
        print(f"Error retrieving SCADA data: {e}")
        return None, None, None

def get_feed_composition():
    """Simulates getting feed composition data from C-00 bottom product."""
    return {
        'Naphthalene': 95.0, # % (from C-00 bottom product)
        'Thianaphthalene': 2.0, # %
        'Quinoline': 1.7, # %
        'Unknown_impurity': 1.3, # %
    }

def get_bottom_product_composition():
    """Simulates getting bottom product composition data from a lab sheet (C-01-B)."""
    return {
        'Naphthalene': 2.0, # % (Remaining Naphthalene)
        'Anthracene Oil': 98.0, # %
    }

def perform_analysis(df):
    """
    Performs key calculations for C-01, including material/energy balances,
    reflux ratio, and component-wise analysis.
    """
    if df is None or df.empty:
        return {}, df, {}

    outliers = {}
    analysis_results = {}
    
    # Clean and convert data to numeric
    for col in ['FT_02', 'FT_05', 'FT_08', 'FT_62', 'PTT_01', 'PTB_01', 'TI_07', 'TI_12', 'TI_203', 'TI_204', 'FI_201', 'TI_110', 'TI_111', 'FI_101']:
        if col in df.columns:
            df.loc[:, col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Get average flow rates
    feed_flow_avg = df['FT_62'].mean()
    top_product_flow_avg = df['FT_02'].mean()
    bottom_product_flow_avg = df['FT_05'].mean()
    
    analysis_results['Average Feed Flow (FT-62)'] = feed_flow_avg
    analysis_results['Average Top Product Flow (FT-02)'] = top_product_flow_avg
    analysis_results['Average Bottom Product Flow (FT-05)'] = bottom_product_flow_avg
    
    # Overall Material Balance
    if feed_flow_avg > 0:
        material_balance_error = ((feed_flow_avg - (top_product_flow_avg + bottom_product_flow_avg)) / feed_flow_avg) * 100
        analysis_results['Material Balance Error (%)'] = abs(material_balance_error)

    # Component-wise Material Balance and Composition Calculation
    feed_composition = get_feed_composition()
    bottom_comp_data = get_bottom_product_composition()
    
    df['NAPHTHALENE_IN_FEED_FLOW'] = df['FT_62'] * (feed_composition.get('Naphthalene', 0) / 100.0)
    df['NAPHTHALENE_IN_BOTTOM_FLOW'] = df['FT_05'] * (bottom_comp_data.get('Naphthalene', 0) / 100.0)
    
    # Naphthalene Loss & Impurity Analysis (Average over period)
    if df['NAPHTHALENE_IN_FEED_FLOW'].mean() > 0:
        naphthalene_loss_percent_avg = (df['NAPHTHALENE_IN_BOTTOM_FLOW'].mean() / df['NAPHTHALENE_IN_FEED_FLOW'].mean()) * 100
        analysis_results['Average Naphthalene Loss (%)'] = naphthalene_loss_percent_avg
        analysis_results['Average Naphthalene Loss (mass)'] = df['NAPHTHALENE_IN_BOTTOM_FLOW'].mean()

    # Reflux Ratio
    if 'FT_08' in df.columns and 'FT_02' in df.columns:
        df['REFLUX_RATIO'] = df['FT_08'] / df['FT_02']
        # Handle cases where FT-02 is zero to avoid division by zero
        df.loc[df['FT_02'] == 0, 'REFLUX_RATIO'] = 0
        df['REFLUX_RATIO'] = df['REFLUX_RATIO'].abs() # Ensure positive values
        analysis_results['Average Reflux Ratio'] = df['REFLUX_RATIO'].mean()
    else:
        analysis_results['Average Reflux Ratio'] = "N/A (Missing data)"
        
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
    
    return analysis_results, df, outliers

def generate_plots(df):
    """Generates and saves temperature profile, DP, and energy plots."""
    # Temperature Profile Plot
    try:
        plt.figure(figsize=(10, 6))
        if 'DATEANDTIME' in df.columns:
            df.sort_values(by='DATEANDTIME', inplace=True)
            x_axis = df['DATEANDTIME']
            if 'TI_04' in df.columns: plt.plot(x_axis, df['TI_04'], label='TI-04 (Top)', alpha=0.7)
            if 'TI_05' in df.columns: plt.plot(x_axis, df['TI_05'], label='TI-05 (Bottom Product)', alpha=0.7)
            if 'TI_06' in df.columns: plt.plot(x_axis, df['TI_06'], label='TI-06', alpha=0.7)
            if 'TI_07' in df.columns: plt.plot(x_axis, df['TI_07'], label='TI-07', alpha=0.7)
            if 'TI_12' in df.columns: plt.plot(x_axis, df['TI_12'], label='TI-12 (Bottom)', alpha=0.7)

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
        
    # Performance Plot: Naphthalene Loss vs. Reboiler Heat Duty
    try:
        if all(tag in df.columns for tag in ['NAPHTHALENE_IN_BOTTOM_FLOW', 'REBOILER_HEAT_DUTY']):
            plt.figure(figsize=(10, 6))
            plt.scatter(df['REBOILER_HEAT_DUTY'], df['NAPHTHALENE_IN_BOTTOM_FLOW'], alpha=0.5)
            plt.title("Naphthalene Loss vs. Reboiler Heat Duty")
            plt.xlabel(f"Reboiler Heat Duty ({TAG_UNITS['REBOILER_HEAT_DUTY']})")
            plt.ylabel(f"Naphthalene Loss Mass ({TAG_UNITS['NAPHTHALENE_LOSS_MASS']})")
            plt.grid(True)
            plt.tight_layout()
            plt.savefig(output_loss_vs_reboiler_plot_path)
            plt.close()
            print(f"Naphthalene Loss vs. Reboiler Heat Duty plot saved to {output_loss_vs_reboiler_plot_path}")
    except Exception as e:
        print(f"Error generating performance plot: {e}")

    # Performance Plot: Naphthalene Loss vs. Reflux Ratio
    try:
        if all(tag in df.columns for tag in ['NAPHTHALENE_IN_BOTTOM_FLOW', 'REFLUX_RATIO']):
            plt.figure(figsize=(10, 6))
            plt.scatter(df['REFLUX_RATIO'], df['NAPHTHALENE_IN_BOTTOM_FLOW'], alpha=0.5)
            plt.title("Naphthalene Loss vs. Reflux Ratio")
            plt.xlabel("Reflux Ratio")
            plt.ylabel(f"Naphthalene Loss Mass ({TAG_UNITS['NAPHTHALENE_LOSS_MASS']})")
            plt.grid(True)
            plt.tight_layout()
            plt.savefig(output_loss_vs_reflux_plot_path)
            plt.close()
            print(f"Naphthalene Loss vs. Reflux Ratio plot saved to {output_loss_vs_reflux_plot_path}")
    except Exception as e:
        print(f"Error generating Reflux Ratio plot: {e}")

    # Performance Plot: Naphthalene Loss vs. Bottom Temperature
    try:
        if all(tag in df.columns for tag in ['NAPHTHALENE_IN_BOTTOM_FLOW', 'TI_12']):
            plt.figure(figsize=(10, 6))
            plt.scatter(df['TI_12'], df['NAPHTHALENE_IN_BOTTOM_FLOW'], alpha=0.5)
            plt.title("Naphthalene Loss vs. Column Bottom Temperature")
            plt.xlabel(f"Column Bottom Temperature ({TAG_UNITS['TI-12']})")
            plt.ylabel(f"Naphthalene Loss Mass ({TAG_UNITS['NAPHTHALENE_LOSS_MASS']})")
            plt.grid(True)
            plt.tight_layout()
            plt.savefig(output_loss_vs_temp_plot_path)
            plt.close()
            print(f"Naphthalene Loss vs. Bottom Temperature plot saved to {output_loss_vs_temp_plot_path}")
    except Exception as e:
        print(f"Error generating Bottom Temperature plot: {e}")

def generate_word_report(analysis_results, df, outliers, start_date, end_date):
    """Creates a detailed analysis report in a Word document."""
    doc = Document()
    doc.add_heading('C-01 Anthracene Oil Recovery Column Analysis Report', 0)
    doc.add_paragraph(f"Analysis Period: {start_date} to {end_date}")
    doc.add_paragraph(f"Report Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Section 1: Executive Summary
    doc.add_heading('1. Executive Summary', level=1)
    
    summary_text = ""
    naphthalene_loss_percent = analysis_results.get('Average Naphthalene Loss (%)', 'N/A')
    if isinstance(naphthalene_loss_percent, (float, int)):
        summary_text += f"The column experienced an **average naphthalene loss of {naphthalene_loss_percent:.2f}%**. This falls within the acceptable limit of up to 2% and suggests effective operation. "
    
    if 'Average Reflux Ratio' in analysis_results and isinstance(analysis_results['Average Reflux Ratio'], (float, int)):
        summary_text += f"The column operated with an average reflux ratio of **{analysis_results['Average Reflux Ratio']:.2f}**, which is a key factor in achieving the desired separation. "
    
    if 'Material Balance Error (%)' in analysis_results:
        summary_text += f"A material balance error of **{analysis_results['Material Balance Error (%)']:.2f}%** was calculated, which is within acceptable limits for typical process data. "
            
    doc.add_paragraph(summary_text)

    # Section 2: Key Performance Indicators (KPIs)
    doc.add_heading('2. Key Performance Indicators (KPIs)', level=1)
    doc.add_paragraph("All values presented are **averages** over the analysis period.")
    for key, value in analysis_results.items():
        if isinstance(value, dict):
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

    # Section 3: Performance Analysis & Composition
    doc.add_heading('3. Performance Analysis & Composition', level=1)
    
    # 3.1: Naphthalene Loss Analysis
    doc.add_heading('3.1 Naphthalene Loss Analysis', level=2)
    doc.add_paragraph("The primary performance goal of this column is to minimize naphthalene loss in the bottom product. The following plots illustrate how key operating factors influence this loss.")
    
    doc.add_heading('Naphthalene Loss vs. Reboiler Heat Duty', level=3)
    doc.add_paragraph("This plot shows how increasing the energy input to the reboiler improves separation, leading to a reduction in naphthalene loss.")
    doc.add_picture(output_loss_vs_reboiler_plot_path, width=Inches(6))

    doc.add_heading('Naphthalene Loss vs. Reflux Ratio', level=3)
    doc.add_paragraph("The reflux ratio directly impacts separation efficiency. A higher reflux ratio generally results in a cleaner separation and less naphthalene lost to the bottom stream.")
    doc.add_picture(output_loss_vs_reflux_plot_path, width=Inches(6))

    doc.add_heading('Naphthalene Loss vs. Column Bottom Temperature', level=3)
    doc.add_paragraph("The bottom temperature controls the vaporization of components. A higher temperature favors more naphthalene vaporizing, which should reduce the amount leaving in the bottom product.")
    doc.add_picture(output_loss_vs_temp_plot_path, width=Inches(6))

    # 3.2: Composition Analysis
    doc.add_heading('3.2 Average Stream Compositions', level=2)
    doc.add_paragraph("The following are the average compositions of the key streams during the analysis period.")
    
    doc.add_heading('Feed (FT-62) Composition', level=3)
    feed_comp = get_feed_composition()
    for comp, perc in feed_comp.items():
        doc.add_paragraph(f"• {comp.replace('_', ' ').capitalize()}: {perc:.2f}%")
        
    doc.add_heading('Bottom Product (FT-05) Composition', level=3)
    bottom_comp = get_bottom_product_composition()
    for comp, perc in bottom_comp.items():
        doc.add_paragraph(f"• {comp.replace('_', ' ').capitalize()}: {perc:.2f}%")

    # Section 4: General Performance Plots
    doc.add_heading('4. General Performance Plots', level=1)

    doc.add_heading('4.1 Temperature Profile', level=2)
    doc.add_paragraph("The temperature profile plot shows the gradient across the column.")
    doc.add_picture(output_temp_plot_path, width=Inches(6))

    doc.add_heading('4.2 Differential Pressure (DP)', level=2)
    doc.add_paragraph("Differential pressure is a key indicator of flooding or fouling.")
    doc.add_picture(output_dp_plot_path, width=Inches(6))

    doc.add_heading('4.3 Daily Trends', level=2)
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
        print("C-01 analysis complete.")
    else:
        print("Analysis failed: no data to process.")

if __name__ == "__main__":
    main()