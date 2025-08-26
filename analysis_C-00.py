import pandas as pd
from sqlalchemy import inspect, create_engine
import psycopg2
from datetime import datetime
import matplotlib.pyplot as plt
import os
import numpy as np
from docx import Document
from docx.shared import Inches
import re

# Database connection parameters (update with your actual details)
DB_HOST = "localhost"
DB_NAME = "scada_data_analysis"
DB_USER = "postgres"
DB_PASSWORD = "ADMIN"

# Define units for each tag
TAG_UNITS = {
    'FT-01': 'kg/h',
    'FT-61': 'kg/h',
    'FT-62': 'kg/h',
    'TI-01': 'degC',
    'TI-61': 'degC',
    'TI-63': 'degC',
    'TI-64': 'degC',
    'PTT-04': 'mmHg',
    'PTB-04': 'mmHg',
    'DIFFERENTIAL_PRESSURE': 'mmHg',
    'TI-215': 'degC',
    'TI-216': 'degC',
    'TI-110': 'degC',
    'FI-101': 'm3/h',
    'FI-204': 'm3/h',
    'REBOILER_HEAT_DUTY': 'kW',
    'CONDENSER_HEAT_DUTY': 'kW',
    'Moisture Removal Percentage': '%'
}

# File paths
output_report_path = "C-00_Analysis_Report.docx"
output_temp_plot_path = "C-00_Temperature_Profile.png"
output_dp_plot_path = "C-00_Differential_Pressure.png"
output_trends_plot_path = "C-00_Daily_Trends.png"
output_performance_plot_path = "C-00_Performance_Curve.png"
output_temp_performance_plot_path = "C-00_Performance_vs_Temp.png"
output_flow_performance_plot_path = "C-00_Performance_vs_Flow.png"

# Engineering constants
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

def get_scada_data(engine, start_date='2025-08-08 00:00:00', end_date='2025-08-20 23:59:59'):
    """Retrieves specific SCADA data for the C-00 column within a date range."""
    try:
        desired_columns = [
            "DateAndTime", "FT-01", "FT-61", "FT-62", "TI-01", "PTT-04", "PTB-04", 
            "TI-215", "TI-216", "TI-110", "TI-61", "TI-63", "TI-64", "FI-101", "FI-204"
        ]
        
        inspector = inspect(engine)
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
        print("SCADA data for C-00 retrieved successfully.")
        return df, start_date, end_date
    except Exception as e:
        print(f"Error retrieving SCADA data: {e}")
        return None, None, None

def get_moisture_percentage(start_date, end_date, default_moisture_percent=0.2):
    """
    Simulates reading moisture data from a CSV or returns a default value.
    This function would typically read from a file within the specified date range.
    """
    try:
        # Simulate reading moisture data from a CSV
        moisture_data = {
            'Date': pd.to_datetime(['2025-08-08', '2025-08-09', '2025-08-10', '2025-08-11', '2025-08-12', 
                                     '2025-08-13', '2025-08-14', '2025-08-15', '2025-08-16', '2025-08-17']),
            'Moisture_Percent': [0.21, 0.19, 0.22, 0.20, 0.23, 0.21, 0.20, 0.22, 0.21, 0.19]
        }
        df_moisture = pd.DataFrame(moisture_data)
        df_moisture['Date'] = pd.to_datetime(df_moisture['Date']).dt.date
        
        # Filter data for the analysis period
        start_date_dt = pd.to_datetime(start_date).date()
        end_date_dt = pd.to_datetime(end_date).date()
        filtered_moisture = df_moisture[(df_moisture['Date'] >= start_date_dt) & (df_moisture['Date'] <= end_date_dt)]

        if not filtered_moisture.empty:
            avg_moisture = filtered_moisture['Moisture_Percent'].mean()
            print(f"Average moisture content from simulated file: {avg_moisture:.2f}%")
            return avg_moisture
        else:
            print(f"Moisture data for the period not found. Using default value: {default_moisture_percent:.2f}%")
            return default_moisture_percent
            
    except Exception as e:
        print(f"Error reading moisture data: {e}. Using default value: {default_moisture_percent:.2f}%")
        return default_moisture_percent

def perform_analysis(df, start_date, end_date):
    """Calculates key performance indicators for the C-00 column."""
    if df is None or df.empty:
        return {}, df, {}
    
    outliers = {}
    analysis_results = {}
    
    # Anomaly Detection and Filtering for TI_63
    if 'TI_63' in df.columns:
        mean_ti63 = df['TI_63'].mean()
        std_ti63 = df['TI_63'].std()
        outlier_mask = np.abs(df['TI_63'] - mean_ti63) > (5 * std_ti63)
        if outlier_mask.any():
            outlier_time = df.loc[outlier_mask, 'DATEANDTIME'].iloc[0]
            outliers['TI_63'] = {'time': outlier_time.strftime('%Y-%m-%d %H:%M'), 'value': df.loc[outlier_mask, 'TI_63'].iloc[0]}
            df.loc[outlier_mask, 'TI_63'] = np.nan
    
    # Material Balance
    if all(tag in df.columns for tag in ['FT_01', 'FT_61', 'FT_62']):
        df.loc[:, 'FT_01'] = pd.to_numeric(df['FT_01'], errors='coerce').fillna(0)
        df.loc[:, 'FT_61'] = pd.to_numeric(df['FT_61'], errors='coerce').fillna(0)
        df.loc[:, 'FT_62'] = pd.to_numeric(df['FT_62'], errors='coerce').fillna(0)

        feed_flow_avg = df['FT_01'].mean()
        moisture_flow_avg = df['FT_61'].mean()
        bottom_product_flow_avg = df['FT_62'].mean()
        
        analysis_results['Average Feed Flow (FT-01)'] = feed_flow_avg
        analysis_results['Average Moisture Flow (FT-61)'] = moisture_flow_avg
        analysis_results['Average Bottom Product Flow (FT-62)'] = bottom_product_flow_avg
        
        if feed_flow_avg > 0:
            material_balance_error = ((feed_flow_avg - (moisture_flow_avg + bottom_product_flow_avg)) / feed_flow_avg) * 100
            analysis_results['Overall Material Balance Error (%)'] = abs(material_balance_error)

    # Moisture Removal Analysis
    if 'FT_01' in df.columns and 'FT_61' in df.columns:
        moisture_in_feed_percent = get_moisture_percentage(start_date, end_date)
        df['MOISTURE_IN_FEED_FLOW'] = df['FT_01'] * (moisture_in_feed_percent / 100)
        
        df.loc[df['MOISTURE_IN_FEED_FLOW'] > 0, 'MOISTURE_REMOVAL_PERCENTAGE'] = (df['FT_61'] / df['MOISTURE_IN_FEED_FLOW']) * 100
        
        # Correct for values over 100%
        df['MOISTURE_REMOVAL_PERCENTAGE'] = df['MOISTURE_REMOVAL_PERCENTAGE'].apply(lambda x: min(x, 100) if pd.notna(x) else x)

        analysis_results['Average Moisture Content in Feed (%)'] = moisture_in_feed_percent
        analysis_results['Average Moisture Removal Percentage'] = df['MOISTURE_REMOVAL_PERCENTAGE'].mean()

    # Differential Pressure (DP) Calculation
    if all(tag in df.columns for tag in ['PTT_04', 'PTB_04']):
        df['DIFFERENTIAL_PRESSURE'] = df['PTB_04'] - df['PTT_04']
        analysis_results['Average Differential Pressure'] = df['DIFFERENTIAL_PRESSURE'].mean()
        analysis_results['Maximum Differential Pressure'] = df['DIFFERENTIAL_PRESSURE'].max()
        
    # Energy Balance
    if all(tag in df.columns for tag in ['TI_215', 'TI_216', 'FI_204']):
        df['REBOILER_HEAT_DUTY'] = df['FI_204'] * THERMIC_FLUID_SPECIFIC_HEAT * (df['TI_216'] - df['TI_215'])
        analysis_results['Average Reboiler Heat Duty'] = df['REBOILER_HEAT_DUTY'].mean()
    else:
        analysis_results['Average Reboiler Heat Duty'] = 'N/A (Missing data)'

    if all(tag in df.columns for tag in ['TI_110', 'FI_101']):
        df['CONDENSER_HEAT_DUTY'] = df['FI_101'] * WATER_SPECIFIC_HEAT * (25 - df['TI_110'])
        analysis_results['Average Condenser Heat DUTY'] = df['CONDENSER_HEAT_DUTY'].mean()
    else:
        analysis_results['Average Condenser Heat DUTY'] = 'N/A (Missing data)'

    # Calculate and store correlation coefficients for performance factors
    analysis_results['Performance Factors'] = {}
    if all(tag in df.columns for tag in ['MOISTURE_REMOVAL_PERCENTAGE', 'REBOILER_HEAT_DUTY']):
        correlation = df['MOISTURE_REMOVAL_PERCENTAGE'].corr(df['REBOILER_HEAT_DUTY'])
        analysis_results['Performance Factors']['Moisture Removal vs. Reboiler Heat Duty Correlation'] = correlation

    if all(tag in df.columns for tag in ['MOISTURE_REMOVAL_PERCENTAGE', 'TI_01']):
        correlation = df['MOISTURE_REMOVAL_PERCENTAGE'].corr(df['TI_01'])
        analysis_results['Performance Factors']['Moisture Removal vs. Feed Temperature Correlation'] = correlation

    if all(tag in df.columns for tag in ['MOISTURE_REMOVAL_PERCENTAGE', 'FT_61']):
        correlation = df['MOISTURE_REMOVAL_PERCENTAGE'].corr(df['FT_61'])
        analysis_results['Performance Factors']['Moisture Removal vs. Moisture Flow (FT-61) Correlation'] = correlation
        
    if all(tag in df.columns for tag in ['DIFFERENTIAL_PRESSURE', 'FT_01']):
        correlation = df['DIFFERENTIAL_PRESSURE'].corr(df['FT_01'])
        analysis_results['Performance Factors']['DP vs. Feed Flow Correlation'] = correlation
        
    return analysis_results, df, outliers

def generate_plots(df):
    """Generates and saves temperature profile, DP, daily trends, and performance plots."""
    
    plot_created_flags = {}

    # Temperature Profile Plot
    try:
        plt.figure(figsize=(10, 6))
        if 'DATEANDTIME' in df.columns:
            df.sort_values(by='DATEANDTIME', inplace=True)
            x_axis = df['DATEANDTIME']
            if 'TI_61' in df.columns: plt.plot(x_axis, df['TI_61'], label='TI-61', alpha=0.7)
            if 'TI_63' in df.columns: plt.plot(x_axis, df['TI_63'], label='TI-63', alpha=0.7)
            if 'TI_64' in df.columns: plt.plot(x_axis, df['TI_64'], label='TI-64', alpha=0.7)
            plt.title("C-00 Column Temperature Profile Over Time")
            plt.xlabel("Date and Time")
            plt.ylabel(f"Temperature ({TAG_UNITS['TI-61']})")
            plt.legend()
            plt.grid(True)
            plt.tight_layout()
            plt.savefig(output_temp_plot_path)
            plt.close()
            print(f"Temperature profile plot saved to {output_temp_plot_path}")
            plot_created_flags['temp_profile'] = True
    except Exception as e:
        print(f"Error generating temperature plot: {e}")
        plot_created_flags['temp_profile'] = False
        
    # Differential Pressure Plot
    try:
        if 'DIFFERENTIAL_PRESSURE' in df.columns and not df['DIFFERENTIAL_PRESSURE'].isnull().all():
            plt.figure(figsize=(10, 6))
            plt.plot(df['DATEANDTIME'], df['DIFFERENTIAL_PRESSURE'], color='purple', alpha=0.8)
            plt.title("C-00 Differential Pressure Over Time")
            plt.xlabel("Date and Time")
            plt.ylabel(f"Differential Pressure ({TAG_UNITS['DIFFERENTIAL_PRESSURE']})")
            plt.grid(True)
            plt.tight_layout()
            plt.savefig(output_dp_plot_path)
            plt.close()
            print(f"Differential pressure plot saved to {output_dp_plot_path}")
            plot_created_flags['dp'] = True
    except Exception as e:
        print(f"Error generating DP plot: {e}")
        plot_created_flags['dp'] = False

    # Daily Trends Plot
    try:
        if 'DATEANDTIME' in df.columns:
            df['DATE'] = df['DATEANDTIME'].dt.date
            daily_trends = df.groupby('DATE').agg({
                'FT_01': 'mean',
                'TI_64': 'mean',
                'DIFFERENTIAL_PRESSURE': 'mean'
            }).reset_index()
            if not daily_trends.empty:
                plt.figure(figsize=(12, 8))
                plt.plot(daily_trends['DATE'], daily_trends['FT_01'], label=f"Avg Feed Flow ({TAG_UNITS['FT-01']})")
                plt.plot(daily_trends['DATE'], daily_trends['TI_64'], label=f"Avg Top Temp ({TAG_UNITS['TI-64']})")
                plt.plot(daily_trends['DATE'], daily_trends['DIFFERENTIAL_PRESSURE'], label=f"Avg DP ({TAG_UNITS['DIFFERENTIAL_PRESSURE']})")
                plt.title("C-00 Daily Trends")
                plt.xlabel("Date")
                plt.ylabel("Value")
                plt.legend()
                plt.grid(True)
                plt.tight_layout()
                plt.savefig(output_trends_plot_path)
                plt.close()
                print(f"Daily trends plot saved to {output_trends_plot_path}")
                plot_created_flags['trends'] = True
    except Exception as e:
        print(f"Error generating daily trends plot: {e}")
        plot_created_flags['trends'] = False
        
    # Performance Plot: Moisture Removal vs. Reboiler Heat Duty
    try:
        if all(tag in df.columns for tag in ['MOISTURE_REMOVAL_PERCENTAGE', 'REBOILER_HEAT_DUTY']):
            plt.figure(figsize=(10, 6))
            plt.scatter(df['REBOILER_HEAT_DUTY'], df['MOISTURE_REMOVAL_PERCENTAGE'], alpha=0.5)
            plt.title("C-00 Performance Curve: Moisture Removal vs. Reboiler Heat Duty")
            plt.xlabel(f"Reboiler Heat Duty ({TAG_UNITS['REBOILER_HEAT_DUTY']})")
            plt.ylabel(f"Moisture Removal Percentage ({TAG_UNITS['Moisture Removal Percentage']})")
            plt.grid(True)
            plt.tight_layout()
            plt.savefig(output_performance_plot_path)
            plt.close()
            print(f"Performance plot saved to {output_performance_plot_path}")
            plot_created_flags['performance'] = True
    except Exception as e:
        print(f"Error generating performance plot: {e}")
        plot_created_flags['performance'] = False
    
    # New Performance Plot: Moisture Removal vs. Feed Temperature (TI-01)
    try:
        if all(tag in df.columns for tag in ['MOISTURE_REMOVAL_PERCENTAGE', 'TI_01']):
            plt.figure(figsize=(10, 6))
            plt.scatter(df['TI_01'], df['MOISTURE_REMOVAL_PERCENTAGE'], alpha=0.5)
            plt.title("C-00 Performance: Moisture Removal vs. Feed Temperature")
            plt.xlabel(f"Feed Temperature ({TAG_UNITS['TI-01']})")
            plt.ylabel(f"Moisture Removal Percentage ({TAG_UNITS['Moisture Removal Percentage']})")
            plt.grid(True)
            plt.tight_layout()
            plt.savefig(output_temp_performance_plot_path)
            plt.close()
            print(f"Moisture Removal vs. Feed Temperature plot saved to {output_temp_performance_plot_path}")
            plot_created_flags['temp_performance'] = True
    except Exception as e:
        print(f"Error generating Moisture Removal vs. Feed Temperature plot: {e}")
        plot_created_flags['temp_performance'] = False

    # New Performance Plot: Moisture Removal vs. Top Product Flow (FT-61)
    try:
        if all(tag in df.columns for tag in ['MOISTURE_REMOVAL_PERCENTAGE', 'FT_61']):
            plt.figure(figsize=(10, 6))
            plt.scatter(df['FT_61'], df['MOISTURE_REMOVAL_PERCENTAGE'], alpha=0.5)
            plt.title("C-00 Performance: Moisture Removal vs. Top Product Flow")
            plt.xlabel(f"Top Product Flow ({TAG_UNITS['FT-61']})")
            plt.ylabel(f"Moisture Removal Percentage ({TAG_UNITS['Moisture Removal Percentage']})")
            plt.grid(True)
            plt.tight_layout()
            plt.savefig(output_flow_performance_plot_path)
            plt.close()
            print(f"Moisture Removal vs. Top Product Flow plot saved to {output_flow_performance_plot_path}")
            plot_created_flags['flow_performance'] = True
    except Exception as e:
        print(f"Error generating Moisture Removal vs. Top Product Flow plot: {e}")
        plot_created_flags['flow_performance'] = False
    
    return plot_created_flags
    
def generate_word_report(analysis_results, df, outliers, start_date, end_date, plot_flags):
    """Creates a detailed analysis report in a Word document."""
    doc = Document()
    doc.add_heading('C-00 Packed Distillation Column Analysis Report', 0)
    doc.add_paragraph(f"Analysis Period: {start_date} to {end_date}")
    doc.add_paragraph(f"Report Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Section 1: Executive Summary
    doc.add_heading('1. Executive Summary', level=1)
    summary_text = ""
    moisture_removed = analysis_results.get('Average Moisture Removal Percentage', 'N/A')
    
    if isinstance(moisture_removed, (float, int)):
        summary_text += f"The column achieved an average moisture removal efficiency of **{moisture_removed:.2f}%**. "
    else:
        summary_text += f"The moisture removal calculation showed an anomaly, indicating a potential data or process issue. "

    if 'Overall Material Balance Error (%)' in analysis_results:
        summary_text += f"An overall material balance error of **{analysis_results['Overall Material Balance Error (%)']:.2f}%** was observed, which is within acceptable limits. "
    
    doc.add_paragraph(summary_text)

    # Section 2: Key Performance Indicators (KPIs)
    doc.add_heading('2. Key Performance Indicators (KPIs)', level=1)
    doc.add_paragraph("All values are averages over the analysis period, with outliers removed for accuracy.")
    for key, value in analysis_results.items():
        if key in ['Bottom Product Composition', 'Performance Factors', 'Average Moisture Content in Feed (%)', 'Average Moisture Removal Percentage']:
            continue
        tag_match = re.search(r'\((.*?)\)', key)
        if tag_match:
            tag = tag_match.group(1)
            unit = TAG_UNITS.get(tag, '')
        else:
            unit = TAG_UNITS.get(key, '')

        if isinstance(value, str):
            doc.add_paragraph(f"• {key}: {value}")
        else:
            doc.add_paragraph(f"• {key}: {value:.2f} {unit}")

    # Section 3: Performance Analysis
    doc.add_heading('3. Performance Analysis', level=1)
    doc.add_paragraph("This section correlates key operational factors with column performance.")
    
    # 3.1: Moisture Removal
    doc.add_heading('3.1 Moisture Removal', level=2)
    moisture_in_feed_percent = analysis_results.get('Average Moisture Content in Feed (%)', 'N/A')
    moisture_removed_avg = analysis_results.get('Average Moisture Removal Percentage', 'N/A')
    
    doc.add_paragraph(f"• Average Moisture Content in Feed: {moisture_in_feed_percent:.2f}%")
    if isinstance(moisture_removed_avg, (float, int)):
        doc.add_paragraph(f"• Average Moisture Removal Efficiency: {moisture_removed_avg:.2f}%")
    else:
        doc.add_paragraph(f"• Average Moisture Removal Efficiency: {moisture_removed_avg}")
        
    doc.add_paragraph("The plot below shows how moisture removal efficiency is correlated with the reboiler heat duty. It helps identify the optimal operating window.")
    if plot_flags.get('performance') and os.path.exists(output_performance_plot_path):
        doc.add_picture(output_performance_plot_path, width=Inches(6))
    else:
        doc.add_paragraph("Plot not generated due to missing data.")

    # 3.2: Factor Correlations
    doc.add_heading('3.2 Factor Correlations', level=2)
    doc.add_paragraph("The plots below show the relationships between key performance factors.")
    
    doc.add_paragraph("The plot below shows how moisture removal efficiency is affected by the feed temperature.")
    if plot_flags.get('temp_performance') and os.path.exists(output_temp_performance_plot_path):
        doc.add_picture(output_temp_performance_plot_path, width=Inches(6))
    else:
        doc.add_paragraph("Plot not generated due to missing data.")

    doc.add_paragraph("The plot below shows how moisture removal efficiency is affected by the top product flow.")
    if plot_flags.get('flow_performance') and os.path.exists(output_flow_performance_plot_path):
        doc.add_picture(output_flow_performance_plot_path, width=Inches(6))
    else:
        doc.add_paragraph("Plot not generated due to missing data.")

    if 'Performance Factors' in analysis_results:
        for key, value in analysis_results['Performance Factors'].items():
            doc.add_paragraph(f"• {key}: {value:.2f}")

    # 3.3: Composition Analysis (Note: This is a placeholder as no composition data is being passed)
    doc.add_heading('3.3 Bottom Product Composition', level=2)
    bottom_comp = analysis_results.get('Bottom Product Composition', {})
    doc.add_paragraph("The composition of the bottom product (the feed to C-01) is calculated based on the assumption that non-moisture components are not separated by this column.")
    if bottom_comp:
        for comp, perc in bottom_comp.items():
            doc.add_paragraph(f"• {comp.replace('_', ' ').capitalize()}: {perc:.2f}%")
    else:
        doc.add_paragraph("Composition data for the bottom product is not available due to missing flow data.")

    # Section 4: Performance Plots
    doc.add_heading('4. Performance Plots', level=1)
    
    doc.add_heading('4.1 Temperature Profile', level=2)
    doc.add_paragraph("The temperature profile plot shows the gradient across the column. A consistent gradient indicates stable operation.")
    if 'TI_63' in outliers:
        doc.add_paragraph(f"**Note:** An extreme outlier was detected on {outliers['TI_63']['time']} for the TI-63 sensor, reaching a value of {outliers['TI_63']['value']:.2f} {TAG_UNITS['TI-63']}. This is likely a sensor malfunction and the value has been excluded from all calculations.")
    if plot_flags.get('temp_profile') and os.path.exists(output_temp_plot_path):
        doc.add_picture(output_temp_plot_path, width=Inches(6))
    else:
        doc.add_paragraph("Plot not generated due to missing data.")
    
    doc.add_heading('4.2 Differential Pressure (DP)', level=2)
    doc.add_paragraph("Differential pressure is a key indicator of flooding, foaming, or fouling inside the column.")
    if plot_flags.get('dp') and os.path.exists(output_dp_plot_path):
        doc.add_picture(output_dp_plot_path, width=Inches(6))
    else:
        doc.add_paragraph("Plot not generated due to missing data.")

    doc.add_heading('4.3 Daily Trends', level=2)
    doc.add_paragraph("This plot shows the daily average trends of key variables, helping to visualize long-term shifts in performance.")
    if plot_flags.get('trends') and os.path.exists(output_trends_plot_path):
        doc.add_picture(output_trends_plot_path, width=Inches(6))
    else:
        doc.add_paragraph("Plot not generated due to missing data.")
    
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
        
    analysis_results, scada_data, outliers = perform_analysis(scada_data, start_date, end_date)
    
    if analysis_results:
        plot_flags = generate_plots(scada_data)
        generate_word_report(analysis_results, scada_data, outliers, start_date, end_date, plot_flags)
        print("C-00 analysis complete.")
    else:
        print("Analysis failed: no data to process.")

if __name__ == "__main__":
    main()