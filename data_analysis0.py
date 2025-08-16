import pandas as pd
import numpy as np
import io
import sys
import datetime
import psycopg2
import os

# ==============================================================================
# CONFIGURATION
# ==============================================================================
# --- Database Connection ---
# IMPORTANT: Add your PostgreSQL password here
PG_HOST = "localhost"
PG_PORT = "5432"
PG_USER = "postgres"
PG_PASSWORD = "ADMIN"  
PG_DB_NAME = "scada_data_analysis"
PG_TRANSFORMED_TABLE = "wide_scada_data"

# --- Lab Data and Report Output ---
LAB_DATA_STRING = """
date,time,column,component,naphthalene_oil_percent
08.08.25,06.00AM,C-03-T,phthaleine,87.71
08.08.25,06.00AM,C-02-T,"Light Oil",53.42
08.08.25,06.00AM,C-03-B,"Wash Oil",1.25
08.08.25,06.00AM,C-01-B,"Anthracene Oil",0.03
08.08.25,09.30AM,P-01,"WFO",56.38
08.08.25,11.30AM,C-02-T,"Light Oil",8.02
08.08.25,03.15PM,C-03-T,phthaleine,87.05
08.08.25,03.15PM,C-02-T,"Light Oil",8.24
08.08.25,06.30AM,P-01,"WFO",57.03
08.08.25,07.30PM,C-03-B,"Wash Oil",0.01
08.08.25,07.30PM,C-01-B,"Anthracene Oil",0.02
"""
REPORT_FILE_NAME = "purity_analysis_report.txt"

# ==============================================================================
# DATA LOADING AND PROCESSING
# ==============================================================================
def load_data(start_date, end_date):
    """
    Loads SCADA data from the PostgreSQL database and lab results from a string.
    """
    pg_conn = None
    try:
        print("Connecting to PostgreSQL...")
        pg_conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD, dbname=PG_DB_NAME
        )
        print("✅ Successfully connected to PostgreSQL.")

        print(f"\nFetching data from '{PG_TRANSFORMED_TABLE}' between {start_date} and {end_date}...")
        fetch_query = f"""
        SELECT * FROM "{PG_TRANSFORMED_TABLE}"
        WHERE "DateAndTime" BETWEEN %s AND %s
        ORDER BY "DateAndTime" ASC;
        """
        scada_df = pd.read_sql(fetch_query, pg_conn, params=(start_date, end_date))
        print(f"✅ SCADA data fetched successfully. Shape: {scada_df.shape}")

        print(f"\nLoading lab results from string...")
        lab_df = pd.read_csv(io.StringIO(LAB_DATA_STRING))
        lab_df.columns = lab_df.columns.str.strip()
        print(f"✅ Lab results loaded. Shape: {lab_df.shape}")

        # Convert timestamp columns to a proper datetime format
        scada_df['DateAndTime'] = pd.to_datetime(scada_df['DateAndTime'])
        lab_df['DateAndTime'] = pd.to_datetime(lab_df['date'] + ' ' + lab_df['time'], format='%d.%m.%y %I.%M%p')

        # Merge the two dataframes
        # This will merge the lab data with the SCADA data taken at approximately the same time
        df = pd.merge(scada_df, lab_df, on='DateAndTime', how='inner')

        # Separate the WFO data from the final product data
        df_wfo = df[df['component'] == 'WFO'].sort_values('DateAndTime')
        df_products = df[df['component'].isin(["Light Oil", "Wash Oil", "Anthracene Oil", "phthaleine"])].sort_values('DateAndTime')
        
        print("\n✅ Data successfully merged and prepared.")
        return df_wfo, df_products

    except psycopg2.Error as e:
        print(f"❌ PostgreSQL connection or query failed. Error: {e}", file=sys.stderr)
        return None, None
    except Exception as e:
        print(f"❌ An unexpected error occurred while loading data: {e}", file=sys.stderr)
        return None, None
    finally:
        if pg_conn:
            pg_conn.close()
            print("Database connection closed.")


def generate_report_file(df_wfo, df_products):
    """
    Generates a human-readable report and saves it to a text file.
    """
    if df_products.empty:
        print("❌ Cannot generate report. No product data found in the specified date range.")
        return

    try:
        with open(REPORT_FILE_NAME, 'w') as f:
            f.write("------------------------------------------------------------------\n")
            f.write("                Naphthalene Oil Purity Report                     \n")
            f.write("------------------------------------------------------------------\n\n")
            
            # --- Analysis of Product Purity vs. Specs ---
            purity_limits_low = {
                "Light Oil": 15.0,
                "Wash Oil": 2.0,
                "Anthracene Oil": 2.0,
            }
            purity_target_high = {
                "phthaleine": 90.0,
            }
            
            f.write("ANALYSIS OF PRODUCT PURITY\n")
            f.write("------------------------------------------------------------------\n")
            out_of_spec_found = False
            for index, row in df_products.iterrows():
                component = row['component']
                purity_percentage = row['naphthalene_oil_percent']
                date_time = row['DateAndTime']

                if component in purity_limits_low and purity_percentage > purity_limits_low[component]:
                    out_of_spec_found = True
                    f.write(f"❌ WARNING: {component} sample on {date_time} is out of specification.\n")
                    f.write(f"  - Purity: {purity_percentage:.2f}% (Limit: <{purity_limits_low[component]}%)\n")
                    f.write("  - Suggestion: Investigate process settings around this time.\n\n")
                
                if component in purity_target_high and purity_percentage < purity_target_high[component]:
                    out_of_spec_found = True
                    f.write(f"⚠️ NOTE: {component} sample on {date_time} is below target purity.\n")
                    f.write(f"  - Purity: {purity_percentage:.2f}% (Target: >{purity_target_high[component]}%)\n")
                    f.write("  - Suggestion: Look for opportunities to optimize separation efficiency.\n\n")

            if not out_of_spec_found:
                f.write("✅ All samples are within the specified purity limits. The plant is operating well.\n\n")

            # --- Analysis of WFO Impact ---
            f.write("IMPACT OF NAPHTHALENE IN WASH OIL FEED (WFO)\n")
            f.write("------------------------------------------------------------------\n")
            if df_wfo.empty:
                f.write("❌ WFO data is not available in the specified date range.\n\n")
            else:
                correlations = {}
                for product in df_products['component'].unique():
                    product_df = df_products[df_products['component'] == product].copy()
                    
                    product_df['temp_merge_key'] = product_df['DateAndTime']
                    df_wfo['temp_merge_key'] = df_wfo['DateAndTime']
                    
                    merged_df = pd.merge_asof(
                        product_df, 
                        df_wfo, 
                        on='temp_merge_key', 
                        direction='backward'
                    )
                    
                    if len(merged_df) > 1:
                        # Correlate product purity with WFO purity at that time
                        correlation = merged_df['naphthalene_oil_percent_x'].corr(merged_df['naphthalene_oil_percent_y'])
                        correlations[product] = correlation
                
                if correlations:
                    f.write("✅ Correlation coefficients between WFO Naphthalene % and product purity:\n")
                    for product, corr in correlations.items():
                        if not pd.isna(corr):
                            f.write(f"  - {product}: {corr:.2f}\n")
                    f.write("\nInterpretation of Correlation:\n")
                    f.write(" - A positive correlation means as WFO purity increases, so does the product's naphthalene content.\n")
                    f.write(" - A negative correlation means as WFO purity increases, the product's naphthalene content decreases.\n")
                else:
                    f.write("❌ Could not calculate correlations. Not enough data points to analyze.\n")

        print(f"\n✅ Report successfully generated and saved to '{REPORT_FILE_NAME}'")
    except Exception as e:
        print(f"❌ An error occurred while writing the report file: {e}", file=sys.stderr)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("❌ Error: Missing date range arguments.")
        print("Usage: python purity_analysis.py 'YYYY-MM-DD HH:MM:SS' 'YYYY-MM-DD HH:MM:SS'")
        sys.exit(1)

    start_date_str = sys.argv[1]
    end_date_str = sys.argv[2]

    try:
        start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d %H:%M:%S')
        end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        print("❌ Invalid date format. Please use 'YYYY-MM-DD HH:MM:SS'.")
        sys.exit(1)

    df_wfo, df_products = load_data(start_date, end_date)
    if df_wfo is not None and df_products is not None:
        generate_report_file(df_wfo, df_products)
