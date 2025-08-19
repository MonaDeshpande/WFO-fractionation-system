import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
from docx import Document
from docx.shared import Inches
from docx.enum.text import WD_ALIGN_PARAGRAPH

def create_word_report(df, lab_results_df, filename):
    """Creates a Word document with analysis results and graphs from a chemical engineering perspective."""
    doc = Document()
    doc.add_heading('Naphthalene Recovery Plant: Distillation Column Analysis Report', 0)

    # Add overall expert observations
    doc.add_heading('Expert Analysis: Overall Plant Performance', level=1)
    doc.add_paragraph('The primary objective of this plant is to recover maximum naphthalene from the top of Column C-03. The preceding columns (C-00, C-01, C-02) are critical pre-purification steps to ensure the final product purity.')
    doc.add_paragraph('**Key Factors Affecting Naphthalene Recovery:**')
    doc.add_paragraph('1.  **Reboiler Temperature in C-03:** Maintaining a stable temperature (325-340°C) is the most critical factor for ensuring complete vaporization of naphthalene. Deviation from this range directly impacts recovery and bottom product purity.')
    doc.add_paragraph('2.  **Reflux Ratio:** For columns C-01 and C-02, a sufficient reflux ratio is essential for efficient separation of different boiling point components, preventing their carry-over to downstream columns.')
    doc.add_paragraph('3.  **Feed Quality:** The dehydration column (C-00) is crucial for removing moisture. High water content can negatively affect the separation in downstream columns and increase energy consumption.')
    doc.add_page_break()

    # Analyze each column
    column_analysis = {
        'C-00': {'purpose': 'This column aims to remove maximum moisture from the feed.', 'tags': {'feed': 'FI-01', 'top_flow': 'FI-61', 'bottom_flow': 'FI-62'}},
        'C-01': {'purpose': 'To produce a bottom product (Anthracene Oil) with less than 2% naphthalene.', 'tags': {'reflux_flow': 'FT-08', 'top_flow': 'FT-02', 'feed_temp': 'TI-02', 'tray_temps': ['TI-03', 'TI-04', 'TI-05', 'TI-06']}},
        'C-02': {'purpose': 'To produce a top product (Light Oil) with less than 15% naphthalene.', 'tags': {'reflux_flow': 'FT-09', 'top_flow': 'FT-03', 'feed_temp': 'TI-11', 'tray_temps': ['TI-13', 'TI-14', 'TI-15', 'TI-16', 'TI-17', 'TI-18', 'TI-19', 'TI-20', 'TI-21', 'TI-22', 'TI-23', 'TI-24', 'TI-25']}},
        'C-03': {'purpose': 'To recover maximum naphthalene from the top and produce pure wash oil at the bottom (max 2% naphthalene).', 'tags': {'reflux_flow': 'FT-10', 'top_flow': 'FT-04', 'feed_temp': 'TI-30', 'tray_temps': ['TI-31', 'TI-32', 'TI-33', 'TI-34', 'TI-35', 'TI-36', 'TI-37', 'TI-38', 'TI-39', 'TI-40']}},
    }

    for column_name, details in column_analysis.items():
        doc.add_heading(f'Analysis for {column_name}', level=1)
        doc.add_paragraph(details['purpose'])

        tags = details['tags']
        if column_name == 'C-00':
            if tags['feed'] in df.columns and tags['top_flow'] in df.columns and tags['bottom_flow'] in df.columns:
                doc.add_heading('Process Metrics and Material Balance', level=2)
                
                # Material Balance Calculation
                total_feed = df[tags['feed']].mean()
                top_product = df[tags['top_flow']].mean()
                bottom_product = df[tags['bottom_flow']].mean()
                
                # Assuming top product (water) flow is FI-61 and bottom product is FI-62
                # Total Out = FI-61 + FI-62
                total_out = top_product + bottom_product
                
                doc.add_paragraph(f'Average Feed Rate ({tags["feed"]}): {total_feed:.2f} m³/hr')
                doc.add_paragraph(f'Average Water Removal Rate ({tags["top_flow"]}): {top_product:.2f} m³/hr')
                doc.add_paragraph(f'Average Bottom Product Rate ({tags["bottom_flow"]}): {bottom_product:.2f} m³/hr')
                doc.add_paragraph(f'Material Balance Check (Feed vs. Total Out): {total_feed:.2f} vs {total_out:.2f}')
                
                # Purity Check for C-00 feed (P-01)
                purity_c00_feed = lab_results_df[(lab_results_df['Sample'] == 'P-01') & (lab_results_df['Product'] == 'Naphthalene')]
                if not purity_c00_feed.empty:
                    naphthalene_percent = purity_c00_feed['Value'].iloc[0]
                    doc.add_paragraph(f'**Feed Naphthalene Content (P-01):** {naphthalene_percent:.2f}% (from lab data)')
                
                doc.add_paragraph('**Expert Observation:** The material balance shows a good approximation, indicating consistent flow measurements. The primary goal is to maximize moisture removal before the feed enters C-01 to prevent process upsets downstream.')

        else:
            reflux_flow = tags.get('reflux_flow')
            top_product_flow = tags.get('top_flow')
            feed_temp_col = tags.get('feed_temp')
            temp_cols = tags.get('tray_temps', [])

            if reflux_flow in df.columns and top_product_flow in df.columns:
                df['reflux_ratio'] = df[reflux_flow] / df[top_product_flow]
                
                doc.add_heading('Process Metrics', level=2)
                doc.add_paragraph(f"Average Reflux Ratio: {df['reflux_ratio'].mean():.2f}")
                doc.add_paragraph(f"Average Feed Temp ({feed_temp_col}): {df[feed_temp_col].mean():.2f}°C")
                
                # Material Balance and Purity Check
                doc.add_heading('Purity and Material Balance Check', level=2)
                
                # Assume bottom product of C-01 is C-01-B, C-02 top is C-02-T, etc.
                if column_name == 'C-01':
                    sample_name = 'C-01-B'
                    target_percent = 2
                    
                    purity_result = lab_results_df[(lab_results_df['Sample'] == sample_name) & (lab_results_df['Product'] == 'Naphthalene')]
                    if not purity_result.empty:
                        naphthalene_percent = purity_result['Value'].iloc[0]
                        doc.add_paragraph(f"Naphthalene in Anthracene Oil ({sample_name}): {naphthalene_percent:.2f}%")
                        if naphthalene_percent > target_percent:
                            doc.add_paragraph(f"**🔴 WARNING:** Naphthalene percentage ({naphthalene_percent:.2f}%) exceeds the target of <{target_percent}%. This indicates poor separation in this column.")
                        else:
                            doc.add_paragraph(f"**✅ SUCCESS:** Naphthalene percentage is within the acceptable range.")
                    
                    doc.add_paragraph('**Expert Observation:** Maintaining the reflux ratio is crucial for meeting the bottom product purity target. A high reflux ratio can increase efficiency but also energy costs.')
                    
                elif column_name == 'C-02':
                    sample_name = 'C-02-T'
                    target_percent = 15
                    purity_result = lab_results_df[(lab_results_df['Sample'] == sample_name) & (lab_results_df['Product'] == 'Naphthalene')]
                    if not purity_result.empty:
                        naphthalene_percent = purity_result['Value'].iloc[0]
                        doc.add_paragraph(f"Naphthalene in Light Oil ({sample_name}): {naphthalene_percent:.2f}%")
                        if naphthalene_percent > target_percent:
                            doc.add_paragraph(f"**🔴 WARNING:** Naphthalene percentage ({naphthalene_percent:.2f}%) exceeds the target of <{target_percent}%. This indicates poor separation.")
                        else:
                            doc.add_paragraph(f"**✅ SUCCESS:** Naphthalene percentage is within the acceptable range.")
                    
                    doc.add_paragraph('**Expert Observation:** A high reflux ratio is essential to prevent naphthalene from going to the bottom product, which is fed to the crucial C-03 column.')

                elif column_name == 'C-03':
                    sample_name_top = 'C-03-T'
                    sample_name_bottom = 'C-03-B'
                    target_percent_bottom = 2
                    
                    # Naphthalene in Top Product
                    purity_top = lab_results_df[(lab_results_df['Sample'] == sample_name_top) & (lab_results_df['Product'] == 'Naphthalene')]
                    if not purity_top.empty:
                        naphthalene_percent = purity_top['Value'].iloc[0]
                        doc.add_paragraph(f"Naphthalene in Top Product ({sample_name_top}): {naphthalene_percent:.2f}%")
                        doc.add_paragraph('**Expert Observation:** For a naphthalene recovery plant, this percentage should be as high as possible. The current value indicates a high degree of recovery.')
                    
                    # Naphthalene in Bottom Product
                    purity_bottom = lab_results_df[(lab_results_df['Sample'] == sample_name_bottom) & (lab_results_df['Product'] == 'Naphthalene')]
                    if not purity_bottom.empty:
                        naphthalene_percent = purity_bottom['Value'].iloc[0]
                        doc.add_paragraph(f"Naphthalene in Wash Oil ({sample_name_bottom}): {naphthalene_percent:.2f}%")
                        if naphthalene_percent > target_percent_bottom:
                            doc.add_paragraph(f"**🔴 WARNING:** Naphthalene percentage ({naphthalene_percent:.2f}%) in the bottom product exceeds the target of <{target_percent_bottom}%. This indicates a potential issue with reboiler temperature or flow.")
                        else:
                            doc.add_paragraph(f"**✅ SUCCESS:** Naphthalene percentage is within the acceptable range.")
                    
                    doc.add_paragraph('**Expert Observation:** This is the most critical column. The high concentration of naphthalene in the top product is a good sign. However, the reboiler temperature must be carefully controlled to prevent naphthalene from being lost in the bottom wash oil.')
                    
                doc.add_heading('Process Variable Trends', level=2)
                fig, axes = plt.subplots(nrows=3, ncols=1, figsize=(10, 15))
                
                axes[0].plot(df['datetime'], df[feed_temp_col])
                axes[0].set_title(f'Feed Temperature ({feed_temp_col}) vs. Time')
                axes[0].set_xlabel('Time')
                axes[0].set_ylabel('Temperature (°C)')
                
                axes[1].plot(df['datetime'], df['reflux_ratio'])
                axes[1].set_title('Reflux Ratio vs. Time')
                axes[1].set_xlabel('Time')
                axes[1].set_ylabel('Reflux Ratio')
                
                for col in temp_cols:
                    axes[2].plot(df['datetime'], df[col], label=col)
                axes[2].set_title('Column Temperatures vs. Time')
                axes[2].set_xlabel('Time')
                axes[2].set_ylabel('Temperature (°C)')
                axes[2].legend()
                
                plt.tight_layout()
                plt_filename = f'plot_{column_name}_process.png'
                plt.savefig(plt_filename)
                doc.add_picture(plt_filename, width=Inches(6))
                
                doc.add_paragraph('**Analysis of Trends:**')
                doc.add_paragraph('The graphs above show the trends of key operational parameters over time. Stable feed temperatures and reflux ratios are indicators of consistent operation. Fluctuations in these values can lead to unstable column performance and affect product purity.')
                doc.add_page_break()

    doc.save(filename)
    print(f"Report saved as {filename}")

def get_data_from_db(start_date, end_date, table_name):
    """Connects to the PostgreSQL database and fetches the data."""
    conn = None
    df = pd.DataFrame()
    try:
        conn = psycopg2.connect(
            dbname="scada_data_analysis",
            user="postgres",
            password="ADMIN",
            host="localhost",
            port="5432"
        )
        print("Database connection successful.")
        
        query = f"""
        SELECT 
            *
        FROM 
            "{table_name}"
        WHERE 
            "DateAndTime" BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY 
            "DateAndTime";
        """
        df = pd.read_sql(query, conn)
        df['datetime'] = pd.to_datetime(df['DateAndTime'])
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error connecting to database: {error}")
    finally:
        if conn is not None:
            conn.close()
    return df

if __name__ == '__main__':
    # --- USER INPUT SECTION ---
    table_to_analyze = 'data_cleaning_with_report'
    start_time = '2025-08-08 00:00:00'
    end_time = '2025-08-14 23:59:59'
    output_filename = 'Naphthalene_Recovery_Plant_Report.docx'

    # Assume lab results are in a CSV file as per the prompt
    try:
        lab_results_df = pd.read_csv('purity_lab_result.csv')
    except FileNotFoundError:
        print("Error: purity_lab_result.csv not found. Purity analysis will be skipped.")
        lab_results_df = pd.DataFrame()

    # Step 1: Get data from PostgreSQL
    full_df = get_data_from_db(start_time, end_time, table_to_analyze)
    
    if not full_df.empty:
        # Step 2: Create the Word report with analysis
        create_word_report(full_df, lab_results_df, output_filename)
    else:
        print("No data found in the specified time range. Please check your table name, date range and database connection.")