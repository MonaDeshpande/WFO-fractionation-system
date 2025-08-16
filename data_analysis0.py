import pandas as pd
import io
import sys

# ==============================================================================
# CONFIGURATION
# ==============================================================================
# A string containing the data from your lab report image.
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

# ==============================================================================
# FUNCTIONS
# ==============================================================================
def create_report(df):
    """
    Analyzes the lab data and generates a human-readable report based on purity rules.
    """
    print("------------------------------------------------------------------")
    print("                Naphthalene Oil Purity Report                     ")
    print("------------------------------------------------------------------\n")

    # Define desired purity levels for different components
    purity_limits_low = {
        "Light Oil": 15.0,
        "Wash Oil": 2.0,
        "Anthracene Oil": 2.0,
    }
    
    # Naphthalene Oil product should have high purity.
    # We'll set a high target to flag anything below it.
    purity_target_high = {
        "phthaleine": 90.0,
    }
    
    out_of_spec_low = []
    out_of_spec_high = []

    for index, row in df.iterrows():
        component = row['component']
        purity_percentage = row['naphthalene_oil_percent']
        date = row['date']
        time = row['time']

        # Check for products where naphthalene % should be low
        if component in purity_limits_low:
            limit = purity_limits_low[component]
            if purity_percentage > limit:
                out_of_spec_low.append({
                    'date': date,
                    'time': time,
                    'component': component,
                    'actual_purity': purity_percentage,
                    'purity_limit': limit
                })
        
        # Check for products where naphthalene % should be high
        if component in purity_target_high:
            target = purity_target_high[component]
            if purity_percentage < target:
                out_of_spec_high.append({
                    'date': date,
                    'time': time,
                    'component': component,
                    'actual_purity': purity_percentage,
                    'purity_target': target
                })

    # Print the report for low-purity products
    if out_of_spec_low:
        print("❌ WARNING: The following samples are out of specification for Naphthalene Oil purity:")
        print("------------------------------------------------------------------")
        for item in out_of_spec_low:
            print(f"Date: {item['date']} at {item['time']}")
            print(f"  - Component: {item['component']}")
            print(f"  - Purity: {item['actual_purity']:.2f}% (Limit: <{item['purity_limit']}%)")
            print("  - Potential Issue: This indicates a problem with the separation process. Check the distillation column settings or upstream conditions.")
            print("------------------------------------------------------------------")
    
    # Print the report for high-purity products
    if out_of_spec_high:
        print("\n⚠️ NOTE: The following samples are below the target purity for Naphthalene Oil:")
        print("------------------------------------------------------------------")
        for item in out_of_spec_high:
            print(f"Date: {item['date']} at {item['time']}")
            print(f"  - Component: {item['component']}")
            print(f"  - Purity: {item['actual_purity']:.2f}% (Target: >{item['purity_target']}%)")
            print("  - Suggestion: This may indicate an efficiency issue. Consider optimizing process parameters to increase yield.")
            print("------------------------------------------------------------------")
    
    if not out_of_spec_low and not out_of_spec_high:
        print("✅ All samples are within the specified purity limits. The plant is operating well.")
        print("------------------------------------------------------------------")

def analyze_wfo_impact(df_wfo, df_products):
    """
    Analyzes the correlation between WFO percentage and product purity.
    """
    print("------------------------------------------------------------------")
    print("        Analysis: Impact of Naphthalene in WFO on Products        ")
    print("------------------------------------------------------------------")

    if df_wfo.empty or df_products.empty:
        print("❌ Not enough data to perform WFO impact analysis.")
        return

    correlations = {}

    for product in df_products['component'].unique():
        product_df = df_products[df_products['component'] == product].copy()
        
        product_df['temp_merge_key'] = product_df['DateAndTime']
        df_wfo['temp_merge_key'] = df_wfo['DateAndTime']
        
        # We use merge_asof to find the most recent WFO data for each product sample.
        merged_df = pd.merge_asof(
            product_df, 
            df_wfo, 
            on='temp_merge_key', 
            direction='backward'
        )
        
        if len(merged_df) > 1:
            correlation = merged_df['naphthalene_oil_percent_x'].corr(merged_df['naphthalene_oil_percent_y'])
            correlations[product] = correlation
    
    if correlations:
        print("✅ Correlation coefficients between WFO Naphthalene % and product purity:")
        for product, corr in correlations.items():
            if not pd.isna(corr):
                print(f"  - {product}: {corr:.2f}")
    else:
        print("❌ Could not calculate correlations. Not enough data points to analyze.")

def process_lab_data():
    """
    Main function to process the lab data and generate the report.
    """
    try:
        # Use io.StringIO to treat the string as a file
        df = pd.read_csv(io.StringIO(LAB_DATA_STRING))
        
        # Strip any leading/trailing whitespace from column names
        df.columns = df.columns.str.strip()
        
        print("✅ Lab data loaded successfully.")
        
        # Convert date and time columns to a single datetime object
        df['DateAndTime'] = pd.to_datetime(df['date'] + ' ' + df['time'], format='%d.%m.%y %I.%M%p')
        
        # Separate the WFO data from the final product data
        df_wfo = df[df['component'] == 'WFO'].sort_values('DateAndTime')
        df_products = df[df['component'].isin(["Light Oil", "Wash Oil", "Anthracene Oil", "phthaleine"])].sort_values('DateAndTime')
        
        # Run the analysis and generate the report
        create_report(df_products)
        analyze_wfo_impact(df_wfo, df_products)

    except Exception as e:
        print(f"❌ An error occurred while processing data: {e}", file=sys.stderr)

if __name__ == "__main__":
    process_lab_data()
