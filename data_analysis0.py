import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
import psycopg2
import sys
import datetime

# ==============================================================================
# CONFIGURATION
# ==============================================================================
# --- Database Connection (uncomment if you want to use the database directly) ---
PG_HOST = "localhost"
PG_PORT = "5432"
PG_USER = "postgres"
PG_PASSWORD = "ADMIN"  # <-- IMPORTANT: Add your PostgreSQL password here
PG_DB_NAME = "scada_data_analysis"
PG_TRANSFORMED_TABLE = "wide_scada_data" # Using the transformed table from your previous script

# --- User Input ---
# This is the file containing the final lab results for purity.
LAB_DATA_FILE = "purity_lab_results.csv" 
# This is the name of the column in the lab results file that holds the purity values.
PURITY_COLUMN = 'Purity'

def get_user_date_range():
    """
    Prompts the user to enter a start and end date for analysis.
    """
    while True:
        try:
            start_date_str = input("Enter start date (YYYY-MM-DD HH:MM:SS): ")
            end_date_str = input("Enter end date (YYYY-MM-DD HH:MM:SS): ")
            
            start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d %H:%M:%S')
            end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d %H:%M:%S')

            if start_date >= end_date:
                print("❌ Start date must be before end date. Please try again.")
                continue

            return start_date, end_date
        except ValueError:
            print("❌ Invalid date format. Please use 'YYYY-MM-DD HH:MM:SS'.")

def load_data(start_date, end_date):
    """
    Loads SCADA data from the PostgreSQL database and lab results from a CSV.
    """
    pg_conn = None
    try:
        # --- Step 1: Load SCADA data from PostgreSQL ---
        print("Connecting to PostgreSQL...")
        pg_conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD, dbname=PG_DB_NAME)
        pg_cursor = pg_conn.cursor()
        print("✅ Successfully connected to PostgreSQL.")

        print(f"\nFetching data from '{PG_TRANSFORMED_TABLE}' between {start_date} and {end_date}...")
        fetch_query = f"""
        SELECT * FROM "{PG_TRANSFORMED_TABLE}"
        WHERE "DateAndTime" BETWEEN %s AND %s
        ORDER BY "DateAndTime" ASC;
        """
        # The pd.read_sql function handles the query execution and DataFrame creation.
        df_scada = pd.read_sql(fetch_query, pg_conn, params=(start_date, end_date))
        print(f"✅ SCADA data fetched successfully. Shape: {df_scada.shape}")
        
        # --- Step 2: Load lab results from CSV ---
        print(f"\nLoading lab results from {LAB_DATA_FILE}...")
        lab_df = pd.read_csv(LAB_DATA_FILE)
        print(f"✅ Lab results loaded. Shape: {lab_df.shape}")

        # --- Step 3: Prepare and merge the dataframes ---
        # Convert the timestamp columns to a proper datetime format.
        df_scada['DateAndTime'] = pd.to_datetime(df_scada['DateAndTime'])
        lab_df['DateAndTime'] = pd.to_datetime(lab_df['DateAndTime'])

        # Now, we merge the two dataframes.
        df = pd.merge(df_scada, lab_df, on='DateAndTime', how='left')
        
        # We drop any rows where there is no purity data.
        df.dropna(subset=[PURITY_COLUMN], inplace=True)
        
        print("\n✅ Data successfully merged and cleaned.")
        print(f"Final merged dataset shape: {df.shape}")
        return df

    except FileNotFoundError as e:
        print(f"❌ Error: {e}")
        print("Please make sure your lab results file is in the same directory.")
        return None
    except psycopg2.Error as e:
        print(f"❌ PostgreSQL connection or query failed. Error: {e}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"❌ An unexpected error occurred while loading data: {e}", file=sys.stderr)
        return None
    finally:
        if pg_conn:
            pg_conn.close()
            print("Database connection closed.")


def analyze_purity(df):
    """
    Performs a correlation analysis and a linear regression to identify
    factors affecting purity.
    """
    if df is None or df.empty:
        print("Cannot perform analysis. Dataframe is empty after merging.")
        return

    # --- Step 1: Data Preparation ---
    features = df.select_dtypes(include=np.number).drop(columns=[PURITY_COLUMN], errors='ignore')
    target = df[PURITY_COLUMN]
    
    if target.empty or features.empty:
        print(f"❌ Error: Could not find '{PURITY_COLUMN}' or other numeric features.")
        return

    # --- Step 2: Correlation Analysis ---
    print("\n--- Correlation Analysis ---")
    correlations = df.corr(numeric_only=True)[PURITY_COLUMN].sort_values(ascending=False)
    print("Top 10 features most correlated with purity:")
    print(correlations.head(10))

    # --- Step 3: Linear Regression Analysis ---
    print("\n--- Linear Regression Analysis ---")
    
    X = features
    y = target
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = LinearRegression()
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)

    print(f"Mean Squared Error (MSE): {mse:.2f}")
    print(f"R-squared (R²): {r2:.2f}")
    
    feature_importance = pd.DataFrame({
        'Feature': X.columns,
        'Coefficient': model.coef_
    }).sort_values(by='Coefficient', ascending=False)
    
    print("\nTop 10 factors affecting purity (based on regression coefficients):")
    print(feature_importance.head(10))

    # --- Step 4: Visualization (for better understanding) ---
    plt.style.use('seaborn-v0_8-whitegrid')
    
    fig, axes = plt.subplots(5, 1, figsize=(10, 25))
    top_5_corr_features = correlations.index[1:6]
    
    for i, feature in enumerate(top_5_corr_features):
        sns.scatterplot(data=df, x=feature, y=PURITY_COLUMN, ax=axes[i], alpha=0.6)
        axes[i].set_title(f'Purity vs. {feature}')
        axes[i].set_xlabel(feature)
        axes[i].set_ylabel(PURITY_COLUMN)
        axes[i].grid(True)
    
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    start_date, end_date = get_user_date_range()
    data = load_data(start_date, end_date)
    if data is not None:
        analyze_purity(data)
