import psycopg2
import sys
import csv

# ==============================================================================
# CONFIGURATION
# ==============================================================================
PG_HOST = "localhost"
PG_PORT = "5432"
PG_USER = "postgres"
PG_PASSWORD = "ADMIN"  # Add your PostgreSQL password here
PG_DB_NAME = "scada_data"
PG_RAW_TABLE = "scada_data_streamlined"
PG_MAPPING_TABLE = "tag_mapping"
PG_TRANSFORMED_VIEW = "wide_scada_data"

def process_and_insert_tags():
    """
    Connects to PostgreSQL, reads tags from CSV, and inserts them.
    Then, it creates a transformed view based on the mapping.
    """
    pg_conn = None
    try:
        print("Connecting to PostgreSQL to insert data and create view...")
        pg_conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD, dbname=PG_DB_NAME)
        pg_cursor = pg_conn.cursor()
        print("✅ Successfully connected to PostgreSQL.")

        # --- Step 1: Read tags from CSV and insert into the mapping table ---
        print("\n--- Reading tags from tags.csv and inserting into mapping table ---")
        tag_data = []
        try:
            with open('tags.csv', 'r') as f:
                reader = csv.reader(f)
                next(reader)  # Skip the header row
                for row in reader:
                    tag_data.append((int(row[0]), row[1]))
            print(f"📁 Found {len(tag_data)} tags in tags.csv.")
        except FileNotFoundError:
            print("❌ Error: tags.csv not found. Please create the file with TagIndex and TagName columns.")
            return

        insert_mapping_query = f"""
        INSERT INTO "{PG_MAPPING_TABLE}" ("TagIndex", "TagName")
        VALUES (%s, %s)
        ON CONFLICT ("TagIndex") DO NOTHING;
        """
        pg_cursor.executemany(insert_mapping_query, tag_data)
        pg_conn.commit()
        print(f"✅ Successfully inserted/updated {pg_cursor.rowcount} tags.")
        
        # --- Step 2: Dynamically generate and create the PIVOTED VIEW ---
        print(f"\n--- Creating dynamic transformed view '{PG_TRANSFORMED_VIEW}' ---")
        
        # Build the dynamic CASE statements for the view
        pivot_cases = [f"""MAX(CASE WHEN "TagName" = '{tag}' THEN "Val" END) AS "{tag}" """ for tag_index, tag in tag_data]
        pivot_cases_str = ",\n             ".join(pivot_cases)

        create_view_query = f"""
        CREATE OR REPLACE VIEW "{PG_TRANSFORMED_VIEW}" AS
        WITH MappedData AS (
            SELECT
                r."DateAndTime",
                COALESCE(m."TagName", 'Unmapped') AS "TagName",
                r."Val",
                r."TagIndex"
            FROM
                "{PG_RAW_TABLE}" AS r
            LEFT JOIN
                "{PG_MAPPING_TABLE}" AS m ON r."TagIndex" = m."TagIndex"
        )
        SELECT
            "DateAndTime",
            {pivot_cases_str},
            MAX(CASE WHEN "TagName" = 'Unmapped' THEN "TagIndex" END) AS "Unmapped_TagIndex",
            MAX(CASE WHEN "TagName" = 'Unmapped' THEN "Val" END) AS "Unmapped_Value"
        FROM
            MappedData
        GROUP BY
            "DateAndTime"
        ORDER BY
            "DateAndTime" DESC;
        """
        
        pg_cursor.execute(create_view_query)
        pg_conn.commit()
        print(f"✅ View '{PG_TRANSFORMED_VIEW}' created or replaced successfully with {len(tag_data)} columns.")

    except psycopg2.Error as e:
        print(f"❌ PostgreSQL connection or query failed. Error: {e}", file=sys.stderr)
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}", file=sys.stderr)
    finally:
        if pg_conn:
            pg_conn.close()
        print("\nScript finished.")

if __name__ == "__main__":
    process_and_insert_tags()