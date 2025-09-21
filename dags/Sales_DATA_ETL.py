import pandas as pd
import requests
from io import StringIO
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

# Define DAG
@dag(
    dag_id="Sales_DATA_ETL",
    schedule="@once",
    catchup=False,
    start_date=datetime(2025, 1, 1),
    tags=["example"],
)
def sales_data_etl():

    @task()
    def get_data():
        """
        Download dataset from URL directly into memory (pandas DataFrame).
        Returns the raw DataFrame without saving to disk.
        """
        import requests
        import pandas as pd
        from io import StringIO
        import logging
        
        # URL to the sales data CSV
        url = "https://raw.githubusercontent.com/plotly/datasets/master/sales_data_sample.csv"
        
        try:
            logging.info(f"Downloading data from: {url}")
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            # Read CSV content directly into pandas DataFrame
            csv_content = StringIO(response.text)
            df = pd.read_csv(csv_content)
            
            logging.info(f"Downloaded dataset with {len(df)} rows and {len(df.columns)} columns")
            logging.info(f"Columns: {list(df.columns)}")
            
            return df.to_json()  # Convert to JSON string for passing between tasks
            
        except requests.RequestException as e:
            logging.error(f"Failed to download from URL: {e}")
            # Create sample data as fallback
            sample_data = {
                'ORDERNUMBER': [10107, 10121, 10134, 10145, 10159],
                'QUANTITYORDERED': [30, 50, 41, 45, 49],
                'PRICEEACH': [95.70, 81.35, 94.74, 83.26, 100.00],
                'ORDERDATE': ['2/24/2003 0:00', '5/7/2003 0:00', '7/1/2003 0:00', '8/25/2003 0:00', '10/10/2003 0:00'],
                'STATUS': ['Shipped', 'Shipped', 'Shipped', 'Shipped', 'Shipped'],
                'PRODUCTLINE': ['Motorcycles', 'Motorcycles', 'Motorcycles', 'Motorcycles', 'Motorcycles'],
                'MSRP': [95, 81, 94, 83, 100]
            }
            
            df = pd.DataFrame(sample_data)
            logging.info("Using sample data for testing")
            return df.to_json()

    @task()
    def transform_data(json_data: str):
        """
        Clean and transform the dataset in memory.
        """
        import pandas as pd
        import logging
        
        # Convert JSON back to DataFrame
        df = pd.read_json(json_data)
        logging.info(f"Processing {len(df)} rows")
        
        # Data cleaning
        df = df.dropna()
        
        # Feature Engineering
        if "QUANTITYORDERED" in df.columns and "PRICEEACH" in df.columns:
            df["TOTAL_REVENUE"] = df["QUANTITYORDERED"] * df["PRICEEACH"]
            logging.info("Added TOTAL_REVENUE column")

        if "TOTAL_REVENUE" in df.columns:
            df["EST_PROFIT"] = df["TOTAL_REVENUE"] * 0.2
            df["HIGH_VALUE_ORDER"] = df["TOTAL_REVENUE"] > 5000
            logging.info("Added EST_PROFIT and HIGH_VALUE_ORDER columns")

        if "ORDERDATE" in df.columns:
            df["ORDERDATE"] = pd.to_datetime(df["ORDERDATE"], errors="coerce")
            df["ORDER_YEAR"] = df["ORDERDATE"].dt.year
            df["ORDER_MONTH"] = df["ORDERDATE"].dt.month
            logging.info("Added date-based columns")

        logging.info(f"Transformation complete. Final dataset has {len(df)} rows and {len(df.columns)} columns")
        return df.to_json()

    @task()
    def load_to_postgres(json_data: str):
        """
        Load transformed data directly into Postgres from memory.
        """
        import pandas as pd
        import logging
        from io import StringIO
        
        try:
            # Convert JSON back to DataFrame
            df = pd.read_json(json_data)
            logging.info(f"DataFrame loaded with {len(df)} rows and columns: {list(df.columns)}")
            
            # Log sample data for debugging
            logging.info(f"First few rows:\n{df.head()}")
            logging.info(f"Data types:\n{df.dtypes}")
            
            # Test connection first
            hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
            
            # Test basic connection
            logging.info("Testing PostgreSQL connection...")
            conn = hook.get_conn()
            cur = conn.cursor()
            
            cur.execute("SELECT version();")
            version = cur.fetchone()
            logging.info(f"PostgreSQL version: {version[0]}")

            # Create table with proper data types
            logging.info("Creating/recreating table...")
            cur.execute("""
                DROP TABLE IF EXISTS sales_data;
                CREATE TABLE sales_data (
                    ORDERNUMBER BIGINT,
                    QUANTITYORDERED INTEGER,
                    PRICEEACH DECIMAL(10,2),
                    ORDERDATE TIMESTAMP,
                    STATUS VARCHAR(50),
                    PRODUCTLINE VARCHAR(100),
                    MSRP INTEGER,
                    TOTAL_REVENUE DECIMAL(12,2),
                    EST_PROFIT DECIMAL(12,2),
                    HIGH_VALUE_ORDER BOOLEAN,
                    ORDER_YEAR INTEGER,
                    ORDER_MONTH INTEGER
                );
            """)
            conn.commit()
            logging.info("Table created successfully")

            # Prepare data for loading - handle NaN values and data types
            df_clean = df.copy()
            
            # Replace NaN values with None for proper NULL handling
            df_clean = df_clean.where(pd.notna(df_clean), None)
            
            # Ensure proper data types
            if 'ORDERDATE' in df_clean.columns:
                df_clean['ORDERDATE'] = pd.to_datetime(df_clean['ORDERDATE'], errors='coerce')
                
            # Convert boolean column properly
            if 'HIGH_VALUE_ORDER' in df_clean.columns:
                df_clean['HIGH_VALUE_ORDER'] = df_clean['HIGH_VALUE_ORDER'].astype(bool)

            logging.info("Data cleaned and prepared for loading")

            # Method 1: Try using pandas to_sql (simpler approach)
            try:
                logging.info("Attempting to load data using pandas to_sql...")
                df_clean.to_sql(
                    'sales_data', 
                    conn, 
                    if_exists='append', 
                    index=False, 
                    method='multi',
                    chunksize=1000
                )
                logging.info("Data loaded successfully using pandas to_sql")
                
            except Exception as e:
                logging.warning(f"pandas to_sql failed: {e}")
                logging.info("Trying alternative COPY method...")
                
                # Method 2: Use COPY with proper CSV formatting
                csv_buffer = StringIO()
                
                # Format data properly for PostgreSQL COPY
                df_clean.to_csv(
                    csv_buffer, 
                    index=False, 
                    header=False,
                    na_rep='\\N',  # PostgreSQL NULL representation
                    date_format='%Y-%m-%d %H:%M:%S'
                )
                csv_buffer.seek(0)
                
                # Show first few lines of CSV for debugging
                csv_sample = csv_buffer.getvalue()[:500]
                logging.info(f"CSV sample:\n{csv_sample}")
                csv_buffer.seek(0)

                cur.copy_expert(
                    """COPY sales_data FROM STDIN WITH 
                       CSV DELIMITER ',' 
                       NULL '\\N' 
                       QUOTE '"'""",
                    csv_buffer
                )
                logging.info("Data loaded successfully using COPY")

            conn.commit()

            # Verify the load
            cur.execute("SELECT COUNT(*) FROM sales_data;")
            count = cur.fetchone()[0]
            
            cur.execute("SELECT * FROM sales_data LIMIT 3;")
            sample_rows = cur.fetchall()
            logging.info(f"Sample loaded data: {sample_rows}")
            
            cur.close()
            conn.close()

            logging.info(f"Successfully loaded {count} records into sales_data table")
            return f"Loaded {count} records into Postgres table 'sales_data'"
            
        except Exception as e:
            logging.error(f"Error in load_to_postgres: {str(e)}")
            logging.error(f"Error type: {type(e).__name__}")
            
            # Try to close connections if they exist
            try:
                if 'cur' in locals() and cur:
                    cur.close()
                if 'conn' in locals() and conn:
                    conn.close()
            except:
                pass
            
            raise e

    @task()
    def validate_data():
        """
        Validate the loaded data by running some basic queries.
        """
        import logging
        
        hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
        conn = hook.get_conn()
        cur = conn.cursor()

        # Run validation queries
        queries = {
            "Total records": "SELECT COUNT(*) FROM sales_data;",
            "High value orders": "SELECT COUNT(*) FROM sales_data WHERE high_value_order = true;",
            "Revenue range": "SELECT MIN(total_revenue), MAX(total_revenue) FROM sales_data;",
            "Date range": "SELECT MIN(orderdate), MAX(orderdate) FROM sales_data WHERE orderdate IS NOT NULL;",
            "Product lines": "SELECT COUNT(DISTINCT productline) FROM sales_data;"
        }

        results = {}
        for description, query in queries.items():
            cur.execute(query)
            result = cur.fetchone()
            results[description] = result
            logging.info(f"{description}: {result}")

        cur.close()
        conn.close()

        return results

    @task()
    def test_postgres_connection():
        """
        Test PostgreSQL connection before attempting data load.
        """
        import logging
        
        try:
            hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
            
            # Test connection
            conn = hook.get_conn()
            cur = conn.cursor()
            
            # Test basic queries
            cur.execute("SELECT current_database(), current_user, version();")
            result = cur.fetchone()
            
            logging.info(f"Database: {result[0]}")
            logging.info(f"User: {result[1]}")
            logging.info(f"Version: {result[2]}")
            
            # Test table creation permissions
            cur.execute("""
                CREATE TABLE IF NOT EXISTS connection_test (
                    id SERIAL PRIMARY KEY,
                    test_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            cur.execute("INSERT INTO connection_test DEFAULT VALUES;")
            cur.execute("SELECT COUNT(*) FROM connection_test;")
            count = cur.fetchone()[0]
            
            cur.execute("DROP TABLE connection_test;")
            conn.commit()
            
            cur.close()
            conn.close()
            
            logging.info(f"Connection test successful! Created and dropped test table with {count} rows")
            return "PostgreSQL connection test passed"
            
        except Exception as e:
            logging.error(f"PostgreSQL connection test failed: {str(e)}")
            logging.error(f"Error type: {type(e).__name__}")
            raise e

    # Task dependencies - completely in-memory pipeline
    raw_data = get_data()
    transformed_data = transform_data(raw_data)
    connection_test = test_postgres_connection()
    load_result = load_to_postgres(transformed_data)
    validation_result = validate_data()
    
    # Set dependencies
    connection_test >> load_result >> validation_result

dag_instance = sales_data_etl()