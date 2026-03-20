import logging
import pandas as pd
from sqlalchemy import create_engine
from airflow.models import Variable

def load_taxi_model(**context):
    """
    Pulls transformed data from XCom, models it into a star schema,
    and loads it into a MySQL database.
    """
    # 1. Pull the transformed CSV path from XCom
    # Make sure 'task_ids' matches the actual name of your transform task
    ti = context['ti']
    csv_path = ti.xcom_pull(key='output_path', task_ids='transform_taxi_data')
    
    if not csv_path:
        raise ValueError("No CSV path found in XCom. Check the upstream transform task.")

    logging.info(f"Loading transformed data from: {csv_path}")

    # 2. Load data with pandas
    df = pd.read_csv(csv_path)

    # 3. Connect to MySQL using SQLAlchemy & Airflow Variables
    # Note: Requires the 'pymysql' package installed in your Airflow Docker image
    mysql_host = Variable.get("MYSQL_HOST")
    mysql_user = Variable.get("MYSQL_USER")
    mysql_pass = Variable.get("MYSQL_PASS")
    mysql_db = Variable.get("MYSQL_DB")
    
    # Create connection string (using pymysql driver)
    db_url = f"mysql+pymysql://{mysql_user}:{mysql_pass}@{mysql_host}/{mysql_db}"
    engine = create_engine(db_url)

    # 4. Create Star-Schema DataFrames
    
    # --- A. Create dim_payment ---
    # Extract unique payment types and assign an ID
    dim_payment = df[['payment_type']].drop_duplicates().reset_index(drop=True)
    dim_payment['payment_id'] = dim_payment.index + 1

    # --- B. Create dim_time ---
    # Convert pickup time to datetime to extract parts
    pickup_dt = pd.to_datetime(df['tpep_pickup_datetime'])
    
    # Create a temporary dataframe to extract time features
    time_features = pd.DataFrame({
        'hour': pickup_dt.dt.hour,
        'day': pickup_dt.dt.day,
        'is_weekend': pickup_dt.dt.dayofweek >= 5  # 5=Sat, 6=Sun
    })
    
    # Get unique combinations of hour/day/weekend
    dim_time = time_features.drop_duplicates().reset_index(drop=True)
    dim_time['time_id'] = dim_time.index + 1

    # --- C. Create fact_trips ---
    # Map the new dimension IDs (Foreign Keys) back to the main dataframe
    
    # Merge payment_id
    df_fact = df.merge(dim_payment, on='payment_type', how='left')
    
    # Merge time_id (we need to temporarily add the time features to the main df to match)
    df_fact['hour'] = pickup_dt.dt.hour
    df_fact['day'] = pickup_dt.dt.day
    df_fact['is_weekend'] = pickup_dt.dt.dayofweek >= 5
    df_fact = df_fact.merge(dim_time, on=['hour', 'day', 'is_weekend'], how='left')

    # Select only the FKs and the Measures for the final Fact table
    fact_cols = [
        'time_id', 'payment_id', 'fare_amount', 'trip_distance', 
        'trip_duration_minutes', 'speed_mph', 'fare_per_mile', 'passenger_count'
    ]
    fact_trips = df_fact[fact_cols]

    # 5. Use pandas to_sql to load tables into MySQL
    logging.info("Starting database load...")
    
    # Load Dimensions First (Best practice for Referential Integrity)
    dim_time.to_sql('dim_time', con=engine, if_exists='replace', chunksize=1000, index=False)
    dim_payment.to_sql('dim_payment', con=engine, if_exists='replace', chunksize=1000, index=False)
    
    # Load Facts
    fact_trips.to_sql('fact_trips', con=engine, if_exists='replace', chunksize=1000, index=False)

    # 6. Log row counts
    logging.info(f"Successfully loaded {len(dim_time)} rows into dim_time table.")
    logging.info(f"Successfully loaded {len(dim_payment)} rows into dim_payment table.")
    logging.info(f"Successfully loaded {len(fact_trips)} rows into fact_trips table.")