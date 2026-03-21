from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models import Variable
from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import numpy as np
import logging
import os # เพิ่ม os เพื่อใช้สร้างโฟลเดอร์

# --- 1. The Cleaning Task (Upstream) ---
def clean_taxi_data(**context):
    api_url = "https://data.cityofnewyork.us/resource/t29m-gskq.csv?$limit=1000"
    
    # 🔴 แก้ไข: สั่งให้สร้างโฟลเดอร์ dataset หากยังไม่มี
    os.makedirs("/opt/airflow/dataset", exist_ok=True)
    clean_path = "/opt/airflow/dataset/clean_taxi_data_temp.csv"
    
    logging.info(f"Downloading 1,000 rows directly from NYC OpenData API...")
    df = pd.read_csv(api_url)
    df.to_csv(clean_path, index=False)
    
    context['ti'].xcom_push(key="clean_path", value=clean_path)
    logging.info(f"API data saved to {clean_path} and pushed to XCom.")

# --- 2. The Transformation Task (Downstream) ---
def transform_taxi_data(**context):
    ti = context['ti']
    clean_path = ti.xcom_pull(key="clean_path", task_ids="clean_taxi_data")
    
    if not clean_path:
        raise ValueError("No 'clean_path' found in XCom.")

    df = pd.read_csv(clean_path, parse_dates=['tpep_pickup_datetime', 'tpep_dropoff_datetime'])

    # Compute derived features
    duration_delta = df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']
    df['trip_duration_minutes'] = duration_delta.dt.total_seconds() / 60.0
    df['speed_mph'] = df['trip_distance'] / (df['trip_duration_minutes'] / 60.0)
    df['fare_per_mile'] = df['fare_amount'] / df['trip_distance']
    df['pickup_hour'] = df['tpep_pickup_datetime'].dt.hour
    df['pickup_day_of_week'] = df['tpep_pickup_datetime'].dt.dayofweek
    df['is_weekend'] = df['pickup_day_of_week'] >= 5

    # Filter out anomalous records
    initial_row_count = len(df)
    df.replace([np.inf, -np.inf], np.nan, inplace=True) 
    df = df[(df['speed_mph'] <= 80) & (df['trip_duration_minutes'] >= 1)]
    logging.info(f"Filtered out {initial_row_count - len(df)} anomalous records.")

    output_path = "/opt/airflow/data/nyc_taxi_transformed.csv"
    df.to_csv(output_path, index=False)
    
    # 🔴 ดัน XCom ด้วยชื่อ transformed_path
    ti.xcom_push(key="transformed_path", value=output_path)

# --- 3. The Load Task (Database) ---
def load_taxi_model(**context):
    ti = context['ti']
    # 🔴 ดึง XCom ด้วยชื่อ transformed_path ให้ตรงกัน
    csv_path = ti.xcom_pull(key="transformed_path", task_ids="transform_taxi_data")
    
    if not csv_path:
        raise ValueError("No CSV path found in XCom.")

    df = pd.read_csv(csv_path)

    # 🔴 เชื่อมต่อ MySQL ด้วยข้อมูลจาก Variables
    mysql_host = Variable.get("MYSQL_HOST")
    mysql_user = Variable.get("MYSQL_USER")
    mysql_pass = Variable.get("MYSQL_PASS")
    mysql_db = Variable.get("MYSQL_DB")
    
    db_url = f"mysql+pymysql://{mysql_user}:{mysql_pass}@{mysql_host}/{mysql_db}"
    engine = create_engine(db_url)

    # --- สร้าง Star Schema (dim_payment, dim_time, fact_trips) ---
    if 'payment_type' in df.columns:
        dim_payment = df[['payment_type']].drop_duplicates().reset_index(drop=True)
        dim_payment['payment_id'] = dim_payment.index + 1
    else:
        dim_payment = pd.DataFrame(columns=['payment_type', 'payment_id'])

    if 'tpep_pickup_datetime' in df.columns:
        pickup_dt = pd.to_datetime(df['tpep_pickup_datetime'])
        time_features = pd.DataFrame({
            'hour': pickup_dt.dt.hour,
            'day': pickup_dt.dt.day,
            'is_weekend': pickup_dt.dt.dayofweek >= 5
        })
        dim_time = time_features.drop_duplicates().reset_index(drop=True)
        dim_time['time_id'] = dim_time.index + 1
    else:
        dim_time = pd.DataFrame(columns=['hour', 'day', 'is_weekend', 'time_id'])

    df_fact = df.copy()
    if 'payment_type' in df.columns:
        df_fact = df_fact.merge(dim_payment, on='payment_type', how='left')
    if 'tpep_pickup_datetime' in df.columns:
        df_fact['hour'] = pd.to_datetime(df['tpep_pickup_datetime']).dt.hour
        df_fact['day'] = pd.to_datetime(df['tpep_pickup_datetime']).dt.day
        df_fact['is_weekend'] = pd.to_datetime(df['tpep_pickup_datetime']).dt.dayofweek >= 5
        df_fact = df_fact.merge(dim_time, on=['hour', 'day', 'is_weekend'], how='left')

    fact_cols = ['time_id', 'payment_id', 'fare_amount', 'trip_distance', 'passenger_count']
    existing_cols = [col for col in fact_cols if col in df_fact.columns]
    fact_trips = df_fact[existing_cols]

    logging.info("Starting database load...")
    dim_time.to_sql('dim_time', con=engine, if_exists='replace', chunksize=1000, index=False)
    dim_payment.to_sql('dim_payment', con=engine, if_exists='replace', chunksize=1000, index=False)
    fact_trips.to_sql('fact_trips', con=engine, if_exists='replace', chunksize=1000, index=False)
    logging.info("Load Complete!")




    