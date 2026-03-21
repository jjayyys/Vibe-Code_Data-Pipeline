import pandas as pd
import logging

def clean_taxi_data(**context):
    """
    ฟังก์ชันทำความสะอาดข้อมูลแท็กซี่ปี 2018
    รับไฟล์ต่อมาจาก Ingest Task และส่งต่อให้ Transform Task
    """
    ti = context['ti']
    
    # 1. รับไฟล์มาจากเพื่อนที่ทำ Ingest (ต้องตกลงชื่อ task_id กับเพื่อนให้ตรงกันนะครับ)
    # สมมติว่าเพื่อนตั้งชื่อ task_id ว่า 'ingest_data_task'
    raw_path = ti.xcom_pull(key="raw_file_path", task_ids="ingest_taxi_data")
    
    if not raw_path:
        raise ValueError("หาไฟล์จาก Ingest ไม่เจอ! เช็คว่าเพื่อนส่ง key='raw_path' และ task_ids ตรงกันไหม")
        
    logging.info(f"เริ่มทำความสะอาดข้อมูลจากไฟล์: {raw_path}")
    df = pd.read_csv(raw_path)
    initial_count = len(df)
    
    # 2. กระบวนการทำความสะอาดข้อมูล (Clean)
    # ลบแถวที่มีค่าว่าง (Null) ในคอลัมน์สำคัญ
    df = df.dropna(subset=['fare_amount', 'trip_distance', 'tpep_pickup_datetime'])
    
    # แปลงคอลัมน์วันที่ให้เป็นรูปแบบ Datetime ที่ถูกต้อง
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'], errors='coerce')
    df = df.dropna(subset=['tpep_pickup_datetime'])
    
    # กรองค่าโดยสาร (ต้องมากกว่า 0 และไม่เกิน 500) และระยะทาง (มากกว่า 0 และไม่เกิน 100)
    df = df[(df['fare_amount'] > 0) & (df['fare_amount'] <= 500)]
    df = df[(df['trip_distance'] > 0) & (df['trip_distance'] <= 100)]
    
    # กรองรหัสโซนสถานที่ (Location ID ของปี 2018 ต้องอยู่ระหว่าง 1 ถึง 265)
    df = df[df['PULocationID'].between(1, 265) & df['DOLocationID'].between(1, 265)]
    
    final_count = len(df)
    logging.info(f"ทำความสะอาดเสร็จสิ้น! ลบข้อมูลขยะไป: {initial_count - final_count} แถว. เหลือข้อมูลสุทธิ: {final_count} แถว")

    # 3. บันทึกไฟล์ที่คลีนแล้ว และส่งต่อให้เพื่อนทำ Transform
    clean_path = '/tmp/nyc_taxi_clean_2018.csv'
    df.to_csv(clean_path, index=False)
    
    # ดันชื่อไฟล์เข้า XCom ให้เพื่อนคนที่ทำ Transform ดึงไปใช้ต่อ
    ti.xcom_push(key="clean_path", value=clean_path)
    logging.info(f"บันทึกไฟล์ข้อมูลที่สะอาดแล้วไปที่: {clean_path}")
    
    return clean_path