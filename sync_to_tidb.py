import os
import sys
import pandas as pd
import pymysql
from sqlalchemy import create_engine, text

TIDB_HOST = "gateway01.ap-northeast-1.prod.aws.tidbcloud.com"
TIDB_PORT = 4000
TIDB_USER = "31BjYwwhUu1B9S2.root"
TIDB_PASS = "tL1mnHnmvznD1FAw"

print("1. Menghubungkan ke TiDB Cloud untuk inisialisasi database...")
try:
    init_conn = pymysql.connect(
        host=TIDB_HOST,
        port=TIDB_PORT,
        user=TIDB_USER,
        password=TIDB_PASS,
        ssl={"ssl_verify_cert": False},
        autocommit=True
    )
    cursor = init_conn.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS health_analytics;")
    print("Database 'health_analytics' berhasil dipastikan ada di TiDB Cloud!")
    cursor.close()
    init_conn.close()
except Exception as e:
    print(f"Error saat inisialisasi DB: {e}")
    sys.exit(1)

# Hubungkan via SQLAlchemy Engine ke health_analytics
cloud_url = f"mysql+pymysql://{TIDB_USER}:{TIDB_PASS}@{TIDB_HOST}:{TIDB_PORT}/health_analytics?ssl_verify_cert=false"
cloud_engine = create_engine(cloud_url)

dataset_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "datasets")
tables = [
    "dim_countries",
    "fact_life_expectancy",
    "fact_cardiovascular",
    "fact_hiv_aids",
    "fact_diabetes_obesity"
]

print("\n2. Mengunggah data 5 tabel ke TiDB Cloud...")
for tbl in tables:
    csv_file = os.path.join(dataset_dir, f"{tbl}.csv")
    if os.path.exists(csv_file):
        print(f"-> Membaca {tbl}.csv...")
        df = pd.read_csv(csv_file)
        print(f"-> Mengunggah {tbl} ({len(df):,} baris, {df.shape[1]} kolom) ke TiDB...")
        df.to_sql(tbl, con=cloud_engine, if_exists="replace", index=False, chunksize=1000)
        print(f"   SUKSES: Tabel '{tbl}' terunggah.")
    else:
        print(f"   WARNING: File {csv_file} tidak ditemukan!")

print("\n3. Memverifikasi jumlah baris di TiDB Cloud:")
with cloud_engine.connect() as conn:
    for tbl in tables:
        count = conn.execute(text(f"SELECT COUNT(*) FROM {tbl}")).scalar()
        print(f"   • {tbl}: {count:,} baris")

print("\nSEMUA DATA BERHASIL DISINKRONISASI KE TIDB CLOUD!")
