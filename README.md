# 🏥 Global Chronic Disease & Life Expectancy Intelligence Pipeline

> **ETL + Machine Learning Project | Data Science UTS**
> Tech Stack: Apache Airflow (Astronomer) · MySQL 8.0 · Metabase · Python

---

## 📋 Judul Besar

**"Global Chronic Disease & Life Expectancy Intelligence: From Raw Data to AI-Powered Health Risk Predictions"**

*Pipeline ETL otomatis untuk menganalisis tren penyakit kronis global (Kardiovaskular, HIV/AIDS, Diabetes & Obesitas) dan memprediksi Life Expectancy menggunakan Random Forest dan XGBoost.*

---

## ❓ 5 Research Questions

| # | Question | Dijawab dengan |
|---|----------|---------------|
| **Q1** | Bagaimana tren Life Expectancy global selama 30 tahun terakhir (1990–2022), dan faktor penyakit kronis apa (CVD, HIV, Diabetes) yang paling berkorelasi dengan penurunan harapan hidup per negara? | 📊 Dashboard Metabase: Line chart tren LE + Scatter correlation matrix |
| **Q2** | Negara mana yang mengalami *double burden* — beban penyakit menular (HIV/AIDS) DAN penyakit tidak menular (CVD, Diabetes) yang tinggi secara bersamaan — dan bagaimana distribusinya per region dunia? | 📊 Dashboard Metabase: Bubble chart + Choropleth map per region |
| **Q3** | Apakah terdapat kesenjangan gender (*gender gap*) yang signifikan dalam Life Expectancy dan angka kematian CVD antar region dunia, dan apakah akses layanan kesehatan (dokter per 1000 penduduk) mempengaruhi gap ini? | 📊 Dashboard Metabase: Bar chart gender gap + Scatter (physicians vs LE gap) |
| **Q4** 🤖 | Dapatkah kita mengklasifikasikan negara ke dalam kategori risiko kesehatan (**Low / Medium / High / Critical**) berdasarkan indikator penyakit kronis, akses layanan kesehatan, dan faktor sosio-ekonomi? | 🤖 **Random Forest Classifier** + Dashboard: Confusion matrix, feature importance, risk map |
| **Q5** 🤖 | Berapa prediksi Life Expectancy 5 tahun ke depan (2023–2027) per negara berdasarkan tren historis beban CVD, HIV, Diabetes, dan pengeluaran kesehatan? | 🤖 **XGBoost Regressor** + Dashboard: Forecast chart, RMSE, confidence intervals, ranking |

---

## 🗄️ Arsitektur Data

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                 │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐  │
│  │  REST Countries  │  │  World Bank API  │  │  Our World in    │  │
│  │  API (v3.1)      │  │  (JSON, free)    │  │  Data (GitHub    │  │
│  │  restcountries   │  │  api.worldbank   │  │  raw CSV/JSON)   │  │
│  │  .com/v3.1/all   │  │  .org/v2         │  │  owid-datasets   │  │
│  └────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘  │
└───────────┼─────────────────────┼─────────────────────┼────────────┘
            │                     │                     │
            ▼                     ▼                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   AIRFLOW DAG (Apache Airflow 3.x)                  │
│                                                                     │
│  EXTRACT → TRANSFORM → LOAD → ML TRAINING → DQ CHECK               │
│                                                                     │
│  extract_countries()     transform_countries()    load_countries()  │
│  extract_world_bank()    transform_world_bank()   load_world_bank() │
│  extract_cardiovascular() transform_cardiovascular() load_cvd()     │
│  extract_hiv_aids()      transform_hiv_aids()     load_hiv()        │
│  extract_diabetes_obesity() transform_diabetes()  load_diabetes()   │
│                                                                     │
│  ml_train_risk_classifier()  ← Random Forest (Q4)                  │
│  ml_train_life_expectancy_forecaster() ← XGBoost (Q5)              │
│  data_quality_check()                                               │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         MySQL 8.0                                   │
│                    Database: health_analytics                        │
│                                                                     │
│  dim_countries          fact_cardiovascular    fact_hiv_aids        │
│  (250+ countries)       (CVD deaths, DALYs)   (prevalence, ART)    │
│                                                                     │
│  fact_life_expectancy   fact_diabetes_obesity  ml_predictions       │
│  (LE, mortality, HCI)   (prevalence, BMI)     ml_model_metrics      │
└────────────────────────────┬────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Metabase Dashboard                               │
│                    (localhost:3000)                                 │
│                                                                     │
│  📈 Trend Analysis  🗺️ Global Maps  🤖 AI Predictions  ⚡ KPIs     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 🗃️ Struktur Tabel MySQL

| Tabel | Rows (est.) | Keterangan |
|-------|-------------|-----------|
| `dim_countries` | ~250 | Country dimension: region, income group, koordinat |
| `fact_life_expectancy` | ~8,000 | World Bank: LE total/male/female, infant mortality, healthcare access |
| `fact_cardiovascular` | ~6,000 | OWID: CVD deaths, DALYs, risk factors per negara per tahun |
| `fact_hiv_aids` | ~5,000 | OWID: HIV prevalence, ART coverage, AIDS deaths |
| `fact_diabetes_obesity` | ~5,000 | OWID: Diabetes prevalence, obesity %, BMI |
| `ml_predictions` | ~500 | Output RF Classifier (Q4) + XGBoost Regressor (Q5) |
| `ml_model_metrics` | ~10 | Accuracy, RMSE, F1, top features per model run |

---

## 🤖 Model Machine Learning

### Q4 — Random Forest Classifier
- **Target**: `health_risk_category` (Low / Medium / High / Critical)
- **Features**: CVD death rate, HIV prevalence, Diabetes %, Obesity %, Life Expectancy, Infant Mortality, Physicians per 1000, Health Expenditure % GDP
- **Algorithm**: `RandomForestClassifier(n_estimators=200, max_depth=10, class_weight='balanced')`
- **Output**: Simpan ke `ml_predictions.predicted_risk_cat` + `ml_model_metrics.accuracy`

### Q5 — XGBoost Regressor
- **Target**: `life_expectancy_total` (forecast tahun berikutnya)
- **Features**: + lag features (LE tahun lalu, 2 tahun lalu, delta LE), CVD burden, HIV burden, Healthcare access
- **Algorithm**: `XGBRegressor(n_estimators=500, learning_rate=0.05, early_stopping_rounds=30)`
- **Output**: Simpan ke `ml_predictions.predicted_le_total` + CI bounds + `ml_model_metrics.rmse`

---

## 📊 Panduan Dashboard Metabase

### Setup Koneksi
1. Buka Metabase di `http://localhost:3000`
2. Admin Panel → Databases → Add Database
3. Database Type: `MySQL`
4. Host: `mysql` | Port: `3306`
5. Database: `health_analytics` | User: `airflow_user` | Pass: `airflow_pass`

### Chart yang Direkomendasikan

#### Dashboard 1: Life Expectancy Trends (Q1)
```sql
-- Tren LE global per region
SELECT dc.region, le.year,
       ROUND(AVG(le.life_expectancy_total), 2) AS avg_le
FROM fact_life_expectancy le
JOIN dim_countries dc ON le.country_code = dc.country_code
WHERE le.year >= 1990
GROUP BY dc.region, le.year
ORDER BY le.year;
```
→ **Chart Type**: Line Chart | X-axis: year | Y-axis: avg_le | Group by: region

#### Dashboard 2: Double Burden Map (Q2)
```sql
-- Double burden score per negara
SELECT dc.country_name, dc.region,
       AVG(hiv.prevalence_pct)         AS avg_hiv_prev,
       AVG(cvd.age_std_death_rate)      AS avg_cvd_rate,
       AVG(dob.diabetes_prevalence_pct) AS avg_diab_prev,
       (AVG(hiv.prevalence_pct) * 10) +
       (AVG(cvd.age_std_death_rate) / 50) +
       AVG(dob.diabetes_prevalence_pct) AS double_burden_score
FROM dim_countries dc
LEFT JOIN fact_hiv_aids hiv ON dc.country_code = hiv.country_code
LEFT JOIN fact_cardiovascular cvd ON dc.country_code = cvd.country_code
LEFT JOIN fact_diabetes_obesity dob ON dc.country_code = dob.country_code
WHERE hiv.year = 2019
GROUP BY dc.country_name, dc.region
ORDER BY double_burden_score DESC
LIMIT 30;
```
→ **Chart Type**: Bubble Chart | X: avg_hiv_prev | Y: avg_cvd_rate | Size: avg_diab_prev

#### Dashboard 3: Gender Gap (Q3)
```sql
-- Gender gap LE per region
SELECT dc.region, le.year,
       ROUND(AVG(le.life_expectancy_female - le.life_expectancy_male), 2) AS gender_gap_years,
       ROUND(AVG(le.physicians_per_1000), 3) AS physicians_per_1000
FROM fact_life_expectancy le
JOIN dim_countries dc ON le.country_code = dc.country_code
WHERE le.life_expectancy_female IS NOT NULL
  AND le.life_expectancy_male IS NOT NULL
GROUP BY dc.region, le.year
ORDER BY gender_gap_years DESC;
```
→ **Chart Type**: Bar Chart | X: region | Y: gender_gap_years + physicians_per_1000

#### Dashboard 4: AI Risk Classification (Q4)
```sql
-- Risk category distribution
SELECT predicted_risk_cat, COUNT(*) AS country_count,
       ROUND(AVG(risk_probability) * 100, 1) AS avg_confidence_pct
FROM ml_predictions
WHERE model_name = 'RandomForest_RiskClassifier'
GROUP BY predicted_risk_cat
ORDER BY FIELD(predicted_risk_cat, 'Critical','High','Medium','Low');
```
→ **Chart Type**: Pie Chart / Donut | + Table showing country list per category

```sql
-- Feature importance from model metrics
SELECT model_name, top_features, accuracy, f1_score
FROM ml_model_metrics
WHERE model_name = 'RandomForest_RiskClassifier';
```
→ **Chart Type**: Table Card | Show top_features as JSON

#### Dashboard 5: Life Expectancy Forecast (Q5)
```sql
-- Forecast vs actual LE
SELECT mp.country_code, dc.country_name, dc.region,
       mp.predicted_le_total AS forecast_le_2023,
       mp.predicted_le_lower AS ci_lower,
       mp.predicted_le_upper AS ci_upper,
       le.life_expectancy_total AS actual_le_2022
FROM ml_predictions mp
JOIN dim_countries dc ON mp.country_code = dc.country_code
LEFT JOIN fact_life_expectancy le
    ON mp.country_code = le.country_code AND le.year = 2022
WHERE mp.model_name = 'XGBoost_LifeExpectancyForecaster'
ORDER BY forecast_le_2023 DESC;
```
→ **Chart Type**: Bar Chart + Error bars (CI) | + Model RMSE KPI card

---

## 🚀 Cara Menjalankan

### Step 1 — Start Services
```bash
astro dev start
```
Ini akan menjalankan: Airflow Webserver, Scheduler, MySQL, dan Metabase secara bersamaan.

### Step 2 — Akses Airflow UI
```
http://localhost:8080
Username: admin
Password: admin
```

### Step 3 — Trigger DAG
1. Buka Airflow UI → DAGs
2. Cari `global_health_etl_pipeline`
3. Toggle ON → Klik ▶️ "Trigger DAG"
4. Monitor progress di Graph view

### Step 4 — Akses Metabase
```
http://localhost:3000
```
Setup koneksi MySQL → Buat dashboard sesuai panduan di atas.

---

## 📁 Struktur Project

```
FIX PROJECT UTS/
├── dags/
│   └── global_health_etl_pipeline.py   ← MAIN DAG (ETL + ML)
├── include/
│   └── sql/
│       └── init_schema.sql             ← MySQL Schema (7 tabel)
├── docker-compose.override.yml         ← MySQL + Metabase services
├── requirements.txt                    ← Python dependencies
├── Dockerfile                          ← Astro Runtime base image
├── README.md                           ← Dokumentasi ini
└── .env                                ← Environment variables
```
