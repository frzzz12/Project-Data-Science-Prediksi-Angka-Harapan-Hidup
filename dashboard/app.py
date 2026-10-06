import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from pathlib import Path

# Path artifacts selalu relatif dari lokasi file script app.py
ARTIFACTS_DIR = Path(__file__).parent / "artifacts"

st.set_page_config(
    page_title="Global Health Analytics Dashboard",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─────────────────────────────────────────────────────────────────────────────
# CUSTOM CSS: Premium Modern Design (No Sidebar, Per-Chart Cards)
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

html, body, [class*="css"] {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    color: #0f172a;
}

/* ── PAKSA PURE LIGHT THEME DI SELURUH HALAMAN ── */
:root {
    color-scheme: light !important;
}
.stApp,
[data-testid="stAppViewContainer"],
[data-testid="stMain"],
[data-testid="stHeader"],
section.main {
    background-color: #f8fafc !important;
    color: #0f172a !important;
}

/* Sembunyikan toggle sidebar */
[data-testid="collapsedControl"] { display: none !important; }
section[data-testid="stSidebar"] { display: none !important; }

/* Header dashboard */
.dash-header {
    background: linear-gradient(135deg, #1e3a8a 0%, #2563eb 60%, #38bdf8 100%);
    color: white;
    padding: 2.2rem 2.4rem;
    border-radius: 16px;
    margin-bottom: 1.8rem;
    box-shadow: 0 10px 25px -5px rgba(37, 99, 235, 0.25);
}
.dash-header h1 {
    font-size: 2.3rem;
    font-weight: 800;
    color: #ffffff !important;
    margin: 0;
    letter-spacing: -0.02em;
}
.dash-header p {
    font-size: 1.02rem;
    color: #e0f2fe !important;
    margin: 0.5rem 0 0 0;
}

/* Judul Grafik (Sangat Jelas, Tebal, & Kontras Tinggi) */
.chart-title {
    font-size: 1.25rem !important;
    font-weight: 800 !important;
    color: #1e3a8a !important; /* Deep Navy Blue solid */
    display: flex;
    align-items: center;
    gap: 0.55rem;
    margin-top: 1.2rem;
    margin-bottom: 0.3rem;
    border-left: 5px solid #2563eb;
    padding-left: 0.85rem;
    background: linear-gradient(90deg, #eff6ff 0%, transparent 60%);
    padding-top: 0.25rem;
    padding-bottom: 0.25rem;
    border-radius: 0 8px 8px 0;
}
.chart-desc {
    font-size: 0.88rem !important;
    color: #334155 !important; /* Slate gelap sangat mudah dibaca */
    margin-bottom: 0.95rem;
    padding-left: 0.85rem;
}

/* Labels filter & dropdown */
label, [data-testid="stWidgetLabel"] p {
    color: #1e293b !important;
    font-weight: 600 !important;
    font-size: 0.84rem !important;
}

/* KPI metric cards: terang & bersih */
[data-testid="stMetric"] {
    background: #ffffff !important;
    border: 1px solid #e2e8f0 !important;
    border-radius: 12px !important;
    padding: 0.95rem 1.15rem !important;
    box-shadow: 0 2px 6px rgba(0,0,0,0.04) !important;
}
[data-testid="stMetricLabel"] {
    font-size: 0.76rem !important;
    font-weight: 700 !important;
    color: #475569 !important;
    text-transform: uppercase;
    letter-spacing: 0.04em;
}
[data-testid="stMetricValue"] {
    font-size: 1.7rem !important;
    font-weight: 800 !important;
    color: #1e3a8a !important;
}

.divider {
    border: none;
    border-top: 1px solid #e2e8f0;
    margin: 2rem 0;
}

/* Tooltip hover Plotly: latar gelap modern dengan teks putih kontras tajam */
.js-plotly-plot .hoverlayer path {
    fill: #0f172a !important;
    stroke: #334155 !important;
}
.js-plotly-plot .hoverlayer text {
    fill: #ffffff !important;
    font-weight: 500 !important;
}
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# LOAD DATASET
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data
def load_dataset():
    data_path = ARTIFACTS_DIR / "df_analytics.parquet"
    if not data_path.exists():
        st.error(f"File data tidak ditemukan di: {data_path}")
        st.stop()
    return pd.read_parquet(data_path)

df = load_dataset()

# Filter master options
ALL_REGIONS = sorted(df["region"].dropna().unique().tolist())
ALL_INCOME = ["Low income", "Lower middle income", "Upper middle income", "High income"]
YEAR_MIN = int(df["year"].min())
YEAR_MAX = int(df["year"].max())

# Layout standar Plotly (terang, elegan, judul & tooltip kontras tinggi)
def get_plotly_layout(height=380, margin=None):
    if margin is None:
        margin = dict(t=45, b=30, l=40, r=20)
    return dict(
        paper_bgcolor="#ffffff",
        plot_bgcolor="#f8fafc",
        title_font=dict(family="Inter, sans-serif", size=13, color="#1e3a8a"),
        font=dict(family="Inter, sans-serif", size=11, color="#334155"),
        height=height,
        margin=margin,
        hoverlabel=dict(
            bgcolor="#0f172a",
            bordercolor="#334155",
            font=dict(family="Inter, sans-serif", size=12, color="#ffffff"),
        ),
    )

# Dictionary nama indikator yang mudah dibaca
INDICATORS = {
    "Harapan Hidup Total (tahun)": "life_expectancy_total",
    "Kematian Kardiovaskular (CVD) per 100k": "cvd_deaths_per_100k",
    "Prevalensi Diabetes (%)": "diabetes_prevalence_pct",
    "Prevalensi Obesitas (%)": "obesity_prevalence_pct",
    "Kematian Bayi per 1.000 Lahir Hidup": "infant_mortality_per_1000",
    "Dokter per 1.000 Penduduk": "physicians_per_1000",
    "Pengeluaran Kesehatan (% GDP)": "health_expenditure_pct_gdp",
    "Prevalensi HIV (%)": "hiv_prevalence_pct",
    "Mean BMI Laki-laki": "mean_bmi_male",
    "Mean BMI Perempuan": "mean_bmi_female",
}


# ─────────────────────────────────────────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="dash-header">
    <h1>🌍 Global Health Analytics Dashboard</h1>
    <p>Eksplorasi visual interaktif indikator kesehatan dunia · 193 negara · 1990–2022 (WHO & World Bank)</p>
</div>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 1: OVERVIEW METRIC CARDS DENGAN FILTER SENDIRI
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="chart-title">📌 Ringkasan Metrik Global</div>
<div class="chart-desc">Rata-rata agregat metrik kesehatan dunia berdasarkan rentang tahun dan wilayah pilihan.</div>
""", unsafe_allow_html=True)

with st.container():
    kpi_f1, kpi_f2, kpi_f3 = st.columns([2, 3, 3])
    with kpi_f1:
        kpi_yr = st.slider("Rentang Tahun (Ringkasan):", YEAR_MIN, YEAR_MAX, (2000, 2022), key="kpi_slider_yr")
    with kpi_f2:
        kpi_rg = st.multiselect("Region:", ALL_REGIONS, default=ALL_REGIONS, key="kpi_multi_rg")
    with kpi_f3:
        kpi_inc = st.multiselect("Income Group:", ALL_INCOME, default=ALL_INCOME, key="kpi_multi_inc")

dff_kpi = df[
    df["year"].between(kpi_yr[0], kpi_yr[1]) &
    df["region"].isin(kpi_rg if kpi_rg else ALL_REGIONS) &
    df["income_group"].isin(kpi_inc if kpi_inc else ALL_INCOME)
]

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Negara Tercakup", f"{dff_kpi['country_code'].nunique():,}")
c2.metric("Avg Harapan Hidup", f"{dff_kpi['life_expectancy_total'].mean():.1f} thn")
c3.metric("Avg Kematian CVD", f"{dff_kpi['cvd_deaths_per_100k'].mean():.0f} /100k")
c4.metric("Avg Diabetes", f"{dff_kpi['diabetes_prevalence_pct'].mean():.1f}%")
c5.metric("Avg Obesitas", f"{dff_kpi['obesity_prevalence_pct'].mean():.1f}%")
c6.metric("Avg Dokter", f"{dff_kpi['physicians_per_1000'].mean():.2f} /1k")

st.markdown('<hr class="divider">', unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# GRAFIK 1: PETA DUNIA (CHOROPLETH) — FILTER PER GRAFIK
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="chart-title">🗺️ 1. Peta Distribusi Geografis Kesehatan Dunia</div>
<div class="chart-desc">Pilih indikator kesehatan dan tahun tertentu untuk melihat variasi distribusi spasial antar negara.</div>
""", unsafe_allow_html=True)

with st.container():
    m_col1, m_col2, m_col3, m_col4 = st.columns([3, 2, 2, 3])
    with m_col1:
        map_ind_name = st.selectbox("Pilih Indikator Peta:", list(INDICATORS.keys()), index=0, key="map_ind_sel")
    with m_col2:
        map_yr = st.slider("Tahun Peta:", YEAR_MIN, YEAR_MAX, 2019, key="map_yr_slider")
    with m_col3:
        map_scale = st.selectbox("Palet Warna:", ["Viridis", "Plasma", "Blues", "Reds", "Teal", "Spectral_r"], index=0, key="map_palette_sel")
    with m_col4:
        map_rg = st.multiselect("Filter Region (Opsional):", ALL_REGIONS, default=ALL_REGIONS, key="map_rg_sel")

map_col = INDICATORS[map_ind_name]
df_map = df[
    (df["year"] == map_yr) &
    (df["region"].isin(map_rg if map_rg else ALL_REGIONS))
][["country_code", map_col, "region", "income_group"]].dropna()

fig_map = px.choropleth(
    df_map,
    locations="country_code",
    color=map_col,
    hover_name="country_code",
    hover_data={"region": True, "income_group": True, map_col: ":.2f"},
    color_continuous_scale=map_scale,
    labels={map_col: map_ind_name},
)
fig_map.update_layout(
    **get_plotly_layout(height=430, margin=dict(l=0, r=0, t=10, b=0)),
    geo=dict(
        showframe=False,
        showcoastlines=True,
        projection_type="natural earth",
        bgcolor="white",
        landcolor="#f1f5f9",
        oceancolor="#f8fafc",
        showocean=True,
        showcountries=True,
        countrycolor="#cbd5e1",
    ),
    coloraxis_colorbar=dict(title=map_ind_name, thickness=12, len=0.7),
)
st.plotly_chart(fig_map, use_container_width=True)

st.markdown('<hr class="divider">', unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# GRAFIK 2: TREN WAKTU PER REGION / INCOME GROUP — FILTER PER GRAFIK
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="chart-title">📈 2. Analisis Tren Indikator Sepanjang Waktu (1990–2022)</div>
<div class="chart-desc">Lihat evolusi historis indikator kesehatan yang dikelompokkan berdasarkan wilayah geografis atau klasifikasi pendapatan.</div>
""", unsafe_allow_html=True)

with st.container():
    tr_col1, tr_col2, tr_col3, tr_col4 = st.columns([3, 2, 3, 2])
    with tr_col1:
        tr_ind_name = st.selectbox("Indikator Tren:", list(INDICATORS.keys()), index=0, key="tr_ind_sel")
    with tr_col2:
        tr_group_by = st.selectbox("Kelompokkan Berdasarkan:", ["Region", "Income Group"], index=0, key="tr_grp_sel")
    with tr_col3:
        tr_yr = st.slider("Rentang Tahun Tren:", YEAR_MIN, YEAR_MAX, (1990, 2022), key="tr_yr_slider")
    with tr_col4:
        if tr_group_by == "Region":
            tr_filter = st.multiselect("Pilih Region:", ALL_REGIONS, default=ALL_REGIONS, key="tr_rg_filter")
        else:
            tr_filter = st.multiselect("Pilih Income:", ALL_INCOME, default=ALL_INCOME, key="tr_inc_filter")

tr_col = INDICATORS[tr_ind_name]
grp_field = "region" if tr_group_by == "Region" else "income_group"

if tr_group_by == "Region":
    df_tr = df[df["year"].between(tr_yr[0], tr_yr[1]) & df["region"].isin(tr_filter if tr_filter else ALL_REGIONS)]
else:
    df_tr = df[df["year"].between(tr_yr[0], tr_yr[1]) & df["income_group"].isin(tr_filter if tr_filter else ALL_INCOME)]

tr_agg = df_tr.groupby(["year", grp_field])[tr_col].mean().reset_index()

cat_orders = {}
if grp_field == "income_group":
    cat_orders = {"income_group": ALL_INCOME}

fig_trend = px.line(
    tr_agg,
    x="year",
    y=tr_col,
    color=grp_field,
    markers=False,
    labels={"year": "Tahun", tr_col: tr_ind_name, grp_field: tr_group_by},
    color_discrete_sequence=px.colors.qualitative.Bold,
    category_orders=cat_orders,
)
fig_trend.update_traces(line_width=2.5)
fig_trend.update_layout(
    **get_plotly_layout(height=380, margin=dict(t=30, b=40, l=40, r=20)),
    legend=dict(orientation="h", y=-0.22, x=0),
)
st.plotly_chart(fig_trend, use_container_width=True)

st.markdown('<hr class="divider">', unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# GRAFIK 3 & 4: SCATTER BUBBLE & BOXPLOT (2 Kolom — MASING-MASING FILTER MANDIRI)
# ─────────────────────────────────────────────────────────────────────────────
col_g3, col_g4 = st.columns([1, 1], gap="large")

with col_g3:
    st.markdown("""
    <div class="chart-title">🔬 3. Hubungan Antar Indikator (Scatter Bubble)</div>
    <div class="chart-desc">Eksplorasi korelasi bivariat dengan garis tren regresi linier.</div>
    """, unsafe_allow_html=True)
    
    sc_f1, sc_f2 = st.columns(2)
    with sc_f1:
        sc_x_label = st.selectbox("Sumbu X:", list(INDICATORS.keys()), index=6, key="sc_x_sel")
        sc_sz_label = st.selectbox("Ukuran Bubble:", list(INDICATORS.keys()), index=5, key="sc_sz_sel")
    with sc_f2:
        sc_y_label = st.selectbox("Sumbu Y:", list(INDICATORS.keys()), index=0, key="sc_y_sel")
        sc_yr = st.slider("Rentang Tahun Scatter:", YEAR_MIN, YEAR_MAX, (2000, 2022), key="sc_yr_slider")
    
    sc_rg = st.multiselect("Region Scatter:", ALL_REGIONS, default=ALL_REGIONS, key="sc_rg_sel")
    
    sc_x = INDICATORS[sc_x_label]
    sc_y = INDICATORS[sc_y_label]
    sc_sz = INDICATORS[sc_sz_label]
    
    df_sc = df[
        df["year"].between(sc_yr[0], sc_yr[1]) &
        df["region"].isin(sc_rg if sc_rg else ALL_REGIONS)
    ][[sc_x, sc_y, sc_sz, "region", "income_group", "country_code"]].dropna()

    fig_sc = px.scatter(
        df_sc,
        x=sc_x,
        y=sc_y,
        color="region",
        size=sc_sz,
        size_max=22,
        hover_name="country_code",
        opacity=0.65,
        trendline="ols",
        labels={sc_x: sc_x_label, sc_y: sc_y_label, "region": "Region"},
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig_sc.update_layout(
        **get_plotly_layout(height=420, margin=dict(t=30, b=40, l=40, r=20)),
        legend=dict(orientation="h", y=-0.3, x=0),
    )
    st.plotly_chart(fig_sc, use_container_width=True)

with col_g4:
    st.markdown("""
    <div class="chart-title">📦 4. Distribusi Nilai per Region (Box Plot)</div>
    <div class="chart-desc">Lihat sebaran, median, kuartil, dan pencilan (outliers) per wilayah.</div>
    """, unsafe_allow_html=True)
    
    bx_f1, bx_f2 = st.columns(2)
    with bx_f1:
        bx_ind_label = st.selectbox("Pilih Indikator Boxplot:", list(INDICATORS.keys()), index=0, key="bx_ind_sel")
    with bx_f2:
        bx_yr = st.slider("Rentang Tahun Boxplot:", YEAR_MIN, YEAR_MAX, (2000, 2022), key="bx_yr_slider")
    
    bx_rg = st.multiselect("Pilih Region Boxplot:", ALL_REGIONS, default=ALL_REGIONS, key="bx_rg_sel")
    
    bx_col = INDICATORS[bx_ind_label]
    df_bx = df[
        df["year"].between(bx_yr[0], bx_yr[1]) &
        df["region"].isin(bx_rg if bx_rg else ALL_REGIONS)
    ][[bx_col, "region"]].dropna()

    fig_box = px.box(
        df_bx,
        x="region",
        y=bx_col,
        color="region",
        points="outliers",
        labels={bx_col: bx_ind_label, "region": "Region"},
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig_box.update_layout(
        **get_plotly_layout(height=420, margin=dict(t=30, b=60, l=40, r=20)),
        showlegend=False,
        xaxis_tickangle=-25,
    )
    st.plotly_chart(fig_box, use_container_width=True)

st.markdown('<hr class="divider">', unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# GRAFIK 5: MATRIKS KORELASI (HEATMAP) — FILTER PER GRAFIK
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="chart-title">🟦 5. Matriks Korelasi Antar Indikator Kesehatan</div>
<div class="chart-desc">Korelasi Pearson antar 10 indikator kesehatan numerik. Nilai +1 menunjukkan korelasi positif sempurna, -1 korelasi negatif kuat.</div>
""", unsafe_allow_html=True)

with st.container():
    hm_f1, hm_f2 = st.columns([3, 2])
    with hm_f1:
        hm_rg = st.multiselect("Filter Region Heatmap:", ALL_REGIONS, default=ALL_REGIONS, key="hm_rg_sel")
    with hm_f2:
        hm_yr = st.slider("Rentang Tahun Heatmap:", YEAR_MIN, YEAR_MAX, (2000, 2022), key="hm_yr_slider")

num_cols = list(INDICATORS.values())
col_short = [
    "Life Exp.", "CVD Deaths", "Diabetes", "Obesity", "Infant Mort.",
    "Physicians", "Health Exp.", "HIV", "BMI Male", "BMI Female"
]

df_hm = df[
    df["year"].between(hm_yr[0], hm_yr[1]) &
    df["region"].isin(hm_rg if hm_rg else ALL_REGIONS)
]
corr_matrix = df_hm[num_cols].dropna().corr().round(2)

fig_hm = px.imshow(
    corr_matrix.values,
    x=col_short,
    y=col_short,
    text_auto=True,
    color_continuous_scale="RdBu_r",
    zmin=-1,
    zmax=1,
    aspect="auto",
)
fig_hm.update_layout(
    **get_plotly_layout(height=450, margin=dict(t=30, b=40, l=60, r=20)),
    font_size=11,
    coloraxis_colorbar=dict(title="Korelasi", thickness=14, len=0.7),
)
st.plotly_chart(fig_hm, use_container_width=True)

st.markdown('<hr class="divider">', unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# GRAFIK 6, 7 & 8: RANKING NEGARA & VIOLIN PLOT (FILTER PER SECTION)
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="chart-title">🏆 6, 7 & 8. Peringkat Negara & Distribusi per Tingkat Pendapatan</div>
<div class="chart-desc">Peringkat negara tertinggi dan terendah beserta perbandingan distribusi per kelompok pendapatan Bank Dunia.</div>
""", unsafe_allow_html=True)

with st.container():
    rk_f1, rk_f2, rk_f3, rk_f4 = st.columns([3, 2, 2, 3])
    with rk_f1:
        rk_ind_label = st.selectbox("Indikator Peringkat:", list(INDICATORS.keys()), index=0, key="rk_ind_sel")
    with rk_f2:
        rk_yr = st.slider("Tahun Evaluasi:", YEAR_MIN, YEAR_MAX, 2019, key="rk_yr_slider")
    with rk_f3:
        rk_n = st.slider("Jumlah Negara (N):", 5, 20, 10, key="rk_n_slider")
    with rk_f4:
        rk_rg = st.multiselect("Filter Wilayah:", ALL_REGIONS, default=ALL_REGIONS, key="rk_rg_sel")

rk_col = INDICATORS[rk_ind_label]
df_rank = df[
    (df["year"] == rk_yr) &
    (df["region"].isin(rk_rg if rk_rg else ALL_REGIONS))
][["country_code", "region", rk_col, "income_group"]].dropna()

df_rank = df_rank.sort_values(rk_col, ascending=False)

col_r1, col_r2, col_r3 = st.columns([1, 1, 1.2], gap="medium")

with col_r1:
    top_n = df_rank.head(rk_n)
    fig_top = px.bar(
        top_n,
        x=rk_col,
        y="country_code",
        orientation="h",
        color=rk_col,
        color_continuous_scale="Blues",
        labels={rk_col: rk_ind_label, "country_code": "Negara"},
        title=f"Top {rk_n} Tertinggi ({rk_yr})",
    )
    fig_top.update_layout(
        **get_plotly_layout(height=390, margin=dict(t=40, b=30, l=40, r=20)),
        yaxis=dict(autorange="reversed"),
        coloraxis_showscale=False,
        title_font_color="#1e3a8a",
        title_font_size=13,
    )
    st.plotly_chart(fig_top, use_container_width=True)

with col_r2:
    bot_n = df_rank.tail(rk_n).sort_values(rk_col)
    fig_bot = px.bar(
        bot_n,
        x=rk_col,
        y="country_code",
        orientation="h",
        color=rk_col,
        color_continuous_scale="Reds_r",
        labels={rk_col: rk_ind_label, "country_code": "Negara"},
        title=f"Bottom {rk_n} Terendah ({rk_yr})",
    )
    fig_bot.update_layout(
        **get_plotly_layout(height=390, margin=dict(t=40, b=30, l=40, r=20)),
        yaxis=dict(autorange="reversed"),
        coloraxis_showscale=False,
        title_font_color="#1e3a8a",
        title_font_size=13,
    )
    st.plotly_chart(fig_bot, use_container_width=True)

with col_r3:
    df_vio = df[(df["year"] == rk_yr) & df["income_group"].isin(ALL_INCOME)].copy()
    df_vio["income_group"] = pd.Categorical(df_vio["income_group"], categories=ALL_INCOME, ordered=True)
    fig_vio = px.violin(
        df_vio,
        x="income_group",
        y=rk_col,
        color="income_group",
        box=True,
        points="outliers",
        labels={rk_col: rk_ind_label, "income_group": ""},
        color_discrete_sequence=["#ef4444", "#f97316", "#3b82f6", "#10b981"],
        category_orders={"income_group": ALL_INCOME},
        title=f"Distribusi per Income Group ({rk_yr})",
    )
    fig_vio.update_layout(
        **get_plotly_layout(height=390, margin=dict(t=40, b=60, l=40, r=20)),
        showlegend=False,
        xaxis_tickangle=-15,
        title_font_color="#1e3a8a",
        title_font_size=13,
    )
    st.plotly_chart(fig_vio, use_container_width=True)

st.markdown('<hr class="divider">', unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# GRAFIK 9 & 10: GENDER GAP & OBESITAS VS DIABETES (MASING-MASING FILTER MANDIRI)
# ─────────────────────────────────────────────────────────────────────────────
col_g9, col_g10 = st.columns([1, 1], gap="large")

with col_g9:
    st.markdown("""
    <div class="chart-title">⚖️ 9. Kesenjangan Harapan Hidup Gender</div>
    <div class="chart-desc">Selisih harapan hidup perempuan vs laki-laki (tahun) sepanjang waktu.</div>
    """, unsafe_allow_html=True)
    
    gg_f1, gg_f2 = st.columns(2)
    with gg_f1:
        gg_rg = st.multiselect("Pilih Region Gender Gap:", ALL_REGIONS, default=ALL_REGIONS, key="gg_rg_sel")
    with gg_f2:
        gg_yr = st.slider("Rentang Tahun Gender Gap:", YEAR_MIN, YEAR_MAX, (1990, 2022), key="gg_yr_slider")
        
    df_gap = df[
        df["year"].between(gg_yr[0], gg_yr[1]) &
        df["region"].isin(gg_rg if gg_rg else ALL_REGIONS)
    ].copy()
    df_gap["gender_gap"] = df_gap["life_expectancy_female"] - df_gap["life_expectancy_male"]
    gap_trend = df_gap.groupby(["year", "region"])["gender_gap"].mean().reset_index()

    fig_gap = px.line(
        gap_trend,
        x="year",
        y="gender_gap",
        color="region",
        markers=False,
        labels={"gender_gap": "Gap Harapan Hidup (thn)", "year": "Tahun", "region": "Region"},
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig_gap.add_hline(y=0, line_dash="dash", line_color="#ef4444", opacity=0.6, annotation_text="0 (Seimbang)")
    fig_gap.update_layout(
        **get_plotly_layout(height=380, margin=dict(t=30, b=40, l=40, r=20)),
        legend=dict(orientation="h", y=-0.3, x=0),
    )
    st.plotly_chart(fig_gap, use_container_width=True)

with col_g10:
    st.markdown("""
    <div class="chart-title">🍔 10. Prevalensi Obesitas vs Diabetes</div>
    <div class="chart-desc">Hubungan antara tingkat obesitas dan prevalensi diabetes dengan ukuran bubble = rata-rata BMI.</div>
    """, unsafe_allow_html=True)
    
    ob_f1, ob_f2 = st.columns(2)
    with ob_f1:
        ob_rg = st.multiselect("Pilih Region Obesitas:", ALL_REGIONS, default=ALL_REGIONS, key="ob_rg_sel")
    with ob_f2:
        ob_yr = st.slider("Rentang Tahun Obesitas:", YEAR_MIN, YEAR_MAX, (2000, 2022), key="ob_yr_slider")

    df_ob = df[
        df["year"].between(ob_yr[0], ob_yr[1]) &
        df["region"].isin(ob_rg if ob_rg else ALL_REGIONS)
    ][["obesity_prevalence_pct", "diabetes_prevalence_pct", "mean_bmi_male", "region", "country_code"]].dropna()

    fig_ob = px.scatter(
        df_ob,
        x="obesity_prevalence_pct",
        y="diabetes_prevalence_pct",
        color="region",
        size="mean_bmi_male",
        size_max=18,
        hover_name="country_code",
        opacity=0.65,
        trendline="ols",
        labels={
            "obesity_prevalence_pct": "Prevalensi Obesitas (%)",
            "diabetes_prevalence_pct": "Prevalensi Diabetes (%)",
            "mean_bmi_male": "BMI Laki-laki",
            "region": "Region",
        },
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig_ob.update_layout(
        **get_plotly_layout(height=380, margin=dict(t=30, b=40, l=40, r=20)),
        legend=dict(orientation="h", y=-0.3, x=0),
    )
    st.plotly_chart(fig_ob, use_container_width=True)

st.markdown('<hr class="divider">', unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# GRAFIK 11 & 12: TREN CVD & KEMATIAN BAYI VS HARAPAN HIDUP (FILTER MANDIRI)
# ─────────────────────────────────────────────────────────────────────────────
col_g11, col_g12 = st.columns([1, 1], gap="large")

with col_g11:
    st.markdown("""
    <div class="chart-title">💔 11. Tren Kematian Kardiovaskular (CVD) per Income Group</div>
    <div class="chart-desc">Perkembangan kematian akibat penyakit kardiovaskular per 100.000 populasi.</div>
    """, unsafe_allow_html=True)
    
    cv_f1, cv_f2 = st.columns(2)
    with cv_f1:
        cv_rg = st.multiselect("Pilih Region CVD:", ALL_REGIONS, default=ALL_REGIONS, key="cv_rg_sel")
    with cv_f2:
        cv_yr = st.slider("Rentang Tahun CVD:", YEAR_MIN, YEAR_MAX, (1990, 2022), key="cv_yr_slider")
        
    cvd_df = df[
        df["year"].between(cv_yr[0], cv_yr[1]) &
        df["region"].isin(cv_rg if cv_rg else ALL_REGIONS) &
        df["income_group"].isin(ALL_INCOME)
    ]
    cvd_trend = cvd_df.groupby(["year", "income_group"])["cvd_deaths_per_100k"].mean().reset_index()

    fig_cvd = px.line(
        cvd_trend,
        x="year",
        y="cvd_deaths_per_100k",
        color="income_group",
        markers=False,
        labels={"cvd_deaths_per_100k": "Kematian CVD / 100k", "year": "Tahun", "income_group": "Income Group"},
        color_discrete_sequence=["#ef4444", "#f97316", "#3b82f6", "#10b981"],
        category_orders={"income_group": ALL_INCOME},
    )
    fig_cvd.update_traces(line_width=2.5)
    fig_cvd.update_layout(
        **get_plotly_layout(height=380, margin=dict(t=30, b=40, l=40, r=20)),
        legend=dict(orientation="h", y=-0.3, x=0),
    )
    st.plotly_chart(fig_cvd, use_container_width=True)

with col_g12:
    st.markdown("""
    <div class="chart-title">👶 12. Hubungan Kematian Bayi vs Harapan Hidup</div>
    <div class="chart-desc">Kematian bayi per 1.000 kelahiran vs Harapan hidup total (ukuran bubble = kematian balita).</div>
    """, unsafe_allow_html=True)
    
    inf_f1, inf_f2 = st.columns(2)
    with inf_f1:
        inf_inc = st.multiselect("Pilih Income Group:", ALL_INCOME, default=ALL_INCOME, key="inf_inc_sel")
    with inf_f2:
        inf_yr = st.slider("Rentang Tahun Kematian Bayi:", YEAR_MIN, YEAR_MAX, (2000, 2022), key="inf_yr_slider")

    df_inf = df[
        df["year"].between(inf_yr[0], inf_yr[1]) &
        df["income_group"].isin(inf_inc if inf_inc else ALL_INCOME)
    ][["infant_mortality_per_1000", "life_expectancy_total", "under5_mortality_per_1000", "income_group", "country_code"]].dropna()

    fig_inf = px.scatter(
        df_inf,
        x="infant_mortality_per_1000",
        y="life_expectancy_total",
        color="income_group",
        size="under5_mortality_per_1000",
        size_max=18,
        hover_name="country_code",
        opacity=0.6,
        trendline="ols",
        labels={
            "infant_mortality_per_1000": "Kematian Bayi per 1.000",
            "life_expectancy_total": "Harapan Hidup (thn)",
            "under5_mortality_per_1000": "Kematian Balita",
            "income_group": "Income Group",
        },
        color_discrete_sequence=["#ef4444", "#f97316", "#3b82f6", "#10b981"],
        category_orders={"income_group": ALL_INCOME},
    )
    fig_inf.update_layout(
        **get_plotly_layout(height=380, margin=dict(t=30, b=40, l=40, r=20)),
        legend=dict(orientation="h", y=-0.3, x=0),
    )
    st.plotly_chart(fig_inf, use_container_width=True)

st.markdown('<hr class="divider">', unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# GRAFIK 13: RADAR CHART PROFIL RISIKO KESEHATAN PER REGION (FILTER MANDIRI)
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="chart-title">🕸️ 13. Radar Chart: Profil Risiko Kesehatan Antar Wilayah</div>
<div class="chart-desc">Perbandingan multivariat 6 dimensi risiko kesehatan (dinormalisasi 0–1 skala min-max) untuk melihat karakteristik komparatif antar kawasan dunia.</div>
""", unsafe_allow_html=True)

with st.container():
    rd_f1, rd_f2 = st.columns([3, 2])
    with rd_f1:
        radar_rg = st.multiselect("Pilih Region yang Dibandingkan:", ALL_REGIONS, default=ALL_REGIONS[:4], key="radar_rg_sel")
    with rd_f2:
        radar_yr = st.slider("Tahun Evaluasi Radar:", YEAR_MIN, YEAR_MAX, 2019, key="radar_yr_slider")

radar_metrics = {
    "Kematian CVD / 100k": "cvd_deaths_per_100k",
    "Diabetes (%)": "diabetes_prevalence_pct",
    "Obesitas (%)": "obesity_prevalence_pct",
    "Kematian Bayi / 1k": "infant_mortality_per_1000",
    "HIV (%)": "hiv_prevalence_pct",
    "Dokter / 1k": "physicians_per_1000",
}

active_regions = radar_rg if radar_rg else ALL_REGIONS
df_rad = df[
    (df["year"] == radar_yr) &
    (df["region"].isin(active_regions))
].groupby("region")[list(radar_metrics.values())].mean()

if len(df_rad) > 0:
    # Min-max scaling untuk radar polar
    df_rad_norm = (df_rad - df_rad.min()) / (df_rad.max() - df_rad.min() + 1e-9)
    categories = list(radar_metrics.keys())
    palette = px.colors.qualitative.Dark24

    fig_radar = go.Figure()
    for idx, reg_name in enumerate(df_rad_norm.index):
        vals = df_rad_norm.loc[reg_name].tolist()
        vals += [vals[0]]  # Tutup loop polar
        fig_radar.add_trace(go.Scatterpolar(
            r=vals,
            theta=categories + [categories[0]],
            fill="toself",
            name=reg_name,
            line_color=palette[idx % len(palette)],
            opacity=0.6,
        ))

    fig_radar.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 1], tickfont_size=9)),
        height=450,
        paper_bgcolor="white",
        margin=dict(t=30, b=40, l=40, r=40),
        legend=dict(orientation="h", y=-0.12, x=0.5, xanchor="center"),
        font=dict(family="Inter, sans-serif", size=11),
        hoverlabel=dict(
            bgcolor="#0f172a",
            bordercolor="#334155",
            font=dict(family="Inter, sans-serif", size=12, color="#ffffff"),
        ),
    )
    st.plotly_chart(fig_radar, use_container_width=True)
else:
    st.info("Pilih minimal satu region untuk menampilkan radar chart.")

# ─────────────────────────────────────────────────────────────────────────────
# FOOTER
# ─────────────────────────────────────────────────────────────────────────────
st.markdown('<hr class="divider">', unsafe_allow_html=True)
st.caption("📊 Global Health Machine Learning & Analytics Dashboard · Sumber Data: WHO Global Health Observatory & World Bank Development Indicators")
