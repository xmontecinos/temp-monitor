import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px
import gc
import pyarrow as pa
import pyarrow.parquet as pq

# 1. CONFIGURACIÓN DE PÁGINA
st.set_page_config(page_title="Monitor Red Huawei - Pro", layout="wide")

UMBRAL_CRITICO = 78 
UMBRAL_PREVENTIVO = 65
FOLDER_PATH = 'Temperatura'
PARQUET_FILE = 'base_historica.parquet'

# --- MOTOR DE EXTRACCIÓN (OPTIMIZADO) ---
def extraer_datos_unidad(path):
    rows = []
    try:
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            content = f.read()
            ts_match = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', content)
            if not ts_match: return None
            ts = pd.to_datetime(f"{ts_match.group(1)} {ts_match.group(2)}")
            
            bloques = re.split(r'NE Name\s*:\s*', content)
            for bloque in bloques[1:]:
                lineas = bloque.split('\n')
                if not lineas: continue
                # Requerimiento: NEName debe ir junto en la lógica interna
                sitio = lineas[0].strip().split()[0]
                filas = re.findall(r'^\s*\d+\s+(\d+)\s+(\d+)\s+(\d+)', bloque, re.MULTILINE)
                for r in filas:
                    rows.append({
                        "Timestamp": ts, "Sitio": sitio, "Slot": int(r[1]),
                        "Temp": int(r[2]), "ID_Full": f"{sitio} (S:{r[0]}-L:{r[1]})"
                    })
    except: return None
    return pd.DataFrame(rows)

@st.cache_data(ttl=60)
def listar_archivos(folder):
    if not os.path.exists(folder): 
        os.makedirs(folder, exist_ok=True)
        return []
    fs = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".txt")]
    fs.sort(key=lambda x: "".join(re.findall(r'\d+', x)), reverse=True)
    return fs

# --- LÓGICA DE NAVEGACIÓN ---
archivos_lista = listar_archivos(FOLDER_PATH)

if archivos_lista:
    # Carga rápida del reporte más reciente para el Dashboard
    if "df_now" not in st.session_state:
        st.session_state["df_now"] = extraer_datos_unidad(archivos_lista[0])
    
    df_actual = st.session_state["df_now"]
    tab_dash, tab_busq, tab_hist, tab_upgrade = st.tabs([
        "📊 DASHBOARD", "🔍 BUSCADOR", "📈 HISTÓRICO MASIVO", "🚀 UPGRADE"
    ])

    # --- DASHBOARD ---
    with tab_dash:
        if df_actual is not None:
            st.title("📊 Estado de Red Actual")
            st.info(f"🕒 Reporte: {df_actual['Timestamp'].max().strftime('%d/%m/%Y %H:%M')}")
            t_crit = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
            m1, m2 = st.columns(2)
            m1.metric("Total Tarjetas", f"{len(df_actual):,}")
            m2.metric("Críticos", len(t_crit), delta_color="inverse")
            if not t_crit.empty:
                st.dataframe(t_crit.sort_values('Temp', ascending=False), use_container_width=True)

    # --- HISTÓRICO (SOLUCIÓN A LOS +300 ARCHIVOS) ---
    with tab_hist:
        st.subheader("📈 Procesamiento de Datos Masivos")
        col_btn, col_graf = st.columns([1, 2])
        with col_btn:
            n_archivos = st.number_input("Archivos a procesar:", 1, len(archivos_lista), len(archivos_lista))
            if st.button("🔥 RECONSTRUIR BASE (SANEAMIENTO)"):
                if os.path.exists(PARQUET_FILE): os.remove(PARQUET_FILE)
                writer = None
                prog_bar = st.progress(0)
                for i, p in enumerate(archivos_lista[:n_archivos]):
                    df_tmp = extraer_datos_unidad(p)
                    if df_tmp is not None and not df_tmp.empty:
                        table = pa.Table.from_pandas(df_tmp)
                        if writer is None:
                            writer = pq.ParquetWriter(PARQUET_FILE, table.schema, compression='snappy')
                        writer.write_table(table)
                    if i % 25 == 0:
                        prog_bar.progress((i+1)/n_archivos)
                        gc.collect() # Limpia la RAM para evitar el "Oh no"
                if writer: writer.close()
                st.success("✅ Base reconstruida."); st.rerun()

        with col_graf:
            if os.path.exists(PARQUET_FILE) and os.path.getsize(PARQUET_FILE) > 0:
                try:
                    df_h = pd.read_parquet(PARQUET_FILE)
                    sitio_h = st.selectbox("Seleccionar Nodo:", sorted(df_h['Sitio'].unique()))
                    st.plotly_chart(px.line(df_h[df_h['Sitio'] == sitio_h], x='Timestamp', y='Temp', color='ID_Full'), use_container_width=True)
                except: st.error("Archivo corrupto. Presiona el botón de la izquierda.")

    # --- ANÁLISIS UPGRADE ---
    with tab_upgrade:
        st.header("🚀 Análisis de Upgrade")
        if os.path.exists(PARQUET_FILE):
            try:
                df_full = pd.read_parquet(PARQUET_FILE)
                tiempos = sorted(df_full['Timestamp'].unique(), reverse=True)
                cu1, cu2 = st.columns(2)
                with cu1: f_up = st.file_uploader("Lista sitios (.xlsx/.csv):", type=['xlsx', 'csv'])
                with cu2:
                    ref_sel = st.selectbox("🎯 Punto Referencia:", tiempos, format_func=lambda x: x.strftime('%Y-%m-%d %H:%M'))
                    ref_ts = pd.Timestamp(ref_sel)
                
                if f_up:
                    df_l = pd.read_csv(f_up) if f_up.name.endswith('.csv') else pd.read_excel(f_up)
                    sitios_up = df_l['Sitio'].astype(str).str.strip().tolist()
                    res_up = df_full[df_full['Sitio'].isin(sitios_up)].groupby(['Timestamp', 'Sitio'])['Temp'].max().reset_index()
                    fig_u = px.line(res_up, x='Timestamp', y='Temp', color='Sitio')
                    # Corrección del TypeError multiplicando por 1000 para Plotly
                    fig_u.add_vline(x=ref_ts.timestamp() * 1000, line_dash="dash", line_color="orange")
                    st.plotly_chart(fig_u, use_container_width=True)
            except: st.warning("Carga la base de datos en la pestaña Histórico primero.")

    with tab_busq:
        if df_actual is not None:
            sb = st.selectbox("Buscador de Sitio:", sorted(df_actual['Sitio'].unique()))
            st.dataframe(df_actual[df_actual['Sitio'] == sb], use_container_width=True)
else:
    st.warning("No hay archivos .txt en la carpeta 'Temperatura'.")
