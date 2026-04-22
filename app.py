import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px
import gc
from io import BytesIO
import pyarrow as pa
import pyarrow.parquet as pq

# 1. CONFIGURACIÓN DE PÁGINA
st.set_page_config(page_title="Monitor Red - Huawei", layout="wide")

# UMBRALES Y RUTAS
UMBRAL_CRITICO = 78 
UMBRAL_PREVENTIVO = 65
FOLDER_PATH = 'Temperatura'
PARQUET_FILE = 'base_historica.parquet'

# --- FUNCIONES DE EXTRACCIÓN ---
def extraer_datos_masivo(path):
    rows = []
    try:
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            content = f.read()
            ts_match = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', content)
            if not ts_match: return []
            ts = pd.to_datetime(f"{ts_match.group(1)} {ts_match.group(2)}")
            
            bloques = re.split(r'NE Name\s*:\s*', content)
            for bloque in bloques[1:]:
                lineas = bloque.split('\n')
                if not lineas: continue
                nombre_sitio = lineas[0].strip().split()[0]
                
                filas = re.findall(r'^\s*\d+\s+(\d+)\s+(\d+)\s+(\d+)', bloque, re.MULTILINE)
                for r in filas:
                    rows.append({
                        "Timestamp": ts, "Sitio": nombre_sitio, "Slot": int(r[1]),
                        "Temp": int(r[2]), "ID_Full": f"{nombre_sitio} (S:{r[0]}-L:{r[1]})"
                    })
    except: pass
    return rows

@st.cache_data(ttl=60)
def listar_archivos(folder):
    if not os.path.exists(folder): 
        os.makedirs(folder, exist_ok=True)
        return []
    fs = [os.path.join(folder, f) for f in os.listdir(folder) if ".txt" in f]
    fs.sort(key=lambda x: "".join(re.findall(r'\d+', x)), reverse=True)
    return fs

# Función de seguridad para validar el archivo Parquet
def es_parquet_valido(file_path):
    if os.path.exists(file_path) and os.path.getsize(file_path) > 100:
        try:
            pq.read_metadata(file_path)
            return True
        except: return False
    return False

# --- PROCESAMIENTO INICIAL ---
archivos_lista = listar_archivos(FOLDER_PATH)

if archivos_lista:
    if "df_now" not in st.session_state:
        data_now = extraer_datos_masivo(archivos_lista[0])
        st.session_state["df_now"] = pd.DataFrame(data_now) if data_now else pd.DataFrame()
    
    df_actual = st.session_state["df_now"]

    tab_dash, tab_hist, tab_upgrade = st.tabs(["📊 DASHBOARD", "📈 HISTÓRICO", "🚀 ANÁLISIS UPGRADE"])

    with tab_dash:
        if not df_actual.empty:
            st.title("📊 Monitor de Salud de Red")
            m1, m2 = st.columns(2)
            m1.metric("Total Tarjetas", len(df_actual))
            m2.metric("Críticos", len(df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]))
            st.dataframe(df_actual, use_container_width=True)
        else:
            st.error("No se pudieron leer datos del último archivo .txt")

    with tab_hist:
        st.subheader("📈 Gestión Histórica")
        c1, c2 = st.columns([1, 2])
        with c1:
            num = st.slider("Archivos:", 1, len(archivos_lista), min(100, len(archivos_lista)))
            if st.button("🔥 Reconstruir Base Parquet"):
                if os.path.exists(PARQUET_FILE): os.remove(PARQUET_FILE)
                writer = None
                p_bar = st.progress(0)
                try:
                    for i, p in enumerate(archivos_lista[:num]):
                        data = extraer_datos_masivo(p)
                        if data:
                            df_tmp = pd.DataFrame(data)
                            table = pa.Table.from_pandas(df_tmp)
                            if writer is None: writer = pq.ParquetWriter(PARQUET_FILE, table.schema, compression='snappy')
                            writer.write_table(table)
                        if i % 20 == 0: # Limpieza de memoria cada 20 archivos
                            p_bar.progress((i + 1) / num)
                            gc.collect()
                    if writer: writer.close()
                    st.success("✅ Base generada.")
                    st.rerun()
                except Exception as e: st.error(f"Error: {e}")
        
        with c2:
            if es_parquet_valido(PARQUET_FILE):
                df_h = pd.read_parquet(PARQUET_FILE)
                s_sel = st.selectbox("🔍 Ver Historial de:", sorted(df_h['Sitio'].unique()))
                fig = px.line(df_h[df_h['Sitio'] == s_sel], x='Timestamp', y='Temp', color='ID_Full')
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Presiona el botón para generar el archivo histórico.")

    with tab_upgrade:
        st.header("🚀 Análisis de Upgrade")
        if es_parquet_valido(PARQUET_FILE):
            df_full = pd.read_parquet(PARQUET_FILE)
            f_up = st.file_uploader("Subir lista de sitios:", type=['xlsx', 'csv'])
            if f_up:
                df_l = pd.read_csv(f_up) if f_up.name.endswith('.csv') else pd.read_excel(f_up)
                nodos = df_l['Sitio'].astype(str).str.strip().tolist()
                res = df_full[df_full['Sitio'].isin(nodos)].groupby(['Timestamp', 'Sitio'])['Temp'].max().reset_index()
                st.plotly_chart(px.line(res, x='Timestamp', y='Temp', color='Sitio'), use_container_width=True)
        else:
            st.warning("Primero debes generar la base en la pestaña HISTÓRICO.")
else:
    st.warning("No hay archivos en la carpeta Temperatura.")
