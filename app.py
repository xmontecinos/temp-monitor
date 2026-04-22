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

# CONFIGURACIÓN DE RUTAS Y UMBRALES
FOLDER_PATH = 'Temperatura'
PARQUET_FILE = 'base_historica.parquet' # Nombre unificado
UMBRAL_CRITICO = 78 
UMBRAL_PREVENTIVO = 65

# --- MOTOR DE EXTRACCIÓN (NEName junto, Board Type con espacio) ---
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
                nombre_sitio = lineas[0].strip().split()[0] # NEName junto
                
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
    if not os.path.exists(folder): os.makedirs(folder, exist_ok=True)
    fs = [os.path.join(folder, f) for f in os.listdir(folder) if ".txt" in f]
    fs.sort(key=lambda x: "".join(re.findall(r'\d+', x)), reverse=True)
    return fs

# --- PROCESAMIENTO ---
archivos_lista = listar_archivos(FOLDER_PATH)

if archivos_lista:
    if "df_now" not in st.session_state:
        st.session_state["df_now"] = pd.DataFrame(extraer_datos_masivo(archivos_lista[0]))
    
    df_actual = st.session_state["df_now"]
    tab_dash, tab_alertas, tab_busq, tab_hist, tab_upgrade = st.tabs([
        "📊 DASHBOARD", "🚨 ALERTAS ACTUALES", "🔍 BUSCADOR", "📈 HISTÓRICO", "🚀 ANÁLISIS UPGRADE"
    ])

    with tab_dash:
        if not df_actual.empty:
            st.title("📊 Monitor de Salud de Red")
            t_crit = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
            m1, m2 = st.columns(2)
            m1.metric("Total Tarjetas", f"{len(df_actual):,}")
            m2.metric("Críticos (≥78°C)", len(t_crit))
            if not t_crit.empty:
                st.dataframe(t_crit.sort_values('Temp', ascending=False), use_container_width=True)

    with tab_hist:
        st.subheader("📈 Gestión Histórica")
        c1, c2 = st.columns([1, 2])
        with c1:
            num = st.slider("Archivos:", 1, len(archivos_lista), min(150, len(archivos_lista)))
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
                        if i % 10 == 0:
                            p_bar.progress((i + 1) / num)
                            gc.collect() # Limpieza de RAM clave
                    st.success("✅ Base generada.")
                    st.rerun()
                except Exception as e: st.error(f"Error: {e}")
                finally: 
                    if writer: writer.close()
        with c2:
            if os.path.exists(PARQUET_FILE) and os.path.getsize(PARQUET_FILE) > 100:
                df_h = pd.read_parquet(PARQUET_FILE)
                s_sel = st.selectbox("🔍 Ver Historial de:", sorted(df_h['Sitio'].unique()))
                fig = px.line(df_h[df_h['Sitio'] == s_sel], x='Timestamp', y='Temp', color='ID_Full')
                st.plotly_chart(fig, use_container_width=True)

    with tab_upgrade:
        st.header("🚀 Análisis de Upgrade")
        if os.path.exists(PARQUET_FILE):
            # Lógica de comparación de sitios 93 (implementada en tu app.py)
            st.info("Utiliza esta pestaña para comparar el 'Antes' y 'Después' del upgrade térmico.")
else:
    st.warning("No hay archivos .txt en la carpeta 'Temperatura'.")
