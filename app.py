import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px
import gc
from io import BytesIO
import pyarrow as pa
import pyarrow.parquet as pq

# 1. CONFIGURACIÓN INICIAL
st.set_page_config(page_title="Monitor Huawei Network", layout="wide")

# Configuración de rutas
FOLDER_PATH = 'Temperatura'
PARQUET_FILE = 'base_v4_pro.parquet'
UMBRAL_CRITICO = 78

# --- FUNCIONES CORE ---
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
                # Requerimiento: NEName junto
                sitio = lineas[0].strip().split()[0]
                filas = re.findall(r'^\s*\d+\s+(\d+)\s+(\d+)\s+(\d+)', bloque, re.MULTILINE)
                for r in filas:
                    rows.append({
                        "Timestamp": ts, "Sitio": sitio, "Slot": int(r[1]),
                        "Temp": int(r[2]), "ID_Full": f"{sitio} (S:{r[0]}-L:{r[1]})"
                    })
    except: return None
    return pd.DataFrame(rows)

def preparar_excel(df):
    output = BytesIO()
    # Usamos openpyxl explícitamente para evitar ModuleNotFoundError
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

# Asegurar directorios
if not os.path.exists(FOLDER_PATH):
    os.makedirs(FOLDER_PATH, exist_ok=True)

# Lista de archivos .txt
archivos_lista = [os.path.join(FOLDER_PATH, f) for f in os.listdir(FOLDER_PATH) if f.endswith(".txt")]
archivos_lista.sort(reverse=True)

# --- INTERFAZ ---
st.title("🌡️ Monitor Térmico de Red")

tab_dash, tab_hist, tab_up = st.tabs(["📊 DASHBOARD", "📈 HISTÓRICO MASIVO", "🚀 UPGRADE"])

with tab_dash:
    if archivos_lista:
        df_now = extraer_datos_unidad(archivos_lista[0])
        if df_now is not None:
            t_crit = df_now[df_now['Temp'] >= UMBRAL_CRITICO]
            c1, c2 = st.columns(2)
            c1.metric("Total Tarjetas", len(df_now))
            c2.metric("Nodos Críticos", len(t_crit))
            
            if not t_crit.empty:
                st.subheader("🚨 Detalle de Alertas")
                st.dataframe(t_crit, use_container_width=True)
                st.download_button("📥 Descargar Críticos (Excel)", preparar_excel(t_crit), "criticos.xlsx")
    else:
        st.warning("No se encontraron archivos .txt en la carpeta 'Temperatura'.")

with tab_hist:
    st.header("Gestión de Datos Históricos")
    col_a, col_b = st.columns([1, 2])
    
    with col_a:
        n = st.number_input("Archivos a procesar:", 1, len(archivos_lista) if archivos_lista else 1, len(archivos_lista) if archivos_lista else 1)
        if st.button("🔥 GENERAR BASE DE DATOS"):
            if os.path.exists(PARQUET_FILE):
                os.remove(PARQUET_FILE)
            
            writer = None
            bar = st.progress(0)
            status = st.empty()
            
            try:
                for i, path in enumerate(archivos_lista[:n]):
                    status.text(f"Procesando: {os.path.basename(path)}")
                    df_tmp = extraer_datos_unidad(path)
                    if df_tmp is not None and not df_tmp.empty:
                        table = pa.Table.from_pandas(df_tmp)
                        if writer is None:
                            writer = pq.ParquetWriter(PARQUET_FILE, table.schema, compression='snappy')
                        writer.write_table(table)
                    
                    if i % 20 == 0:
                        bar.progress((i+1)/n)
                        gc.collect()
                
                if writer:
                    writer.close()
                    st.success("✅ Base generada correctamente.")
                    st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")

    with col_b:
        if os.path.exists(PARQUET_FILE) and os.path.getsize(PARQUET_FILE) > 100:
            df_h = pd.read_parquet(PARQUET_FILE)
            sitio_sel = st.selectbox("Seleccione Sitio:", sorted(df_h['Sitio'].unique()))
            fig = px.line(df_h[df_h['Sitio'] == sitio_sel], x='Timestamp', y='Temp', color='ID_Full')
            fig.add_hline(y=UMBRAL_CRITICO, line_dash="dash", line_color="red")
            st.plotly_chart(fig, use_container_width=True)

with tab_up:
    st.subheader("Análisis de Upgrade")
    f_up = st.file_uploader("Cargar lista de sitios (xlsx/csv):", type=['xlsx', 'csv'])
    if f_up and os.path.exists(PARQUET_FILE):
        df_full = pd.read_parquet(PARQUET_FILE)
        df_l = pd.read_csv(f_up) if f_up.name.endswith('.csv') else pd.read_excel(f_up)
        sitios = df_l['Sitio'].astype(str).str.strip().tolist()
        res = df_full[df_full['Sitio'].isin(sitios)].groupby(['Timestamp', 'Sitio'])['Temp'].max().reset_index()
        fig_u = px.line(res, x='Timestamp', y='Temp', color='Sitio')
        st.plotly_chart(fig_u, use_container_width=True)