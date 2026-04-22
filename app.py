import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px
import gc
from io import BytesIO
import pyarrow as pa
import pyarrow.parquet as pq

# 1. CONFIGURACIÓN
st.set_page_config(page_title="Monitor Red Huawei - Estable", layout="wide")

UMBRAL_CRITICO = 78 
UMBRAL_PREVENTIVO = 65
FOLDER_PATH = 'Temperatura'
PARQUET_FILE = 'base_historica.parquet'

# --- MOTOR DE EXTRACCIÓN MEJORADO ---
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
                # NEName junto sin espacios
                sitio = lineas[0].strip().split()[0]
                filas = re.findall(r'^\s*\d+\s+(\d+)\s+(\d+)\s+(\d+)', bloque, re.MULTILINE)
                for r in filas:
                    rows.append({
                        "Timestamp": ts, "Sitio": sitio, "Slot": int(r[1]),
                        "Temp": int(r[2]), "ID_Full": f"{sitio} (S:{r[0]}-L:{r[1]})"
                    })
    except: return None
    return pd.DataFrame(rows)

# Función de descarga corregida (usa el motor predeterminado de pandas)
def preparar_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

@st.cache_data(ttl=60)
def listar_archivos(folder):
    if not os.path.exists(folder): 
        os.makedirs(folder, exist_ok=True)
        return []
    fs = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".txt")]
    fs.sort(key=lambda x: "".join(re.findall(r'\d+', x)), reverse=True)
    return fs

# --- INICIO APP ---
archivos_lista = listar_archivos(FOLDER_PATH)

if archivos_lista:
    if "df_now" not in st.session_state:
        st.session_state["df_now"] = extraer_datos_unidad(archivos_lista[0])
    
    df_actual = st.session_state["df_now"]
    t1, t2, t3, t4 = st.tabs(["📊 DASHBOARD", "🔍 BUSCADOR", "📈 HISTÓRICO", "🚀 UPGRADE"])

    with t1:
        if df_actual is not None:
            st.title("📊 Estado de Red Actual")
            st.info(f"🕒 Último Reporte: {df_actual['Timestamp'].max()}")
            
            t_crit = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
            m1, m2 = st.columns(2)
            m1.metric("Tarjetas Totales", len(df_actual))
            m2.metric("Alertas Críticas", len(t_crit))
            
            if not t_crit.empty:
                st.subheader("⚠️ Detalle de Críticos")
                st.dataframe(t_crit, use_container_width=True)
                # Botón de descarga corregido para evitar ModuleNotFoundError
                excel_data = preparar_excel(t_crit)
                st.download_button("📥 Descargar Críticos (Excel)", excel_data, "criticos.xlsx")

    with t3:
        st.subheader("📈 Reconstrucción de Histórico")
        c_btn, c_fig = st.columns([1, 2])
        
        with c_btn:
            n = st.number_input("Cantidad de archivos:", 1, len(archivos_lista), len(archivos_lista))
            if st.button("🔥 GENERAR BASE DE DATOS"):
                if os.path.exists(PARQUET_FILE): os.remove(PARQUET_FILE)
                
                writer = None
                p_bar = st.progress(0)
                for i, path in enumerate(archivos_lista[:n]):
                    df_tmp = extraer_datos_unidad(path)
                    if df_tmp is not None:
                        # Forzamos tipos de datos pequeños para evitar falta de memoria
                        df_tmp['Slot'] = df_tmp['Slot'].astype('int16')
                        df_tmp['Temp'] = df_tmp['Temp'].astype('int16')
                        table = pa.Table.from_pandas(df_tmp)
                        if writer is None:
                            writer = pq.ParquetWriter(PARQUET_FILE, table.schema, compression='snappy')
                        writer.write_table(table)
                    
                    if i % 20 == 0: 
                        p_bar.progress((i+1)/n)
                        gc.collect()
                if writer: writer.close()
                st.success("Base de datos creada."); st.rerun()

        with c_fig:
            if os.path.exists(PARQUET_FILE) and os.path.getsize(PARQUET_FILE) > 0:
                try:
                    df_h = pd.read_parquet(PARQUET_FILE)
                    nodo = st.selectbox("Sitio:", sorted(df_h['Sitio'].unique()))
                    fig = px.line(df_h[df_h['Sitio'] == nodo], x='Timestamp', y='Temp', color='ID_Full')
                    st.plotly_chart(fig, use_container_width=True)
                except: st.error("Archivo corrupto. Reconstruye la base.")

    with t4:
        st.header("🚀 Análisis de Upgrade")
        if os.path.exists(PARQUET_FILE):
            df_full = pd.read_parquet(PARQUET_FILE)
            tiempos = sorted(df_full['Timestamp'].unique(), reverse=True)
            cu1, cu2 = st.columns(2)
            with cu1: f_up = st.file_uploader("Lista sitios:", type=['xlsx', 'csv'])
            with cu2:
                ref_sel = st.selectbox("🎯 Referencia:", tiempos, format_func=lambda x: x.strftime('%Y-%m-%d %H:%M'))
            
            if f_up:
                df_l = pd.read_csv(f_up) if f_up.name.endswith('.csv') else pd.read_excel(f_up)
                sitios_up = df_l['Sitio'].astype(str).str.strip().tolist()
                res_up = df_full[df_full['Sitio'].isin(sitios_up)].groupby(['Timestamp', 'Sitio'])['Temp'].max().reset_index()
                fig_up = px.line(res_up, x='Timestamp', y='Temp', color='Sitio')
                # SOLUCIÓN AL TYPEERROR: Convertimos la referencia a milisegundos para Plotly
                ref_ts = pd.Timestamp(ref_sel).timestamp() * 1000
                fig_up.add_vline(x=ref_ts, line_dash="dash", line_color="orange")
                st.plotly_chart(fig_up, use_container_width=True)
