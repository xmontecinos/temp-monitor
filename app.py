import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px
import gc
import pyarrow as pa
import pyarrow.parquet as pq

# 1. CONFIGURACIÓN
st.set_page_config(page_title="Monitor Red Huawei - Masivo", layout="wide")

UMBRAL_CRITICO = 78 
FOLDER_PATH = 'Temperatura'
PARQUET_FILE = 'base_historica.parquet'

def extraer_datos_unidad(path):
    """Procesa un solo archivo de forma aislada para ahorrar RAM"""
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
                # Requerimiento: NEName debe ir junto
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

archivos_lista = listar_archivos(FOLDER_PATH)

if archivos_lista:
    # Cargar solo el último para el Dashboard rápido
    if "df_now" not in st.session_state:
        st.session_state["df_now"] = extraer_datos_unidad(archivos_lista[0])
    
    df_actual = st.session_state["df_now"]
    t1, t2, t3, t4 = st.tabs(["📊 DASHBOARD", "🔍 BUSCADOR", "📈 HISTÓRICO", "🚀 UPGRADE"])

    with t1:
        if df_actual is not None:
            st.title("📊 Estado Actual")
            st.info(f"🕒 Reporte: {df_actual['Timestamp'].max()}")
            t_crit = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
            if not t_crit.empty:
                st.error(f"Se detectaron {len(t_crit)} tarjetas críticas")
                st.dataframe(t_crit.sort_values('Temp', ascending=False), use_container_width=True)

    with t3: # PESTAÑA HISTÓRICO: El motor del cambio
        st.subheader("📈 Procesamiento Masivo (+300 archivos)")
        c1, c2 = st.columns([1, 2])
        with c1:
            num = st.number_input("Cantidad de archivos a procesar:", 1, len(archivos_lista), len(archivos_lista))
            if st.button("🔥 Reconstruir Base Masiva"):
                # PASO CRÍTICO: Borrar archivo dañado antes de empezar
                if os.path.exists(PARQUET_FILE): os.remove(PARQUET_FILE)
                
                writer = None
                p_bar = st.progress(0)
                status = st.empty()
                
                for i, p in enumerate(archivos_lista[:num]):
                    df_temp = extraer_datos_unidad(p)
                    if df_temp is not None and not df_temp.empty:
                        table = pa.Table.from_pandas(df_temp)
                        if writer is None:
                            writer = pq.ParquetWriter(PARQUET_FILE, table.schema, compression='snappy')
                        writer.write_table(table)
                    
                    # Limpieza cada 20 archivos para no saturar RAM
                    if i % 20 == 0:
                        p_bar.progress((i+1)/num)
                        status.text(f"Procesando: {i+1} de {num}")
                        gc.collect() 
                
                if writer: writer.close()
                st.success("✅ Base reconstruida exitosamente."); st.rerun()
        
        with c2:
            if os.path.exists(PARQUET_FILE):
                try:
                    df_h = pd.read_parquet(PARQUET_FILE)
                    sh = st.selectbox("Sitio Histórico:", sorted(df_h['Sitio'].unique()))
                    df_v = df_h[df_h['Sitio'] == sh]
                    st.plotly_chart(px.line(df_v, x='Timestamp', y='Temp', color='ID_Full'), use_container_width=True)
                except:
                    st.error("Archivo corrupto detectado. Por favor, pulsa 'Reconstruir Base Masiva'.")

    with t4: # UPGRADE
        st.header("🚀 Análisis de Upgrade")
        if os.path.exists(PARQUET_FILE):
            df_full = pd.read_parquet(PARQUET_FILE)
            tiempos = sorted(df_full['Timestamp'].unique(), reverse=True)
            ref_sel = st.selectbox("🎯 Punto Referencia:", tiempos, format_func=lambda x: x.strftime('%Y-%m-%d %H:%M'))
            
            f_up = st.file_uploader("Subir lista (.csv preferible):", type=['csv', 'xlsx'])
            if f_up:
                try:
                    df_l = pd.read_csv(f_up) if f_up.name.endswith('.csv') else pd.read_excel(f_up)
                    sitios_up = df_l['Sitio'].astype(str).str.strip().tolist()
                    res_up = df_full[df_full['Sitio'].isin(sitios_up)].groupby(['Timestamp', 'Sitio'])['Temp'].max().reset_index()
                    fig = px.line(res_up, x='Timestamp', y='Temp', color='Sitio')
                    # Solución al error TypeError de la imagen 5a0afb
                    fig.add_vline(x=pd.to_datetime(ref_sel).timestamp() * 1000, line_dash="dash", line_color="orange")
                    st.plotly_chart(fig, use_container_width=True)
                except Exception as e: st.error(f"Error: {e}")

else:
    st.warning("Carpeta 'Temperatura' vacía.")
