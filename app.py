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
st.set_page_config(page_title="Monitor Huawei - Estable", layout="wide")

# Rutas y archivos
FOLDER_PATH = 'Temperatura'
PARQUET_FILE = 'base_v4_pro.parquet'
UMBRAL_CRITICO = 78

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

# Listar archivos
if not os.path.exists(FOLDER_PATH): os.makedirs(FOLDER_PATH, exist_ok=True)
archivos_lista = [os.path.join(FOLDER_PATH, f) for f in os.listdir(FOLDER_PATH) if f.endswith(".txt")]
archivos_lista.sort(key=lambda x: "".join(re.findall(r'\d+', x)), reverse=True)

# --- INTERFAZ ---
tab1, tab2 = st.tabs(["🚀 GENERAR BASE", "📊 TENDENCIAS"])

with tab1:
    st.header("Generador de Histórico")
    n = st.number_input("Cantidad de archivos a procesar:", 1, len(archivos_lista), len(archivos_lista))
    
    if st.button("🔥 INICIAR PROCESAMIENTO"):
        # PASO CRÍTICO: Borrar archivo viejo si existe para evitar conflictos de "Magic Bytes"
        if os.path.exists(PARQUET_FILE):
            try:
                os.remove(PARQUET_FILE)
                st.write("🗑️ Archivo anterior eliminado.")
            except: pass
            
        writer = None
        p_bar = st.progress(0)
        status = st.empty()
        
        try:
            for i, path in enumerate(archivos_lista[:n]):
                status.text(f"Procesando {i+1}/{n}: {os.path.basename(path)}")
                df_tmp = extraer_datos_unidad(path)
                
                if df_tmp is not None and not df_tmp.empty:
                    # Optimización de memoria: convertir tipos
                    df_tmp['Slot'] = df_tmp['Slot'].astype('int16')
                    df_tmp['Temp'] = df_tmp['Temp'].astype('int16')
                    
                    table = pa.Table.from_pandas(df_tmp)
                    if writer is None:
                        # Creamos el archivo Parquet físicamente en el disco
                        writer = pq.ParquetWriter(PARQUET_FILE, table.schema, compression='snappy')
                    writer.write_table(table)
                
                # Cada 20 archivos liberamos la memoria RAM manualmente
                if i % 20 == 0:
                    p_bar.progress((i+1)/n)
                    gc.collect()
            
            if writer:
                writer.close()
                st.success(f"✅ ¡Éxito! Se creó '{PARQUET_FILE}'.")
                st.info(f"Tamaño: {os.path.getsize(PARQUET_FILE) / 1024:.2f} KB")
                st.rerun()
            else:
                st.error("No se pudieron extraer datos de los archivos.")
        except Exception as e:
            st.error(f"🚨 Error en el servidor: {e}")

with tab2:
    if os.path.exists(PARQUET_FILE) and os.path.getsize(PARQUET_FILE) > 100:
        df_h = pd.read_parquet(PARQUET_FILE)
        st.write(f"Datos cargados: {len(df_h)} registros.")
        nodo = st.selectbox("Sitio:", sorted(df_h['Sitio'].unique()))
        fig = px.line(df_h[df_h['Sitio'] == nodo], x='Timestamp', y='Temp', color='ID_Full')
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("⚠️ La base de datos no existe. Ve a la pestaña 'GENERAR BASE'.")