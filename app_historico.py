import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="Histórico Global por Slot", layout="wide")

FOLDER_PATH = 'Temperatura'
PARQUET_FILE = 'red_historico_slots.parquet' # Archivo compartido

def extraer_datos_red(path):
    rows = []
    try:
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            content = f.read()
            ts_match = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', content)
            if not ts_match: return []
            ts = pd.to_datetime(f"{ts_match.group(1)} {ts_match.group(2)}")
            
            # Captura de Slot y Temperatura en todo el archivo
            filas = re.findall(r'^\s*(\d+)\s+(\d+)\s+(\d+)', content, re.MULTILINE)
            for r in filas:
                rows.append({
                    "Timestamp": ts, 
                    "Slot_Num": int(r[1]),
                    "Temp": int(r[2])
                })
    except: pass
    return rows

# --- INTERFAZ ---
st.title("🌐 Histórico de Red por Slot")
st.info("Esta aplicación analiza la tendencia térmica de cada Slot sumando todos los sitios de la carpeta.")

# Sidebar - Procesamiento
with st.sidebar:
    st.header("Admin de Datos")
    if st.button("🚀 Procesar/Actualizar Red"):
        archivos = [os.path.join(FOLDER_PATH, f) for f in os.listdir(FOLDER_PATH) if f.endswith('.txt')]
        if archivos:
            all_data = []
            bar = st.progress(0)
            for i, arc in enumerate(archivos):
                all_data.extend(extraer_datos_red(arc))
                bar.progress((i + 1) / len(archivos))
            
            df_global = pd.DataFrame(all_data)
            # Agrupar por hora para que el gráfico sea fluido
            df_global['Timestamp'] = df_global['Timestamp'].dt.floor('H')
            df_global.to_parquet(PARQUET_FILE, index=False)
            st.success("¡Base de red actualizada!")
            st.rerun()
        else:
            st.error("No se encontraron archivos .txt")

# --- VISUALIZACIÓN ---
if os.path.exists(PARQUET_FILE):
    df = pd.read_parquet(PARQUET_FILE)
    
    # Selector de métrica y slots
    col_a, col_b = st.columns([1, 3])
    
    with col_a:
        metrica = st.radio("Métrica:", ["Máxima", "Promedio"])
        todos_slots = sorted(df['Slot_Num'].unique())
        selección = st.multiselect("Slots a comparar:", todos_slots, default=todos_slots[:3])
    
    with col_b:
        if selección:
            # Agrupación dinámica
            if metrica == "Máxima":
                df_g = df.groupby(['Timestamp', 'Slot_Num'])['Temp'].max().reset_index()
            else:
                df_g = df.groupby(['Timestamp', 'Slot_Num'])['Temp'].mean().reset_index()
            
            df_plot = df_g[df_g['Slot_Num'].isin(selección)]
            df_plot['Slot'] = df_plot['Slot_Num'].apply(lambda x: f"Slot {x}")

            fig = px.line(df_plot, x='Timestamp', y='Temp', color='Slot',
                         title=f"Tendencia de Temperatura {metrica} en la Red",
                         template="seaborn")
            st.plotly_chart(fig, use_container_width=True)
            
    st.divider()
    st.subheader("🔍 Detalle de Dispersión")
    # Boxplot para ver si hay sitios específicos que "ensucian" el promedio del slot
    fig_box = px.box(df[df['Slot_Num'].isin(selección)], x='Slot_Num', y='Temp', color='Slot_Num')
    st.plotly_chart(fig_box, use_container_width=True)
else:
    st.warning("Aún no existe el archivo histórico. Dale al botón 'Procesar/Actualizar Red' en el panel de la izquierda.")
