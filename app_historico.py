import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Histórico Global de Red", layout="wide")

# Ruta absoluta basada en tus carpetas
FOLDER_PATH = r'D:\Temperaturas\temperaturas\temp-monitor\Temperatura\temp-monitor\Temperatura'
PARQUET_FILE = 'red_historico_hardware.parquet'
UMBRAL_CRITICO = 78

def extraer_datos_huawei(path):
    rows = []
    try:
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            content = f.read()
            
            # Extraer Timestamp del encabezado
            ts_match = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', content)
            if not ts_match: return []
            ts = pd.to_datetime(f"{ts_match.group(1)} {ts_match.group(2)}")
            
            # Buscamos las líneas de la tabla: Subrack | Slot | Temperature
            # Este patrón es más flexible con los espacios
            filas = re.findall(r'^\s*(\d+)\s+(\d+)\s+(\d+)', content, re.MULTILINE)
            
            for r in filas:
                subrack, slot, temp = int(r[0]), int(r[1]), int(r[2])
                
                # Filtro para ignorar IDs que se confunden con temperatura (como 0 o 1)
                if temp > 15 and slot < 30: 
                    rows.append({
                        "Timestamp": ts, 
                        "Hardware_ID": f"S:{subrack}-L:{slot}", 
                        "Temp": temp
                    })
    except: pass
    return rows

# --- INTERFAZ ---
st.title("🌐 Evolución Térmica Histórica: Red Global")

with st.sidebar:
    st.header("Gestión de Datos")
    if st.button("🚀 Actualizar Base de Red"):
        if not os.path.exists(FOLDER_PATH):
            st.error(f"Ruta no encontrada: {FOLDER_PATH}")
        else:
            archivos = [os.path.join(FOLDER_PATH, f) for f in os.listdir(FOLDER_PATH) if f.endswith('.txt')]
            if archivos:
                all_data = []
                bar = st.progress(0)
                for i, arc in enumerate(archivos):
                    all_data.extend(extraer_datos_huawei(arc))
                    bar.progress((i + 1) / len(archivos))
                
                if all_data:
                    df = pd.DataFrame(all_data)
                    df['Timestamp'] = df['Timestamp'].dt.floor('H') # Unir todos los sitios por hora
                    df.to_parquet(PARQUET_FILE, index=False)
                    st.success("✅ ¡Base de red lista!")
                    st.rerun()
                else:
                    st.error("No se encontraron datos. Verifica el formato de los TXT.")

# --- VISUALIZACIÓN ---
if os.path.exists(PARQUET_FILE):
    df_full = pd.read_parquet(PARQUET_FILE)
    
    metrica = st.radio("Cálculo para la Red:", ["Promedio de Red", "Máximo de Red"], horizontal=True)
    
    if "Promedio" in metrica:
        df_plot = df_full.groupby(['Timestamp', 'Hardware_ID'])['Temp'].mean().reset_index()
    else:
        df_plot = df_full.groupby(['Timestamp', 'Hardware_ID'])['Temp'].max().reset_index()

    hw_ids = sorted(df_plot['Hardware_ID'].unique(), key=lambda x: [int(c) for c in re.findall(r'\d+', x)])
    seleccion = st.multiselect("Comparar Hardware de la Red (Subrack-Slot):", hw_ids, default=hw_ids[:5])

    if seleccion:
        fig = px.line(
            df_plot[df_plot['Hardware_ID'].isin(seleccion)], 
            x='Timestamp', y='Temp', color='Hardware_ID',
            markers=True, template="plotly_white",
            labels={'Temp': 'Temperatura (°C)'}
        )
        fig.add_hline(y=UMBRAL_CRITICO, line_dash="dash", line_color="red", annotation_text="CRÍTICO")
        st.plotly_chart(fig, use_container_width=True)
else:
    st.info("👈 Pulsa en 'Actualizar Base de Red' para cargar los datos.")
