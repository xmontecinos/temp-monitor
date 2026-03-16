import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Evolución Térmica: Red Global", layout="wide")

# Ruta absoluta basada en tu estructura de carpetas
FOLDER_PATH = r'D:\Temperaturas\temperaturas\temp-monitor\Temperatura\temp-monitor\Temperatura'
PARQUET_FILE = 'red_global_hardware.parquet'
UMBRAL_CRITICO = 78

def extraer_datos_dispositivos(path):
    rows = []
    try:
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            content = f.read()
            # 1. Extraer Fecha y Hora
            ts_match = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', content)
            if not ts_match: return []
            ts = pd.to_datetime(f"{ts_match.group(1)} {ts_match.group(2)}")
            
            # 2. Extraer columnas: Subrack No. | Slot No. | Temperature
            # Buscamos líneas que tengan al menos 3 bloques numéricos
            filas = re.findall(r'^\s*(\d+)\s+(\d+)\s+(\d+)', content, re.MULTILINE)
            for r in filas:
                temp_val = int(r[2])
                # Filtro para asegurar que capturamos temperaturas reales y no IDs (ej: >15°C)
                if temp_val > 15:
                    rows.append({
                        "Timestamp": ts, 
                        "HW_ID": f"S{r[0]}-L{r[1]}", # Ejemplo: S0-L4
                        "Temp": temp_val
                    })
    except: pass
    return rows

# --- INTERFAZ STREAMLIT ---
st.title("🌐 Evolución Térmica Histórica: Red Global")
st.markdown(f"**Analizando flota en:** `{FOLDER_PATH}`")

with st.sidebar:
    st.header("⚙️ Base de Datos")
    if st.button("🚀 Actualizar Histórico Global"):
        archivos = [os.path.join(FOLDER_PATH, f) for f in os.listdir(FOLDER_PATH) if f.endswith('.txt')]
        if archivos:
            all_data = []
            bar = st.progress(0)
            for i, arc in enumerate(archivos):
                all_data.extend(extraer_datos_dispositivos(arc))
                bar.progress((i + 1) / len(archivos))
            
            if all_data:
                df = pd.DataFrame(all_data)
                # Sincronizamos por hora para que todos los sitios coincidan en el gráfico
                df['Timestamp'] = df['Timestamp'].dt.floor('H')
                df.to_parquet(PARQUET_FILE, index=False)
                st.success("✅ Base de Red actualizada.")
                st.rerun()
            else:
                st.error("No se encontraron datos válidos en los TXT.")

# --- VISUALIZACIÓN ---
if os.path.exists(PARQUET_FILE):
    df_red = pd.read_parquet(PARQUET_FILE)
    
    metrica = st.radio("Ver en la Red:", ["Promedio de Red", "Máximo de Red"], horizontal=True)
    
    # Agrupamos por ID de Hardware y tiempo para consolidar la Red Global
    if "Promedio" in metrica:
        df_plot = df_red.groupby(['Timestamp', 'HW_ID'])['Temp'].mean().reset_index()
    else:
        df_plot = df_red.groupby(['Timestamp', 'HW_ID'])['Temp'].max().reset_index()

    # Selector de Slots (Ordenado numéricamente)
    ids_disponibles = sorted(df_plot['HW_ID'].unique(), key=lambda x: [int(c) for c in re.findall(r'\d+', x)])
    seleccion = st.multiselect("Seleccionar Hardware (Subrack-Slot):", ids_disponibles, default=ids_disponibles[:5])

    if seleccion:
        fig = px.line(
            df_plot[df_plot['HW_ID'].isin(seleccion)], 
            x='Timestamp', y='Temp', color='HW_ID',
            markers=True, template="plotly_white",
            labels={'Temp': 'Temperatura (°C)', 'HW_ID': 'Slot de Red'}
        )
        fig.add_hline(y=UMBRAL_CRITICO, line_dash="dash", line_color="red", annotation_text="UMBRAL CRÍTICO")
        fig.update_layout(hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)
else:
    st.info("👈 Haz clic en 'Actualizar Histórico Global' para procesar los archivos.")
