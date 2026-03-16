import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="Evolución Térmica: Red Global", layout="wide")

# Ruta absoluta basada en tus capturas
FOLDER_PATH = r'D:\Temperaturas\temperaturas\temp-monitor\Temperatura\temp-monitor\Temperatura'
PARQUET_FILE = 'red_global_hardware.parquet'
UMBRAL_CRITICO = 78

def extraer_datos_dispositivos(path):
    rows = []
    try:
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            content = f.read()
            # 1. Extraer Fecha y Hora del encabezado
            ts_match = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', content)
            if not ts_match: return []
            ts = pd.to_datetime(f"{ts_match.group(1)} {ts_match.group(2)}")
            
            # 2. Capturar: Subrack No. | Slot No. | Board Temperature
            # Buscamos líneas con 3 números seguidos (formato tabla Huawei)
            filas = re.findall(r'^\s*(\d+)\s+(\d+)\s+(\d+)', content, re.MULTILINE)
            for r in filas:
                temp_val = int(r[2])
                # Filtro: Evitamos capturar IDs (0-1) como temperaturas
                if temp_val > 15:
                    rows.append({
                        "Timestamp": ts, 
                        "HW_ID": f"S{r[0]}-L{r[1]}", # Ejemplo: S0-L4
                        "Temp": temp_val
                    })
    except: pass
    return rows

# --- INTERFAZ ---
st.title("🌐 Evolución Térmica Histórica: Red Global")
st.markdown(f"**Analizando red completa en:** `{FOLDER_PATH}`")

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
                # Sincronizamos por hora para unir todos los sitios en los mismos puntos
                df['Timestamp'] = df['Timestamp'].dt.floor('H')
                df.to_parquet(PARQUET_FILE, index=False)
                st.success("✅ Base de Red actualizada.")
                st.rerun()
            else:
                st.error("No se encontraron datos válidos. Verifica el formato de los TXT.")

# --- VISUALIZACIÓN ---
if os.path.exists(PARQUET_FILE):
    df_red = pd.read_parquet(PARQUET_FILE)
    
    metrica = st.radio("Cálculo para la Red:", ["Promedio de Red", "Máximo de Red"], horizontal=True)
    
    # Consolidamos la red global: promediamos todos los sitios por cada Slot/Subrack
    if "Promedio" in metrica:
        df_plot = df_red.groupby(['Timestamp', 'HW_ID'])['Temp'].mean().reset_index()
    else:
        df_plot = df_red.groupby(['Timestamp', 'HW_ID'])['Temp'].max().reset_index()

    ids_disponibles = sorted(df_plot['HW_ID'].unique(), key=lambda x: [int(c) for c in re.findall(r'\d+', x)])
    seleccion = st.multiselect("Seleccionar Slots de Red:", ids_disponibles, default=ids_disponibles[:5])

    if seleccion:
        fig = px.line(
            df_plot[df_plot['HW_ID'].isin(seleccion)], 
            x='Timestamp', y='Temp', color='HW_ID',
            markers=True, template="plotly_white",
            labels={'Temp': 'Grados Celsius (°C)', 'HW_ID': 'Hardware ID'}
        )
        fig.add_hline(y=UMBRAL_CRITICO, line_dash="dash", line_color="red", annotation_text="CRÍTICO")
        fig.update_layout(hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)
else:
    st.info("👈 Pulsa en 'Actualizar Histórico Global' para procesar la red.")
