import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="Histórico de Temperaturas", layout="wide")

FOLDER_PATH = r'D:\Temperaturas\temperaturas\temp-monitor\Temperatura\temp-monitor\Temperatura'
PARQUET_FILE = 'historico_temperaturas.parquet'

def extraer_datos(path):
    rows = []
    try:
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            content = f.read()
            # Extraer Fecha y Hora
            ts_match = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', content)
            if not ts_match: return []
            ts = pd.to_datetime(f"{ts_match.group(1)} {ts_match.group(2)}")
            
            # Capturar: Subrack | Slot | Temperature
            # El regex busca la estructura de columnas numéricas
            filas = re.findall(r'^\s*(\d+)\s+(\d+)\s+(\d+)', content, re.MULTILINE)
            for r in filas:
                subrack, slot, temp = r[0], r[1], int(r[2])
                if temp > 15:  # Filtro para evitar IDs o ruido
                    rows.append({
                        "Timestamp": ts,
                        "Subrack": subrack,
                        "Slot": slot,
                        "HW_ID": f"Subrack {subrack} - Slot {slot}",
                        "Temperatura": temp
                    })
    except Exception as e:
        st.error(f"Error procesando {os.path.basename(path)}: {e}")
    return rows

# --- INTERFAZ Y PROCESAMIENTO ---
st.title("🌡️ Histórico de Temperaturas por Subrack y Slot")

if st.sidebar.button("🔄 Actualizar Base de Datos"):
    archivos = [os.path.join(FOLDER_PATH, f) for f in os.listdir(FOLDER_PATH) if f.endswith('.txt')]
    if archivos:
        all_data = []
        progress = st.progress(0)
        for i, arc in enumerate(archivos):
            all_data.extend(extraer_datos(arc))
            progress.progress((i + 1) / len(archivos))
        
        if all_data:
            df = pd.DataFrame(all_data)
            df.to_parquet(PARQUET_FILE, index=False)
            st.success("✅ Histórico actualizado correctamente.")
            st.rerun()

# --- VISUALIZACIÓN ---
if os.path.exists(PARQUET_FILE):
    df = pd.read_parquet(PARQUET_FILE)
    
    # Filtros de selección
    subracks_disp = sorted(df['Subrack'].unique(), key=int)
    sub_sel = st.sidebar.multiselect("Filtrar Subrack:", subracks_disp, default=subracks_disp)
    
    df_filtrado = df[df['Subrack'].isin(sub_sel)]
    slots_disp = sorted(df_filtrado['HW_ID'].unique())
    seleccion = st.multiselect("Seleccionar Slots específicos:", slots_disp, default=slots_disp[:3])

    if seleccion:
        data_plot = df_filtrado[df_filtrado['HW_ID'].isin(seleccion)].sort_values('Timestamp')
        
        fig = px.line(
            data_plot, 
            x='Timestamp', 
            y='Temperatura', 
            color='HW_ID',
            markers=True,
            title="Evolución de Temperatura (°C)",
            labels={'Temperatura': 'Temp °C', 'Timestamp': 'Fecha/Hora'},
            template="plotly_dark"
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Tabla detallada al final
        with st.expander("Ver datos tabulares"):
            st.dataframe(data_plot.sort_values(by=['Timestamp', 'Subrack', 'Slot'], ascending=False))
else:
    st.info("👈 Por favor, haz clic en 'Actualizar Base de Datos' para comenzar.")
