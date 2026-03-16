import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px

st.set_page_config(page_title="Histórico Global de Red", layout="wide")

# --- RUTA ABSOLUTA SUMINISTRADA ---
# Usamos r'' para que Windows reconozca las barras invertidas correctamente
FOLDER_PATH = r'D:\Temperaturas\temperaturas\temp-monitor\Temperatura\temp-monitor\Temperatura'
PARQUET_FILE = 'red_historico_slots.parquet'

def extraer_datos_red(path):
    rows = []
    try:
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            content = f.read()
            ts_match = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', content)
            if not ts_match: return []
            ts = pd.to_datetime(f"{ts_match.group(1)} {ts_match.group(2)}")
            
            # Buscamos Slot y Temp en todo el archivo (independiente del sitio)
            filas = re.findall(r'^\s*(\d+)\s+(\d+)\s+(\d+)', content, re.MULTILINE)
            for r in filas:
                rows.append({
                    "Timestamp": ts, 
                    "Slot_ID": f"Slot {r[1]}", 
                    "Temp": int(r[2])
                })
    except: pass
    return rows

# --- INTERFAZ ---
st.title("🌐 Evolución Térmica Histórica: Red Global")
st.info(f"Buscando archivos en: {FOLDER_PATH}")

with st.sidebar:
    st.header("Configuración de Red")
    if st.button("🔄 Actualizar Base de Red"):
        if not os.path.exists(FOLDER_PATH):
            st.error("⚠️ La ruta no existe. Verifica la ubicación de los archivos.")
        else:
            archivos = [os.path.join(FOLDER_PATH, f) for f in os.listdir(FOLDER_PATH) if f.endswith('.txt')]
            st.write(f"Archivos encontrados: {len(archivos)}")
            
            if archivos:
                all_data = []
                progreso = st.progress(0)
                for i, arc in enumerate(archivos):
                    data = extraer_datos_red(arc)
                    if data:
                        all_data.extend(data)
                    progreso.progress((i + 1) / len(archivos))
                
                if all_data:
                    df_global = pd.DataFrame(all_data)
                    # Sincronizamos por hora para la red
                    df_global['Timestamp'] = df_global['Timestamp'].dt.floor('H')
                    df_global.to_parquet(PARQUET_FILE, index=False)
                    st.success("✅ Base de red generada.")
                    st.rerun()
                else:
                    st.error("No se extrajeron datos. Revisa el formato de los TXT.")

# --- CARGA Y GRÁFICO ---
if os.path.exists(PARQUET_FILE):
    df = pd.read_parquet(PARQUET_FILE)
    
    col1, col2 = st.columns([1, 4])
    
    with col1:
        metrica = st.radio("Métrica de Red:", ["Promedio", "Máximo"])
        # Ordenar slots numéricamente
        slots_disponibles = sorted(df['Slot_ID'].unique(), key=lambda x: int(re.findall(r'\d+', x)[0]))
        seleccion = st.multiselect("Comparar Slots:", slots_disponibles, default=slots_disponibles[:5])
    
    with col2:
        if seleccion:
            if metrica == "Promedio":
                df_resumen = df.groupby(['Timestamp', 'Slot_ID'])['Temp'].mean().reset_index()
            else:
                df_resumen = df.groupby(['Timestamp', 'Slot_ID'])['Temp'].max().reset_index()
            
            df_plot = df_resumen[df_resumen['Slot_ID'].isin(seleccion)]
            
            fig = px.line(
                df_plot, x='Timestamp', y='Temp', color='Slot_ID',
                title=f"Tendencia Térmica de la Red ({metrica})",
                markers=True,
                template="plotly_white"
            )
            fig.add_hline(y=78, line_dash="dash", line_color="red", annotation_text="CRÍTICO")
            fig.update_layout(hovermode="x unified")
            st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("👈 Haz clic en 'Actualizar Base de Red' para procesar los archivos de la ruta especificada.")
