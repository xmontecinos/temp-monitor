import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px
import gc

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="Histórico Global de Red", layout="wide")

# Ruta absoluta según tu estructura (ajustada para evitar errores de ruta)
FOLDER_PATH = r'D:\Temperaturas\temperaturas\temp-monitor\Temperatura\temp-monitor\Temperatura'
PARQUET_FILE = 'red_historico_slots.parquet'
UMBRAL_CRITICO = 78

def extraer_datos_red(path):
    rows = []
    try:
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            content = f.read()
            ts_match = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', content)
            if not ts_match: return []
            ts = pd.to_datetime(f"{ts_match.group(1)} {ts_match.group(2)}")
            
            # Buscamos Slot (segunda columna) y Temperatura (tercera columna)
            filas = re.findall(r'^\s*\d+\s+(\d+)\s+(\d+)', content, re.MULTILINE)
            for r in filas:
                slot_n = int(r[0])
                if slot_n < 20: # Filtramos slots de servicio/físicos
                    rows.append({
                        "Timestamp": ts, 
                        "Slot_ID": f"Slot {slot_n}", 
                        "Temp": int(r[1])
                    })
    except: pass
    return rows

# --- INTERFAZ ---
st.title("🌐 Evolución Térmica Histórica: Red Global")

with st.sidebar:
    st.header("Gestión de Datos")
    num_reportes = st.slider("Cantidad de archivos:", 1, 500, 100)
    
    if st.button("🔥 Generar/Actualizar Base de Red"):
        archivos = [os.path.join(FOLDER_PATH, f) for f in os.listdir(FOLDER_PATH) if f.endswith('.txt')]
        if archivos:
            all_data = []
            progreso = st.progress(0)
            for i, arc in enumerate(archivos[:num_reportes]):
                all_data.extend(extraer_datos_red(arc))
                progreso.progress((i + 1) / len(archivos[:num_reportes]))
            
            df_global = pd.DataFrame(all_data)
            # Agrupamos por HORA y SLOT para promediar toda la red
            df_global['Timestamp'] = df_global['Timestamp'].dt.floor('H')
            df_final = df_global.groupby(['Timestamp', 'Slot_ID'])['Temp'].mean().reset_index()
            
            df_final.to_parquet(PARQUET_FILE, index=False)
            st.session_state["df_red"] = df_final
            st.success("¡Base de red actualizada!")
            st.rerun()

# --- VISUALIZACIÓN ---
if os.path.exists(PARQUET_FILE) and "df_red" not in st.session_state:
    st.session_state["df_red"] = pd.read_parquet(PARQUET_FILE)

if "df_red" in st.session_state:
    df = st.session_state["df_red"]
    
    # Selectores similares a tu imagen pero de RED
    slots_disponibles = sorted(df['Slot_ID'].unique(), key=lambda x: int(x.split()[1]))
    
    st.subheader("📊 Comparativa de Slots a Nivel Red")
    seleccion = st.multiselect("Seleccionar Slots para comparar en toda la red:", 
                               slots_disponibles, 
                               default=slots_disponibles[:3])
    
    if seleccion:
        df_plot = df[df['Slot_ID'].isin(seleccion)]
        
        fig = px.line(
            df_plot, 
            x='Timestamp', 
            y='Temp', 
            color='Slot_ID',
            title="Tendencia de Temperatura Promedio por Slot (Toda la Red)",
            markers=True,
            template="plotly_white"
        )
        
        fig.add_hline(y=UMBRAL_CRITICO, line_dash="dash", line_color="red", annotation_text="UMBRAL CRÍTICO")
        fig.update_layout(hovermode="x unified", yaxis_title="Temp °C")
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Tabla de resumen
        st.write("### 📋 Resumen Estadístico de la Red")
        resumen = df_plot.groupby('Slot_ID')['Temp'].agg(['max', 'mean']).reset_index()
        resumen.columns = ['Slot', 'Máximo Global (°C)', 'Promedio Global (°C)']
        st.table(resumen)
else:
    st.info("👈 Usa el panel lateral para generar la base de datos de red primero.")
