import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px
import gc

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="Histórico de Red: Subrack & Slot", layout="wide")

# Ruta absoluta suministrada
FOLDER_PATH = r'D:\Temperaturas\temperaturas\temp-monitor\Temperatura\temp-monitor\Temperatura'
PARQUET_FILE = 'red_historico_hardware.parquet'
UMBRAL_CRITICO = 78

def extraer_datos_hardware(path):
    rows = []
    try:
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            content = f.read()
            # Extraer Timestamp
            ts_match = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', content)
            if not ts_match: return []
            ts = pd.to_datetime(f"{ts_match.group(1)} {ts_match.group(2)}")
            
            # Buscamos la tabla de datos: 
            # r[0] = Subrack No. | r[1] = Slot No. | r[2] = Board Temperature
            filas = re.findall(r'^\s*(\d+)\s+(\d+)\s+(\d+)', content, re.MULTILINE)
            for r in filas:
                subrack = r[0]
                slot = r[1]
                temp = int(r[2])
                
                # Creamos un ID único de hardware (ej: S0-L3)
                rows.append({
                    "Timestamp": ts, 
                    "Hardware_ID": f"S{subrack}-L{slot}", 
                    "Temp": temp
                })
    except: pass
    return rows

# --- INTERFAZ ---
st.title("🌐 Monitoreo de Red: Histórico por Subrack/Slot")
st.markdown(f"**Análisis consolidado de hardware** basado en archivos de: `{FOLDER_PATH}`")

with st.sidebar:
    st.header("⚙️ Procesamiento de Datos")
    num_archivos = st.number_input("Cantidad de archivos a procesar:", min_value=1, value=200)
    
    if st.button("🚀 Generar Histórico de Red"):
        if not os.path.exists(FOLDER_PATH):
            st.error("Ruta no encontrada.")
        else:
            archivos = [os.path.join(FOLDER_PATH, f) for f in os.listdir(FOLDER_PATH) if f.endswith('.txt')]
            if archivos:
                all_data = []
                bar = st.progress(0)
                status = st.empty()
                
                for i, arc in enumerate(archivos[:num_archivos]):
                    status.text(f"Leyendo: {os.path.basename(arc)}")
                    all_data.extend(extraer_datos_hardware(arc))
                    bar.progress((i + 1) / len(archivos[:num_archivos]))
                
                df_global = pd.DataFrame(all_data)
                # Sincronizamos por hora para unir todos los sitios en un punto temporal
                df_global['Timestamp'] = df_global['Timestamp'].dt.floor('H')
                
                # Guardamos la base procesada
                df_global.to_parquet(PARQUET_FILE, index=False)
                st.session_state["df_hardware"] = df_global
                st.success("✅ Base de red actualizada.")
                st.rerun()

# --- VISUALIZACIÓN ---
if os.path.exists(PARQUET_FILE) and "df_hardware" not in st.session_state:
    st.session_state["df_hardware"] = pd.read_parquet(PARQUET_FILE)

if "df_hardware" in st.session_state:
    df = st.session_state["df_hardware"]
    
    # 1. Agregación Global (Promedio o Máximo de la red por hardware)
    metrica = st.radio("Métrica para la Red:", ["Promedio Global", "Temperatura Máxima"], horizontal=True)
    
    if metrica == "Promedio Global":
        df_resumen = df.groupby(['Timestamp', 'Hardware_ID'])['Temp'].mean().reset_index()
    else:
        df_resumen = df.groupby(['Timestamp', 'Hardware_ID'])['Temp'].max().reset_index()

    # 2. Selector de Hardware (Subrack-Slot)
    hw_disponibles = sorted(df_resumen['Hardware_ID'].unique())
    seleccion = st.multiselect("Seleccione Hardware (S=Subrack, L=Slot):", 
                               hw_disponibles, 
                               default=hw_disponibles[:5])

    if seleccion:
        df_plot = df_resumen[df_resumen['Hardware_ID'].isin(seleccion)]
        
        # 3. Gráfico idéntico al solicitado
        fig = px.line(
            df_plot, 
            x='Timestamp', 
            y='Temp', 
            color='Hardware_ID',
            title=f"Evolución Térmica de Red por Componente ({metrica})",
            markers=True,
            template="plotly_white"
        )
        
        fig.add_hline(y=UMBRAL_CRITICO, line_dash="dash", line_color="red", annotation_text="CRÍTICO")
        fig.update_layout(hovermode="x unified", yaxis_title="Temp (°C)", legend_title="Hardware ID")
        
        st.plotly_chart(fig, use_container_width=True)
        
        # 4. Tabla de Ranking (¿Qué hardware está sufriendo más?)
        st.subheader("⚠️ Top 5 Componentes más calientes de la Red")
        ranking = df_plot.groupby('Hardware_ID')['Temp'].max().sort_values(ascending=False).head(5)
        st.table(ranking)

else:
    st.info("👈 Use el panel lateral para procesar los archivos TXT de la red.")
