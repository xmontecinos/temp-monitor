import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px
import gc

st.set_page_config(page_title="Monitor Red - 180h+ Estable", layout="wide")

FOLDER_PATH = 'Temperatura'

def extraer_masivo_eficiente(path):
    """Extrae datos sin cargar el archivo completo en RAM."""
    rows = []
    try:
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            ts = None
            sitio = "Desconocido"
            # Procesamos línea por línea para ahorrar RAM
            for line in f:
                # 1. Capturar Fecha (solo una vez por archivo)
                if not ts and "REPORT" in line:
                    m = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', line)
                    if m: ts = pd.to_datetime(f"{m.group(1)} {m.group(2)}")
                
                # 2. Capturar nombre del Sitio
                if "NE Name" in line:
                    sitio = line.split(":")[-1].strip().split()[0]
                
                # 3. Capturar datos de la tabla (Sub Slot Temp)
                # Formato: espacios + numero + numero + numero + temperatura
                match = re.match(r'^\s*\d+\s+(\d+)\s+(\d+)\s+(\d+)', line)
                if match and ts:
                    rows.append({
                        "Timestamp": ts, "Sitio": sitio,
                        "ID": f"{sitio} (S:{match.group(1)}-L:{match.group(2)})",
                        "Temp": int(match.group(3))
                    })
    except: pass
    return rows

@st.cache_data(ttl=300)
def listar_archivos(folder):
    if not os.path.exists(folder): return []
    fs = [os.path.join(folder, f) for f in os.listdir(folder) if ".txt" in f]
    fs.sort(key=lambda x: "".join(re.findall(r'\d+', x)), reverse=True)
    return fs

# --- INTERFAZ ---
archivos = listar_archivos(FOLDER_PATH)

if archivos:
    st.sidebar.title("Configuración")
    # Aumentamos el rango hasta 300 o el total de archivos
    total_disponible = len(archivos)
    horas_a_cargar = st.sidebar.slider("Horas a procesar:", 10, total_disponible, 100)
    
    tab1, tab2 = st.tabs(["📊 TENDENCIA HISTÓRICA", "🚨 ÚLTIMO REPORTE"])

    with tab1:
        if st.button(f"🚀 Procesar {horas_a_cargar} horas ahora"):
            all_data = []
            prog = st.progress(0)
            status = st.empty()
            
            # PROCESAMIENTO POR BLOQUES (Evita el Crash)
            for i, p in enumerate(archivos[:horas_a_cargar]):
                all_data.extend(extraer_masivo_eficiente(p))
                
                # Cada 20 archivos forzamos limpieza de RAM
                if i % 20 == 0:
                    prog.progress((i + 1) / horas_a_cargar)
                    status.text(f"Analizando reporte {i+1} de {horas_a_cargar}...")
                    gc.collect() 

            if all_data:
                df = pd.DataFrame(all_data)
                # Reducimos peso: solo guardamos la temperatura máxima por hora por sitio
                df['Hora'] = df['Timestamp'].dt.floor('h')
                st.session_state["df_final"] = df.groupby(['Hora', 'Sitio', 'ID'])['Temp'].max().reset_index()
                status.success(f"✅ ¡{horas_a_cargar} horas cargadas! RAM liberada.")
            else:
                status.error("No se encontraron datos.")

        if "df_final" in st.session_state:
            df_plot = st.session_state["df_final"]
            sitio = st.selectbox("Seleccionar Sitio:", sorted(df_plot['Sitio'].unique()))
            fig = px.line(df_plot[df_plot['Sitio'] == sitio], x='Hora', y='Temp', color='ID', markers=True)
            st.plotly_chart(fig, use_container_width=True)

    with tab2:
        # Mostrar el reporte más nuevo rápidamente
        df_now = pd.DataFrame(extraer_masivo_eficiente(archivos[0]))
        if not df_now.empty:
            st.write(f"Viendo: {os.path.basename(archivos[0])}")
            st.dataframe(df_now.sort_values('Temp', ascending=False), use_container_width=True)

else:
    st.error("No hay archivos en la carpeta.")
