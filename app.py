import streamlit as st
import pandas as pd
import re
import os
import plotly.express as px
import gc # Recolector de basura para liberar RAM

st.set_page_config(page_title="Monitor Red - Ultra Liviano", layout="wide")

# --- CONFIGURACIÓN ---
UMBRAL_CRITICO = 65 
FOLDER_PATH = 'Temperatura'

st.sidebar.header("🛡️ Control de Red")
if st.sidebar.button("♻️ Forzar Recarga Total"):
    st.cache_data.clear()
    st.session_state.clear()
    st.rerun()

def extraer_datos_archivo(path):
    """Extracción optimizada para no saturar la RAM."""
    try:
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            content = f.read()
            blocks = re.findall(r'NE Name:\s+([^\n\r]+).*?(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2}).*?Display Board Temperature(.*?)\n---    END', content, re.DOTALL)
            res = []
            for ne_name, fecha, hora, table in blocks:
                rows = re.findall(r'(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(?:\d+|NULL)', table)
                for r in rows:
                    res.append({
                        "Timestamp": pd.to_datetime(f"{fecha} {hora}"),
                        "Sitio": ne_name.strip(), 
                        "Subrack": r[1], "Slot": int(r[2]),
                        "Temp": int(r[3]), 
                        "ID_Full": f"{ne_name.strip()} (S:{r[1]}-L:{r[2]})"
                    })
            return res
    except: return []

@st.cache_data(ttl=30)
def listar_archivos(folder):
    if not os.path.exists(folder): return []
    archivos = [f for f in os.listdir(folder) if f.endswith(".txt")]
    archivos.sort(key=lambda x: "".join(re.findall(r'\d+', x)), reverse=True)
    return [os.path.join(folder, f) for f in archivos]

# --- FLUJO PRINCIPAL ---
archivos_total = listar_archivos(FOLDER_PATH)

if archivos_total:
    # 1. CARGA INMEDIATA (Solo 1 archivo para que la app no inicie en blanco)
    df_actual = pd.DataFrame(extraer_datos_archivo(archivos_total[0]))
    
    tab1, tab2, tab3 = st.tabs(["🚨 ALERTA ÚLTIMA", "📍 BUSCADOR", "📈 HISTÓRICO 7D"])

    with tab1:
        st.subheader(f"Reporte: {os.path.basename(archivos_total[0])}")
        if not df_actual.empty:
            criticos = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
            if not criticos.empty:
                cols = st.columns(4)
                for i, (_, r) in enumerate(criticos.sort_values('Temp', ascending=False).iterrows()):
                    with cols[i % 4]:
                        st.error(f"**{r['Sitio']}** \n{r['Temp']}°C")
            else: st.success("✅ Todo normal.")

    with tab2:
        if not df_actual.empty:
            sitio_sel = st.selectbox("Sitio:", sorted(df_actual['Sitio'].unique()))
            # Corregido: 'width' en lugar de 'use_container_width' para evitar errores en consola
            st.dataframe(df_actual[df_actual['Sitio'] == sitio_sel], width=1200)

    with tab3:
        st.subheader("Tendencia Semanal")
        
        # LÓGICA DE CARGA POR LOTES PARA EL HISTÓRICO
        if st.button("📊 Generar Gráfica Histórica"):
            data_acumulada = []
            progreso = st.progress(0)
            status = st.empty()
            
            # Procesamos máximo 15 archivos para asegurar que la RAM no colapse
            archivos_a_procesar = archivos_total[:15]
            
            for idx, p in enumerate(archivos_a_procesar):
                status.text(f"Cargando reporte {idx+1}/{len(archivos_a_procesar)}...")
                data_acumulada.extend(extraer_datos_archivo(p))
                progreso.progress((idx + 1) / len(archivos_a_procesar))
                gc.collect() # Limpieza forzada de RAM en cada vuelta
            
            df_h = pd.DataFrame(data_acumulada)
            if not df_h.empty:
                df_h['Hora'] = df_h['Timestamp'].dt.floor('h')
                st.session_state["df_h"] = df_h.groupby(['Hora', 'Sitio', 'ID_Full'])['Temp'].max().reset_index()
                status.success("✅ ¡Gráfica lista!")
            else:
                status.error("No se pudieron extraer datos.")

        if "df_h" in st.session_state:
            df_plot = st.session_state["df_h"]
            sitio_h = st.selectbox("Filtrar Sitio:", sorted(df_plot['Sitio'].unique()), key="h_site")
            
            fig = px.line(df_plot[df_plot['Sitio'] == sitio_h], 
                         x='Hora', y='Temp', color='ID_Full', markers=True)
            st.plotly_chart(fig, width=1200)
else:
    st.info("Carpeta 'Temperatura' no encontrada.")
