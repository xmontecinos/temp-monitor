import streamlit as st
import pandas as pd
import re
import os
import plotly.express as px
from datetime import datetime

st.set_page_config(page_title="Monitor Red - Histórico Corregido", layout="wide")

# --- CONFIGURACIÓN ---
UMBRAL_CRITICO = 78 
FOLDER_PATH = 'Temperatura'

st.sidebar.header("🛡️ Control de Red")
if st.sidebar.button("♻️ Forzar Recarga Total"):
    st.cache_data.clear()
    st.rerun()

def extraer_datos_archivo(path):
    """Extracción optimizada para evitar fugas de memoria."""
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

@st.cache_data(ttl=60)
def listar_archivos(folder):
    """Lista archivos ordenados por nombre numérico (YYYYMMDD_HHMMSS)."""
    if not os.path.exists(folder): return []
    archivos = [f for f in os.listdir(folder) if f.endswith(".txt")]
    archivos.sort(key=lambda x: "".join(re.findall(r'\d+', x)), reverse=True)
    return [os.path.join(folder, f) for f in archivos]

# --- LÓGICA DE CARGA ---
archivos_total = listar_archivos(FOLDER_PATH)

if archivos_total:
    # 1. CARGA INMEDIATA (Pestaña 1 y 2)
    df_actual = pd.DataFrame(extraer_datos_archivo(archivos_total[0]))
    
    tab1, tab2, tab3 = st.tabs(["🚨 ALERTA ÚLTIMA", "📍 BUSCADOR", "📈 HISTÓRICO 7D"])

    with tab1:
        st.subheader(f"Último Reporte: {os.path.basename(archivos_total[0])}")
        slots_f = sorted(df_actual['Slot'].unique()) if not df_actual.empty else []
        sel_s = st.multiselect("Filtrar Slots:", slots_f, default=slots_f)
        criticos = df_actual[(df_actual['Temp'] >= UMBRAL_CRITICO) & (df_actual['Slot'].isin(sel_s))] if not df_actual.empty else pd.DataFrame()
        
        if not criticos.empty:
            cols = st.columns(4)
            for i, (_, r) in enumerate(criticos.sort_values('Temp', ascending=False).iterrows()):
                with cols[i % 4]:
                    st.error(f"**{r['Sitio']}** \n{r['Temp']}°C (S:{r['Subrack']} L:{r['Slot']})")
        else:
            st.success("✅ Todo normal en el último reporte.")

    with tab2:
        if not df_actual.empty:
            sitio_sel = st.selectbox("Sitio:", sorted(df_actual['Sitio'].unique()))
            st.dataframe(df_actual[df_actual['Sitio'] == sitio_sel], use_container_width=True)

    with tab3:
        st.subheader("Tendencia Semanal")
        st.info("Para evitar que la app se bloquee, procesaremos los últimos 20 reportes.")
        
        if st.button("📊 Cargar Gráfico Histórico"):
            all_data = []
            progreso = st.progress(0)
            status_text = st.empty()
            
            # Procesamos máximo 20 archivos para no saturar la RAM del servidor gratuito
            max_archivos = archivos_total[:20]
            for i, p in enumerate(max_archivos):
                status_text.text(f"Procesando archivo {i+1} de {len(max_archivos)}...")
                all_data.extend(extraer_datos_archivo(p))
                progreso.progress((i + 1) / len(max_archivos))
            
            df_h = pd.DataFrame(all_data)
            if not df_h.empty:
                # Agrupamos por hora para que el gráfico sea más liviano
                df_h['Hora'] = df_h['Timestamp'].dt.floor('h')
                df_h = df_h.groupby(['Hora', 'Sitio', 'ID_Full'])['Temp'].max().reset_index()
                
                s_h = st.selectbox("Elegir Sitio para tendencia:", sorted(df_h['Sitio'].unique()))
                fig = px.line(df_h[df_h['Sitio'] == s_h], x='Hora', y='Temp', color='ID_Full', markers=True)
                st.plotly_chart(fig, use_container_width=True)
                status_text.text("✅ Carga completada.")
            else:
                st.error("No se pudieron procesar datos para el gráfico.")
else:
    st.warning("No hay archivos en la carpeta 'Temperatura'.")
