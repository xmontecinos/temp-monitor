import streamlit as st
import pandas as pd
import re
import os
import plotly.express as px
from datetime import datetime, timedelta

st.set_page_config(page_title="Monitor Red - Alta Velocidad", layout="wide")

# --- CONFIGURACIÓN ---
UMBRAL_CRITICO = 79 
FOLDER_PATH = 'Temperatura'

st.sidebar.header("🛡️ Control de Red")
if st.sidebar.button("♻️ Forzar Recarga Total"):
    st.cache_data.clear()
    st.rerun()

def extraer_datos_archivo(path):
    """Procesa un archivo de texto de forma ultra eficiente."""
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
                        "Sitio": ne_name.strip(), "Subrack": r[1], "Slot": int(r[2]),
                        "Temp": int(r[3]), "ID_Full": f"{ne_name.strip()} (S:{r[1]}-L:{r[2]})"
                    })
            return res
    except: return []

@st.cache_data(ttl=60)
def obtener_archivos_ordenados(folder):
    """Obtiene la lista de archivos sin leer su contenido (Instantáneo)."""
    if not os.path.exists(folder): return []
    archivos = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".txt")]
    archivos.sort(key=os.path.getmtime, reverse=True)
    return archivos

# --- PROCESAMIENTO INICIAL (SOLO ÚLTIMO REPORTE) ---
lista_archivos = obtener_archivos_ordenados(FOLDER_PATH)

if lista_archivos:
    # Solo procesamos el primero para las alertas (Carga flash)
    df_ultima = pd.DataFrame(extraer_datos_archivo(lista_archivos[0]))
    
    tab1, tab2, tab3 = st.tabs(["🚨 ALERTA ÚLTIMA", "📍 BUSCADOR", "📈 HISTÓRICO 7D"])

    with tab1:
        st.subheader(f"Alertas Críticas: {os.path.basename(lista_archivos[0])}")
        slots_f = sorted(df_ultima['Slot'].unique())
        sel_s = st.multiselect("Filtrar Slots:", slots_f, default=slots_f)
        
        criticos = df_ultima[(df_ultima['Temp'] >= UMBRAL_CRITICO) & (df_ultima['Slot'].isin(sel_s))]
        
        if not criticos.empty:
            cols = st.columns(4)
            for i, (_, r) in enumerate(criticos.sort_values('Temp', ascending=False).iterrows()):
                with cols[i % 4]:
                    st.markdown(f"""<div style="background-color:#FFC7CE; padding:15px; border-radius:10px; text-align:center; border:2px solid #9C0006; margin-bottom:10px;">
                        <h4 style="margin:0; color:#9C0006;">{r['Sitio']}</h4>
                        <h1 style="margin:5px 0; color:#9C0006;">{r['Temp']}°C</h1>
                        <small style="color:#9C0006;">S:{r['Subrack']} | L:{r['Slot']}</small>
                        </div>""", unsafe_allow_html=True)
        else:
            st.success("✅ No hay alertas críticas en este reporte.")

    with tab2:
        sitio = st.selectbox("Sitio:", sorted(df_ultima['Sitio'].unique()))
        st.dataframe(df_ultima[df_ultima['Sitio'] == sitio][['Subrack', 'Slot', 'Temp', 'Timestamp']], use_container_width=True, hide_index=True)

    with tab3:
        # El histórico solo se calcula si el usuario hace clic aquí
        st.subheader("Tendencia Semanal (Últimos 20 reportes)")
        if st.button("📊 Cargar Histórico"):
            with st.spinner('Procesando datos históricos...'):
                data_h = []
                # Limitamos a 20 archivos para que no se cuelgue
                for p in lista_archivos[:20]:
                    data_h.extend(extraer_datos_archivo(p))
                
                df_h = pd.DataFrame(data_h)
                if not df_h.empty:
                    df_h['Hora'] = df_h['Timestamp'].dt.floor('h')
                    df_h = df_h.groupby(['Hora', 'Sitio', 'ID_Full'])['Temp'].max().reset_index()
                    
                    s_h = st.selectbox("Sitio para tendencia:", sorted(df_h['Sitio'].unique()))
                    fig = px.line(df_h[df_h['Sitio'] == s_h], x='Hora', y='Temp', color='ID_Full', markers=True)
                    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No hay archivos en la carpeta.")
