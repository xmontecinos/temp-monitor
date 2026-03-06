import streamlit as st
import pandas as pd
import re
import os
import plotly.express as px
from datetime import datetime, timedelta

st.set_page_config(page_title="Monitor Red - Corrección Fecha", layout="wide")

# --- CONFIGURACIÓN ---
UMBRAL_CRITICO = 75 
FOLDER_PATH = 'Temperatura'

st.sidebar.header("🛡️ Control de Red")
if st.sidebar.button("♻️ Forzar Recarga Total"):
    st.cache_data.clear()
    st.rerun()

def extraer_datos_archivo(path):
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

@st.cache_data(ttl=30)
def obtener_archivos_por_nombre(folder):
    """Ordena archivos por la fecha contenida en su nombre (YYYYMMDD)."""
    if not os.path.exists(folder): return []
    archivos = [f for f in os.listdir(folder) if f.endswith(".txt")]
    
    # Esta línea busca los números en el nombre del archivo para ordenar
    # MMLTask_BRDTEMP_20260306_... -> extrae 20260306
    archivos.sort(key=lambda x: re.findall(r'\d+', x)[0] if re.findall(r'\d+', x) else x, reverse=True)
    
    return [os.path.join(folder, f) for f in archivos]

# --- PROCESAMIENTO ---
lista_archivos = obtener_archivos_por_nombre(FOLDER_PATH)

if lista_archivos:
    # AHORA SI: Tomamos el que tiene la fecha más reciente en el NOMBRE
    archivo_actual = lista_archivos[0]
    df_ultima = pd.DataFrame(extraer_datos_archivo(archivo_actual))
    
    tab1, tab2, tab3 = st.tabs(["🚨 ALERTA ÚLTIMA", "📍 BUSCADOR", "📈 HISTÓRICO 7D"])

    with tab1:
        # Mostramos el nombre del archivo para confirmar que es el nuevo
        st.subheader(f"Reporte Actual: {os.path.basename(archivo_actual)}")
        
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
            st.success("✅ No hay alertas críticas en el reporte más reciente.")

    # ... (Pestañas 2 y 3 se mantienen igual)
