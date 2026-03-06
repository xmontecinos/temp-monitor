import streamlit as st
import pandas as pd
import re
import os
import plotly.express as px
from datetime import datetime, timedelta

st.set_page_config(page_title="Monitor Red Ultra", layout="wide")

# --- CONFIGURACIÓN ---
UMBRAL_CRITICO = 79 
FOLDER_PATH = 'Temperatura'

st.sidebar.header("🛡️ Control de Red")
if st.sidebar.button("♻️ Forzar Recarga Total"):
    st.cache_data.clear()
    st.rerun()

def parsear_contenido(content, ne_name, fecha, hora):
    """Extrae datos de forma eficiente."""
    rows = re.findall(r'(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(?:\d+|NULL)', content)
    return [{
        "Timestamp": pd.to_datetime(f"{fecha} {hora}"),
        "Sitio": ne_name.strip(),
        "Subrack": r[1],
        "Slot": int(r[2]),
        "Temp": int(r[3]),
        "ID_Full": f"{ne_name.strip()} (S:{r[1]}-L:{r[2]})"
    } for r in rows]

@st.cache_data(ttl=300) # Caché de 5 minutos para velocidad máxima
def obtener_datos_optimizados(folder):
    if not os.path.exists(folder): return None, None
    archivos = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".txt")]
    if not archivos: return None, None
    
    # Ordenar: El primero es el más nuevo
    archivos.sort(key=os.path.getmtime, reverse=True)

    # 1. ULTIMO REPORTE (Solo 1 archivo = Carga instantánea)
    data_ultima = []
    try:
        with open(archivos[0], 'r', encoding='latin-1', errors='ignore') as f:
            content = f.read()
            blocks = re.findall(r'NE Name:\s+([^\n\r]+).*?(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2}).*?Display Board Temperature(.*?)\n---    END', content, re.DOTALL)
            for ne_name, fecha, hora, table_text in blocks:
                data_ultima.extend(parsear_contenido(table_text, ne_name, fecha, hora))
    except: pass
    df_ultima = pd.DataFrame(data_ultima)

    # 2. HISTORICO (7 DÍAS - Procesamiento inteligente)
    data_hist = []
    limite_7d = datetime.now() - timedelta(days=7)
    
    # Procesamos un máximo de 30 archivos (suficiente para tendencia horaria de una semana)
    # Esto reduce el tiempo de carga de minutos a segundos
    for path in archivos[:30]: 
        if datetime.fromtimestamp(os.path.getmtime(path)) < limite_7d: break
        try:
            with open(path, 'r', encoding='latin-1', errors='ignore') as f:
                content = f.read()
                blocks = re.findall(r'NE Name:\s+([^\n\r]+).*?(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2}).*?Display Board Temperature(.*?)\n---    END', content, re.DOTALL)
                for ne_name, fecha, hora, table_text in blocks:
                    data_hist.extend(parsear_contenido(table_text, ne_name, fecha, hora))
        except: continue
    
    df_hist = pd.DataFrame(data_hist)
    if not df_hist.empty:
        df_hist['Hora'] = df_hist['Timestamp'].dt.floor('h')
        # Reducimos tamaño de datos agrupando por hora
        df_hist = df_hist.groupby(['Hora', 'Sitio', 'ID_Full', 'Slot'])['Temp'].max().reset_index()

    return df_ultima, df_hist

# --- INTERFAZ ---
df_u, df_h = obtener_datos_optimizados(FOLDER_PATH)

if df_u is not None and not df_u.empty:
    t1, t2, t3 = st.tabs(["🚨 ALERTA ÚLTIMA", "📍 BUSCADOR", "📈 HISTÓRICO 7D"])

    with t1:
        # Selección de Slots para el semáforo
        slots_opciones = sorted(df_u['Slot'].unique())
        sel_slots = st.multiselect("Filtrar Slots Críticos:", slots_opciones, default=slots_opciones)
        
        criticos = df_u[(df_u['Temp'] >= UMBRAL_CRITICO) & (df_u['Slot'].isin(sel_slots))]
        
        if not criticos.empty:
            cols = st.columns(4)
            for i, (_, r) in enumerate(criticos.sort_values('Temp', ascending=False).iterrows()):
                with cols[i % 4]:
                    st.markdown(f"""<div style="background-color:#FFC7CE; padding:15px; border-radius:10px; text-align:center; border:2px solid #9C0006; margin-bottom:10px;">
                        <h4 style="margin:0; color:#9C0006;">{r['Sitio']}</h4>
                        <h1 style="margin:5px 0; color:#9C0006;">{r['Temp']}°C</h1>
                        <small style="color:#9C0006;">SUB {r['Subrack']} | SLOT {r['Slot']}</small>
                        </div>""", unsafe_allow_html=True)
        else:
            st.success("✅ Sin alertas en slots seleccionados.")

    with t2:
        sitio = st.selectbox("Sitio (Último reporte):", sorted(df_u['Sitio'].unique()))
        st.table(df_u[df_u['Sitio'] == sitio][['Subrack', 'Slot', 'Temp', 'Timestamp']])

    with t3:
        if df_h is not None and not df_h.empty:
            s_h = st.selectbox("Sitio (Tendencia horaria):", sorted(df_h['Sitio'].unique()))
            fig = px.line(df_h[df_h['Sitio'] == s_h], x='Hora', y='Temp', color='ID_Full', markers=True)
            fig.update_xaxes(dtick="H24", tickformat="%d %b")
            st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Suba archivos .txt a la carpeta 'Temperatura' para iniciar.")
