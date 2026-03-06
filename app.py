import streamlit as st
import pandas as pd
import re
import os
import plotly.express as px
from datetime import datetime, timedelta

st.set_page_config(page_title="Monitor Red Ultra Fast", layout="wide")

# --- CONFIGURACIÓN ---
UMBRAL_CRITICO = 79 
FOLDER_PATH = 'Temperatura'

st.sidebar.header("🛡️ Control de Red")
if st.sidebar.button("♻️ Forzar Recarga Total"):
    st.cache_data.clear()
    st.rerun()

def extraer_rapido(path):
    """Extrae datos de un solo archivo de forma eficiente."""
    try:
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            content = f.read()
            blocks = re.findall(r'NE Name:\s+([^\n\r]+).*?(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2}).*?Display Board Temperature(.*?)\n---    END', content, re.DOTALL)
            data = []
            for ne_name, fecha, hora, table_text in blocks:
                rows = re.findall(r'(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(?:\d+|NULL)', table_text)
                for r in rows:
                    data.append({
                        "Timestamp": pd.to_datetime(f"{fecha} {hora}"),
                        "Sitio": ne_name.strip(),
                        "Subrack": r[1],
                        "Slot": int(r[2]),
                        "Temp": int(r[3]),
                        "ID_Full": f"{ne_name.strip()} (S:{r[1]}-L:{r[2]})"
                    })
            return data
    except:
        return []

@st.cache_data(ttl=120) # Cache de 2 minutos
def cargar_todo_optimizado(folder):
    if not os.path.exists(folder): return None, None
    
    # Obtener lista de archivos con su fecha de modificación sin abrirlos
    archivos = []
    for f in os.listdir(folder):
        if f.endswith(".txt"):
            full_path = os.path.join(folder, f)
            archivos.append((full_path, os.path.getmtime(full_path)))
    
    if not archivos: return None, None
    
    # Ordenar por tiempo (el más nuevo primero)
    archivos.sort(key=lambda x: x[1], reverse=True)

    # 1. ULTIMO REPORTE (Carga instantánea: 1 solo archivo)
    df_ultima = pd.DataFrame(extraer_rapido(archivos[0][0]))

    # 2. HISTORICO 7 DIAS (Procesamiento limitado para evitar bloqueos)
    data_hist = []
    limite_7d = datetime.now() - timedelta(days=7)
    
    # Leemos solo los últimos 25 archivos para el histórico
    # Esto es suficiente para tendencias y no mata al servidor
    for path, mtime in archivos[:25]:
        if datetime.fromtimestamp(mtime) < limite_7d: break
        data_hist.extend(extraer_rapido(path))
    
    df_hist = pd.DataFrame(data_hist)
    if not df_hist.empty:
        df_hist['Hora'] = df_hist['Timestamp'].dt.floor('h')
        df_hist = df_hist.groupby(['Hora', 'Sitio', 'ID_Full', 'Slot'])['Temp'].max().reset_index()

    return df_ultima, df_hist

# --- INTERFAZ ---
df_u, df_h = cargar_todo_optimizado(FOLDER_PATH)

if df_u is not None and not df_u.empty:
    t1, t2, t3 = st.tabs(["🚨 ALERTA ÚLTIMA", "📍 BUSCADOR", "📈 HISTÓRICO 7D"])

    with t1:
        st.subheader(f"Estado Crítico (Último reporte)")
        # Selección de Slots para filtrar la alerta
        slots_en_alerta = sorted(df_u['Slot'].unique())
        sel_slots = st.multiselect("Ver Slots específicos:", slots_en_alerta, default=slots_en_alerta)
        
        criticos = df_u[(df_u['Temp'] >= UMBRAL_CRITICO) & (df_u['Slot'].isin(sel_slots))]
        
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
            st.success("✅ Todo normal en los slots seleccionados.")

    with t2:
        sitio = st.selectbox("Buscar Sitio (Último reporte):", sorted(df_u['Sitio'].unique()))
        st.dataframe(df_u[df_u['Sitio'] == sitio][['Subrack', 'Slot', 'Temp', 'Timestamp']], use_container_width=True, hide_index=True)

    with t3:
        if df_h is not None and not df_h.empty:
            sitio_h = st.selectbox("Histórico 7 días (Por hora):", sorted(df_h['Sitio'].unique()))
            fig = px.line(df_h[df_h['Sitio'] == sitio_h], x='Hora', y='Temp', color='ID_Full', markers=True)
            fig.update_layout(xaxis_title="Día / Hora", yaxis_title="Temp °C")
            st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("No se encontraron archivos en la carpeta 'Temperatura'.")
