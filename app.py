import streamlit as st
import pandas as pd
import re
import os
import plotly.express as px

st.set_page_config(page_title="Monitor de Red - Alta Temperatura", layout="wide")

# --- GESTIÓN DE MEMORIA (INCREMENTAL) ---
if 'df_historico' not in st.session_state:
    st.session_state.df_historico = pd.DataFrame()
if 'archivos_leidos' not in st.session_state:
    st.session_state.archivos_leidos = set()

st.sidebar.header("🛡️ Control de Red")
# Ajustamos el umbral crítico por defecto a 65°C según tu requerimiento
UMBRAL_CRITICO = st.sidebar.number_input("Umbral Alerta Crítica (°C)", value=65)

if st.sidebar.button("♻️ Reiniciar Monitor"):
    st.session_state.df_historico = pd.DataFrame()
    st.session_state.archivos_leidos = set()
    st.cache_data.clear()
    st.rerun()

FOLDER_PATH = 'Temperatura'

def procesar_red_veloz(folder):
    if not os.path.exists(folder): return st.session_state.df_historico
    
    todos = [f for f in os.listdir(folder) if f.endswith(".txt")]
    nuevos = [f for f in todos if f not in st.session_state.archivos_leidos]
    
    if not nuevos: return st.session_state.df_historico

    nuevas_filas = []
    for nombre_f in nuevos:
        path = os.path.join(folder, nombre_f)
        try:
            with open(path, 'r', encoding='latin-1', errors='ignore') as f:
                content = f.read()
                # Captura integral: Sitio, Fecha, Hora y Tabla
                blocks = re.findall(r'NE Name:\s+([^\n\r]+).*?(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2}).*?Display Board Temperature(.*?)\n---    END', content, re.DOTALL)
                for ne_name, fecha, hora, table_text in blocks:
                    # Captura columnas: Cab, Sub, Slot, Temp
                    rows = re.findall(r'(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(?:\d+|NULL)', table_text)
                    for r in rows:
                        nuevas_filas.append({
                            "Timestamp": pd.to_datetime(f"{fecha} {hora}"),
                            "Sitio": ne_name.strip(),
                            "Subrack": r[1],
                            "Slot": int(r[2]),
                            "ID_Full": f"{ne_name.strip()} (S:{r[1]}-L:{r[2]})",
                            "Temp": int(r[3])
                        })
            st.session_state.archivos_leidos.add(nombre_f)
        except: continue

    if nuevas_filas:
        df_nuevo = pd.DataFrame(nuevas_filas)
        df_final = pd.concat([st.session_state.df_historico,
