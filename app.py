import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px
import gc

# 1. Configuración de página
st.set_page_config(page_title="Monitor Red - Full Histórico", layout="wide")

UMBRAL_CRITICO = 65 
UMBRAL_PREVENTIVO = 55
FOLDER_PATH = 'Temperatura'

# --- FUNCIONES DE EXTRACCIÓN ---
def extraer_datos_masivo(path):
    """Escaneo profundo por bloques de sitio."""
    rows = []
    try:
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            content = f.read()
            
            ts_match = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', content)
            if not ts_match: return []
            ts = pd.to_datetime(f"{ts_match.group(1)} {ts_match.group(2)}")

            bloques = re.split(r'NE Name\s*:\s*', content)
            
            for bloque in bloques[1:]:
                lineas = bloque.split('\n')
                if not lineas: continue
                nombre_sitio = lineas[0].strip().split()[0]
                
                filas = re.findall(r'^\s*\d+\s+(\d+)\s+(\d+)\s+(\d+)', bloque, re.MULTILINE)
                
                for r in filas:
                    rows.append({
                        "Timestamp": ts,
                        "Sitio": nombre_sitio,
                        "Slot": int(r[1]),
                        "Temp": int(r[2]),
                        "ID_Full": f"{nombre_sitio} (S:{r[0]}-L:{r[1]})"
                    })
    except Exception: pass
    return rows

@st.cache_data(ttl=60)
def listar_archivos(folder):
    if not os.path.exists(folder): return []
    fs = [os.path.join(folder, f) for f in os.listdir(folder) if ".txt" in f]
    fs.sort(key=lambda x: "".join(re.findall(r'\d+', x)), reverse=True)
    return fs

# --- LÓGICA DE NAVEGACIÓN ---
if "active_tab" not in st.session_state:
    st.session_state["active_tab"] = "📊 DASHBOARD"

def cambiar_tab(nombre_tab):
    st.session_state["active_tab"] = nombre_tab
    st.rerun()

# --- INTERFAZ ---
archivos_lista = listar_archivos(FOLDER_PATH)

if archivos_lista:
    # Sidebar: Control de navegación y RAM
    with st.sidebar:
        st.title("🕹️ Navegación")
        seleccion_sidebar = st.radio("Ir a:", 
                                   ["📊 DASHBOARD", "🚨 ALERTAS ACTUALES", "🔍 BUSCADOR", "📈 HISTÓRICO"],
                                   index=["📊 DASHBOARD", "🚨 ALERTAS ACTUALES", "🔍 BUSCADOR", "📈 HISTÓRICO"].index(st.session_state["active_tab"]))
