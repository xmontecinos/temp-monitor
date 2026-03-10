import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px
import gc

# Configuración sin parámetros obsoletos
st.set_page_config(page_title="Monitor Red - Estabilidad Máxima", layout="wide")

FOLDER_PATH = 'Temperatura'

def procesar_archivo_liviano(path):
    """Extrae datos de forma minimalista para proteger la RAM."""
    rows = []
    try:
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            ts, sitio = None, "Desconocido"
            for line in f:
                if not ts and "REPORT" in line:
                    m = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', line)
                    if m: ts = pd.to_datetime(f"{m.group(1)} {m.group(2)}")
                if "NE Name" in line:
                    sitio = line.split(":")[-1].strip().split()[0]
                
                # Captura: Subrack, Slot, Temperatura
                match = re.match(r'^\s*\d+\s+(\d+)\s+(\d+)\s+(\d+)', line)
                if match and ts:
                    rows.append({
                        "Timestamp": ts, "Sitio": sitio,
                        "ID": f"{sitio} (S:{match.group(1)}-L:{match.group(2)})",
                        "Slot": int(match.group(2)), "Temp": int(match.group(3))
                    })
    except: pass
    return rows

@st.cache_data(ttl=300)
def listar_archivos_frio(folder):
    if not os.path.exists(folder): return []
    fs = [os.path.join(folder, f) for f in os.listdir(folder) if ".txt" in f]
    fs.sort(key=lambda x: "".join(re.findall(r'\d+', x)), reverse=True)
    return fs

# --- INTERFAZ ---
archivos = listar_archivos_frio(FOLDER_PATH)

if archivos:
    st.sidebar.title("🛠️ Panel de Control")
    # Botón de reinicio completo para liberar RAM del servidor
    if st.sidebar.button("♻️ Forzar Limpieza Total"):
        st.cache_data.clear()
        st.session_state.clear()
        st.rerun()

    tab1, tab2, tab3 = st.tabs(["🚨 ALERTAS", "🔍 BUSCADOR", "📈 HISTÓRICO MASIVO"])

    # 1. ALERTAS (Reporte más reciente)
    with tab1:
        df_now = pd.DataFrame(procesar_archivo_liviano(archivos[0]))
        if not df_now.empty:
            st.info(f"Reporte Actual: {os.path.basename(archivos[0])}")
            # Cuadros de alerta (Estilo visual original)
            criticos = df_now[df_now['Temp'] >= 65]
            if not criticos.empty:
                cols = st.columns(4)
                for i, (_, r) in enumerate(criticos.sort_values('Temp', ascending=False).iterrows()):
                    with cols[i % 4]:
                        st.markdown(f"""<div style="background-color:#fee2e2; border:1px solid #dc2626; padding:10px; border-radius:8px; margin-bottom:10px; text-align:center;">
                            <strong style="color:#991b1b;">{r['Sitio']}</strong><br>
                            <span style="font-size:24px; font-weight:bold; color:#dc2626;">{r['Temp']}°C</span><br>
                            <small>Slot: {r['Slot']}</small></div>""", unsafe_allow_html=True)
            else: st.success("✅ Temperaturas bajo control.")
        else:
            st.warning("No se pudo leer el reporte más reciente.")

    # 2. BUSCADOR
    with tab2:
        if not df_now.empty:
            s_busq =
