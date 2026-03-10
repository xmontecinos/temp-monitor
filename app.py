import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px
import gc

# Configuración inicial de la página
st.set_page_config(page_title="Monitor Red Pro - Recuperado", layout="wide")

# Carpeta de datos
FOLDER_PATH = 'Temperatura'

# --- MOTOR DE EXTRACCIÓN OPTIMIZADO ---
def extraer_datos_eficiente(path):
    """Lee el archivo línea por línea para no saturar la RAM."""
    rows = []
    try:
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            ts = None
            sitio = "Desconocido"
            for line in f:
                # Captura la fecha del reporte
                if not ts and "REPORT" in line:
                    m = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', line)
                    if m: ts = pd.to_datetime(f"{m.group(1)} {m.group(2)}")
                
                # Captura el nombre del sitio
                if "NE Name" in line:
                    sitio = line.split(":")[-1].strip().split()[0]
                
                # Captura datos de temperatura (Subrack, Slot, Temp)
                match = re.match(r'^\s*\d+\s+(\d+)\s+(\d+)\s+(\d+)', line)
                if match and ts:
                    rows.append({
                        "Timestamp": ts, "Sitio": sitio,
                        "ID": f"{sitio} (S:{match.group(1)}-L:{match.group(2)})",
                        "Slot": int(match.group(2)),
                        "Temp": int(match.group(3))
                    })
    except: pass
    return rows

@st.cache_data(ttl=600)
def listar_archivos(folder):
    if not os.path.exists(folder): return []
    # Detecta .txt y .gz.txt
    fs = [os.path.join(folder, f) for f in os.listdir(folder) if ".txt" in f]
    fs.sort(key=lambda x: "".join(re.findall(r'\d+', x)), reverse=True)
    return fs

# --- LÓGICA DE LA APP ---
archivos = listar_archivos(FOLDER_PATH)

if archivos:
    # Sidebar para controles globales
    st.sidebar.title("⚙️ Control de Red")
    if st.sidebar.button("♻️ Forzar Recarga Total"):
        st.cache_data.clear()
        st.session_state.clear()
        st.rerun()

    # Pestañas principales
    tab1, tab2, tab3 = st.tabs(["🚨 ALERTAS", "🔍 BUSCADOR", "📈 HISTÓRICO 200H+"])

    # 1. ALERTAS (Último reporte)
    with tab1:
        df_now = pd.DataFrame(extraer_datos_eficiente(archivos[0]))
        if not df_now.empty:
            st.subheader(f"Alertas Críticas: {os.path.basename(archivos[0])}")
            slots = sorted(df_now['Slot'].unique())
            sel_slots = st.multiselect("Filtrar Slots:", slots, default=slots)
            
            criticos = df_now[(df_now['Temp'] >= 65) & (df_now['Slot'].isin(sel_slots))]
            if not criticos.empty:
                cols = st.columns(4)
                for i, (_, r) in enumerate(criticos.sort_values('Temp', ascending=False).iterrows()):
                    with cols[i % 4]:
                        st.markdown(f"""<div style="background-color:#fee2e2; border:1px solid #dc2626; padding:10px; border-radius:8px; margin-bottom:10px; text-align:center;">
                            <strong style="color:#991b1b;">{r['Sitio']}</strong><br>
                            <span style="font-size:24px; font-weight:bold; color:#dc2626;">{r['Temp']}°C</span><br>
                            <small>Slot: {r['Slot']}</small></div>""", unsafe_allow_html=True)
            else: st.success("Todo bajo control.")

    # 2. BUSCADOR
    with tab2:
        if not df_now.empty:
            sitio_busq = st.selectbox("Buscar Sitio:", sorted(df_now['Sitio'].unique()))
            st.table(df_now[df_now['Sitio'] == sitio_busq][['Timestamp', 'ID', 'Temp']])

    # 3. HISTÓRICO (La parte pesada)
    with tab3:
        st.subheader("Tendencia de Larga Duración")
        limite = st.slider("Horas a cargar:", 24, len(archivos), 100)
        
        if st.button(f"📊 Cargar {limite} Horas"):
            all_data = []
            bar = st.progress(0)
            status = st.empty()
            
            for i, p in enumerate(archivos[:limite]):
                all_data.extend(extraer_datos_eficiente(p))
                if i % 20 == 0:
                    status.text(f"Procesando reporte {i+1} de {limite}...")
                    bar.progress((i + 1) / limite)
                    gc.collect() # Limpieza de RAM clave

            if all_data:
                df_h = pd.DataFrame(all_data)
                df_h['Hora'] = df_h['Timestamp'].dt.floor('h')
                # Guardamos en session_state para que no se borre al interactuar
                st.session_state["historico_recuperado"] = df_h.groupby(['Hora', 'Sitio', 'ID'])['Temp'].max().reset_index()
                status.success("✅ Histórico cargado con éxito.")

        if "historico_recuperado" in st.session_state:
            df_plot = st.session_state["historico_recuperado"]
            s_sel = st.selectbox("Seleccione Sitio para Gráfico:", sorted(df_plot['Sitio'].unique()))
            fig = px.line(df_plot[df_plot['Sitio'] == s_sel], x='Hora', y='Temp', color='ID', markers=True)
            st.plotly_chart(fig, use_container_width=True)
else:
    st.error("No se encontraron archivos en la carpeta 'Temperatura'.")
