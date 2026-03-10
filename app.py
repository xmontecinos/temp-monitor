import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px
import gc

# Configuración con estándares actuales
st.set_page_config(page_title="Monitor Red - Estabilidad Total", layout="wide")

FOLDER_PATH = 'Temperatura'

def extraer_eficiente(path):
    """Extrae datos línea por línea para proteger la RAM."""
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

@st.cache_data(ttl=600)
def listar_archivos(folder):
    if not os.path.exists(folder): return []
    fs = [os.path.join(folder, f) for f in os.listdir(folder) if ".txt" in f]
    fs.sort(key=lambda x: "".join(re.findall(r'\d+', x)), reverse=True)
    return fs

# --- INICIO DE LA APP ---
archivos = listar_archivos(FOLDER_PATH)

if archivos:
    # Sidebar con corrección de advertencias de logs
    st.sidebar.title("Panel de Control")
    if st.sidebar.button("♻️ Limpiar Memoria y Recargar"):
        st.cache_data.clear()
        st.session_state.clear()
        st.rerun()

    tab1, tab2, tab3 = st.tabs(["🚨 ALERTA ÚLTIMA", "🔍 BUSCADOR", "📈 HISTÓRICO 7D"])

    # 1. ALERTAS
    with tab1:
        df_now = pd.DataFrame(extraer_eficiente(archivos[0]))
        if not df_now.empty:
            st.info(f"Reporte: {os.path.basename(archivos[0])}")
            criticos = df_now[df_now['Temp'] >= 65]
            if not criticos.empty:
                cols = st.columns(4)
                for i, (_, r) in enumerate(criticos.sort_values('Temp', ascending=False).iterrows()):
                    with cols[i % 4]:
                        st.markdown(f"""<div style="background-color:#fee2e2; border:1px solid #dc2626; padding:10px; border-radius:8px; margin-bottom:10px; text-align:center;">
                            <strong style="color:#991b1b;">{r['Sitio']}</strong><br>
                            <span style="font-size:24px; font-weight:bold; color:#dc2626;">{r['Temp']}°C</span><br>
                            <small>Slot: {r['Slot']}</small></div>""", unsafe_allow_html=True)
            else: st.success("✅ Temperaturas normales.")

    # 2. BUSCADOR
    with tab2:
        if not df_now.empty:
            s_busq = st.selectbox("Seleccione Sitio:", sorted(df_now['Sitio'].unique()))
            # Corrección 'use_container_width' -> 'width="stretch"' según logs
            st.dataframe(df_now[df_now['Sitio'] == s_busq], width="stretch")

    # 3. HISTÓRICO (Solución al error de carga)
    with tab3:
        st.subheader("Tendencia Semanal (Carga Inteligente)")
        horas = st.slider("Horas a procesar:", 24, len(archivos), 168)
        
        if st.button("📊 Cargar/Actualizar Gráfica"):
            all_rows = []
            progreso = st.progress(0)
            status = st.empty()
            
            for i, p in enumerate(archivos[:horas]):
                all_rows.extend(extraer_eficiente(p))
                # Limpieza de RAM cada 15 archivos para evitar crash
                if i % 15 == 0:
                    progreso.progress((i + 1) / horas)
                    status.text(f"Procesando hora {i+1}...")
                    gc.collect() 

            if all_rows:
                df_h = pd.DataFrame(all_rows)
                df_h['Hora'] = df_h['Timestamp'].dt.floor('h')
                # Resumir datos para que el gráfico no sea pesado
                st.session_state["historico_7d"] = df_h.groupby(['Hora', 'Sitio', 'ID'])['Temp'].max().reset_index()
                status.success(f"✅ ¡{horas} horas cargadas sin errores!")
            else:
                status.error("No se detectaron datos en los archivos.")

        if "historico_7d" in st.session_state:
            df_p = st.session_state["historico_7d"]
            sitio_sel = st.selectbox("Sitio para tendencia:", sorted(df_p['Sitio'].unique()))
            fig = px.line(df_p[df_p['Sitio'] == sitio_sel], x='Hora', y='Temp', color='ID', markers=True)
            st.plotly_chart(fig, theme="streamlit")

else:
    st.error("Carpeta 'Temperatura' no encontrada.")
