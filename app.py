import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px
import gc

# Configuración sin parámetros obsoletos según logs
st.set_page_config(page_title="Monitor Red - Estabilidad Final", layout="wide")

FOLDER_PATH = 'Temperatura'

def extraccion_segura(path):
    """Procesa el archivo línea a línea para evitar picos de RAM."""
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
    except Exception:
        pass
    return rows

@st.cache_data(ttl=300)
def listar_archivos(folder):
    if not os.path.exists(folder): return []
    fs = [os.path.join(folder, f) for f in os.listdir(folder) if ".txt" in f]
    fs.sort(key=lambda x: "".join(re.findall(r'\d+', x)), reverse=True)
    return fs

# --- LÓGICA DE INTERFAZ ---
archivos = listar_archivos(FOLDER_PATH)

if archivos:
    st.sidebar.title("Configuración")
    if st.sidebar.button("♻️ Reiniciar Memoria"):
        st.cache_data.clear()
        st.session_state.clear()
        st.rerun()

    tab1, tab2, tab3 = st.tabs(["🚨 ALERTAS ACTUALES", "🔍 BUSCADOR", "📈 HISTÓRICO MASIVO"])

    # 1. ALERTAS (Reporte más reciente)
    with tab1:
        df_now = pd.DataFrame(extraccion_segura(archivos[0]))
        if not df_now.empty:
            st.info(f"Último Reporte: {os.path.basename(archivos[0])}")
            # Cuadros de alerta visuales
            criticos = df_now[df_now['Temp'] >= 65]
            if not criticos.empty:
                cols = st.columns(4)
                for i, (_, r) in enumerate(criticos.sort_values('Temp', ascending=False).iterrows()):
                    with cols[i % 4]:
                        st.markdown(f"""<div style="background-color:#fee2e2; border:1px solid #dc2626; padding:10px; border-radius:8px; margin-bottom:10px; text-align:center;">
                            <strong style="color:#991b1b;">{r['Sitio']}</strong><br>
                            <span style="font-size:24px; font-weight:bold; color:#dc2626;">{r['Temp']}°C</span><br>
                            <small>Slot: {r['Slot']}</small></div>""", unsafe_allow_html=True)
            else: st.success("Todo en orden.")

    # 2. BUSCADOR (Sin parámetros obsoletos)
    with tab2:
        if not df_now.empty:
            s_busq = st.selectbox("Sitio:", sorted(df_now['Sitio'].unique()))
            # Se usa width=None para evitar la advertencia del log
            st.dataframe(df_now[df_now['Sitio'] == s_busq], width=None)

    # 3. HISTÓRICO MASIVO (Optimizado para evitar error rosa)
    with tab3:
        st.subheader("Carga de Datos de Larga Duración")
        limite = st.slider("Horas a cargar:", 24, len(archivos), 168)
        
        if st.button(f"📊 Procesar {limite} Horas"):
            datos_acumulados = []
            progreso = st.progress(0)
            status = st.empty()
            
            # PROCESAMIENTO POR LOTES PARA PROTEGER RAM
            for i, p in enumerate(archivos[:limite]):
                datos_acumulados.extend(extraccion_segura(p))
                
                # Liberar memoria cada 20 archivos
                if (i + 1) % 20 == 0 or (i + 1) == limite:
                    progreso.progress((i + 1) / limite)
                    status.text(f"Cargados {i+1} de {limite} reportes...")
                    gc.collect() 

            if datos_acumulados:
                df_h = pd.DataFrame(datos_acumulados)
                df_h['Hora'] = df_h['Timestamp'].dt.floor('h')
                # Resumen para gráfico ligero
                st.session_state["h_final"] = df_h.groupby(['Hora', 'Sitio', 'ID'])['Temp'].max().reset_index()
                status.success("✅ Datos cargados correctamente.")
            else:
                status.error("No se detectaron datos. Revisa la carpeta de origen.")

        if "h_final" in st.session_state:
            df_plot = st.session_state["h_final"]
            s_sel = st.selectbox("Elegir sitio para gráfico:", sorted(df_plot['Sitio'].unique()))
            fig = px.line(df_plot[df_plot['Sitio'] == s_sel], x='Hora', y='Temp', color='ID', markers=True)
            st.plotly_chart(fig, theme="streamlit")
else:
    st.error("No se detectó la carpeta 'Temperatura'.")
