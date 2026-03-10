import streamlit as st
import pandas as pd
import os
import re
import gc

# 1. Configuración ultra-limpia (Evita inundación de logs)
st.set_page_config(page_title="Monitor Red Estable", layout="wide")

FOLDER_PATH = 'Temperatura'

# Función de extracción optimizada para no saturar la RAM
def extraer_datos(path):
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
def obtener_lista_archivos(folder):
    if not os.path.exists(folder): return []
    # Filtra solo archivos .txt reales
    fs = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith('.txt')]
    fs.sort(reverse=True) # Los más nuevos primero
    return fs

# --- INICIO DE LA APP ---
archivos = obtener_lista_archivos(FOLDER_PATH)

if not archivos:
    st.error(f"⚠️ No se encontraron archivos en la carpeta '{FOLDER_PATH}'. Revisa tu repositorio de GitHub.")
else:
    # Sidebar de emergencia
    with st.sidebar:
        st.title("🛡️ Sistema de Control")
        if st.button("♻️ Reiniciar Conexión"):
            st.cache_data.clear()
            st.session_state.clear()
            st.rerun()
        st.write(f"Archivos detectados: {len(archivos)}")

    tab1, tab2, tab3 = st.tabs(["🚨 ALERTAS", "🔍 BUSCADOR", "📈 HISTÓRICO"])

    # PESTAÑA 1: ALERTAS (Debe aparecer sí o sí)
    with tab1:
        with st.spinner("Cargando reporte actual..."):
            df_now = pd.DataFrame(extraer_datos(archivos[0]))
        
        if not df_now.empty:
            st.subheader(f"Estado Actual: {os.path.basename(archivos[0])}")
            criticos = df_now[df_now['Temp'] >= 79]
            
            if not criticos.empty:
                cols = st.columns(4)
                for i, (_, r) in enumerate(criticos.sort_values('Temp', ascending=False).iterrows()):
                    with cols[i % 4]:
                        # HTML simple para evitar errores de renderizado
                        st.markdown(f"""<div style="background-color:#fee2e2; border:2px solid #dc2626; padding:15px; border-radius:10px; text-align:center; margin-bottom:10px;">
                            <b style="color:#991b1b; font-size:18px;">{r['Sitio']}</b><br>
                            <span style="font-size:28px; font-weight:bold; color:#dc2626;">{r['Temp']}°C</span><br>
                            <small style="color:#450a0a;">Slot: {r['Slot']}</small></div>""", unsafe_allow_html=True)
            else:
                st.success("✅ Todas las temperaturas están en niveles normales.")
        else:
            st.warning("⚠️ El archivo más reciente parece estar vacío o tiene un formato incorrecto.")

    # PESTAÑA 2: BUSCADOR
    with tab2:
        if not df_now.empty:
            sitios = sorted(df_now['Sitio'].unique())
            s_sel = st.selectbox("Selecciona un sitio para ver detalles:", sitios)
            # Nota: No usamos 'use_container_width' para evitar el error de los logs
            st.table(df_now[df_now['Sitio'] == s_sel][['ID', 'Temp']].sort_values('Temp', ascending=False))

    # PESTAÑA 3: HISTÓRICO (El punto donde se cae)
    with tab3:
        st.subheader("Análisis de Tendencia")
        horas = st.slider("Horas a analizar:", 12, min(len(archivos), 300), 72)
        
        if st.button(f"📊 Procesar {horas} Horas"):
            all_rows = []
            prog = st.progress(0)
            status_text = st.empty()
            
            # Procesamiento con limpieza de RAM forzada
            for i, p in enumerate(archivos[:horas]):
                all_rows.extend(extraer_datos(p))
                if i % 25 == 0:
                    prog.progress((i + 1) / horas)
                    status_text.text(f"Analizando: {i+1}/{horas} reportes...")
                    gc.collect() # Libera RAM inmediatamente
            
            if all_rows:
                df_h = pd.DataFrame(all_rows)
                df_h['Hora'] = df_h['Timestamp'].dt.floor('h')
                # Guardamos solo lo necesario para el gráfico
                st.session_state["h_limpio"] = df_h.groupby(['Hora', 'Sitio', 'ID'])['Temp'].max().reset_index()
                status_text.success(f"✅ ¡{horas} horas cargadas con éxito!")
            else:
                status_text.error("No se pudieron extraer datos de los archivos seleccionados.")

        if "h_limpio" in st.session_state:
            df_p = st.session_state["h_limpio"]
            sitio_gr = st.selectbox("Ver gráfico de:", sorted(df_p['Sitio'].unique()), key="gr_hist")
            fig = px.line(df_p[df_p['Sitio'] == sitio_gr], x='Hora', y='Temp', color='ID', markers=True)
            st.plotly_chart(fig, use_container_width=True)
