import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px
import gc

st.set_page_config(page_title="Monitor Red - Recuperación Total", layout="wide")

# --- CONFIGURACIÓN ---
FOLDER_PATH = 'Temperatura'

def extraer_datos_ultra_estable(path):
    """Extrae datos usando el mínimo de RAM posible."""
    rows = []
    try:
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            ts = None
            sitio = "Desconocido"
            for line in f:
                # 1. Buscar Fecha
                if not ts and "REPORT" in line:
                    m = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', line)
                    if m: ts = pd.to_datetime(f"{m.group(1)} {m.group(2)}")
                
                # 2. Buscar Sitio
                if "NE Name" in line:
                    sitio = line.split(":")[-1].strip().split()[0]
                
                # 3. Buscar Filas de Temperatura (Sub Slot Temp)
                match = re.match(r'^\s*\d+\s+(\d+)\s+(\d+)\s+(\d+)', line)
                if match and ts:
                    rows.append({
                        "Timestamp": ts, 
                        "Sitio": sitio,
                        "ID": f"{sitio} (S:{match.group(1)}-L:{match.group(2)})",
                        "Slot": int(match.group(2)),
                        "Temp": int(match.group(3))
                    })
    except: pass
    return rows

@st.cache_data(ttl=300)
def obtener_archivos(folder):
    if not os.path.exists(folder): return []
    # Busca .txt y .gz.txt
    fs = [os.path.join(folder, f) for f in os.listdir(folder) if ".txt" in f]
    fs.sort(key=lambda x: "".join(re.findall(r'\d+', x)), reverse=True)
    return fs

# --- LÓGICA PRINCIPAL ---
archivos = obtener_archivos(FOLDER_PATH)

if archivos:
    st.sidebar.header("🛡️ Panel de Control")
    if st.sidebar.button("♻️ Reiniciar App (Limpiar Todo)"):
        st.cache_data.clear()
        st.session_state.clear()
        st.rerun()

    tab1, tab2, tab3 = st.tabs(["🚨 ALERTAS", "🔍 BUSCADOR", "📈 HISTÓRICO +200H"])

    # --- PESTAÑA 1: ALERTAS ---
    with tab1:
        df_now = pd.DataFrame(extraer_datos_ultra_estable(archivos[0]))
        if not df_now.empty:
            st.info(f"Último reporte: {os.path.basename(archivos[0])}")
            slots = sorted(df_now['Slot'].unique())
            sel_s = st.multiselect("Filtrar Slots:", slots, default=slots)
            
            criticos = df_now[(df_now['Temp'] >= 65) & (df_now['Slot'].isin(sel_s))]
            if not criticos.empty:
                cols = st.columns(4)
                for i, (_, r) in enumerate(criticos.sort_values('Temp', ascending=False).iterrows()):
                    with cols[i % 4]:
                        st.markdown(f"""<div style="background-color:#fee2e2; border:1px solid #dc2626; padding:10px; border-radius:8px; margin-bottom:10px; text-align:center;">
                            <strong style="color:#991b1b;">{r['Sitio']}</strong><br>
                            <span style="font-size:24px; font-weight:bold; color:#dc2626;">{r['Temp']}°C</span><br>
                            <small>Slot: {r['Slot']}</small></div>""", unsafe_allow_html=True)
            else: st.success("✅ No hay temperaturas críticas.")

    # --- PESTAÑA 2: BUSCADOR ---
    with tab2:
        if not df_now.empty:
            s_busq = st.selectbox("Seleccione Sitio:", sorted(df_now['Sitio'].unique()))
            st.dataframe(df_now[df_now['Site'] == s_busq] if 'Site' in df_now else df_now[df_now['Sitio'] == s_busq], use_container_width=True)

    # --- PESTAÑA 3: HISTÓRICO ---
    with tab3:
        limite = st.slider("Horas a cargar:", 10, len(archivos), 100)
        if st.button(f"📊 Procesar {limite} Horas"):
            all_rows = []
            prog = st.progress(0)
            msg = st.empty()
            
            for i, p in enumerate(archivos[:limite]):
                all_rows.extend(extraer_datos_ultra_estable(p))
                if i % 10 == 0:
                    prog.progress((i + 1) / limite)
                    msg.text(f"Cargando reporte {i+1}...")
                    gc.collect() # LIBERACIÓN DE RAM CRÍTICA
            
            if all_rows:
                df_h = pd.DataFrame(all_rows)
                # Agrupamos por hora para que el gráfico sea ligero
                df_h['Hora'] = df_h['Timestamp'].dt.floor('h')
                st.session_state["historico_ok"] = df_h.groupby(['Hora', 'Sitio', 'ID'])['Temp'].max().reset_index()
                msg.success(f"✅ ¡{limite} horas cargadas!")
            else:
                msg.error("No se pudo extraer información. Revisa el formato de los archivos.")

        if "historico_ok" in st.session_state:
            df_p = st.session_state["historico_ok"]
            s_sel = st.selectbox("Sitio para Gráfico:", sorted(df_p['Sitio'].unique()), key="graph")
            fig = px.line(df_p[df_p['Sitio'] == s_sel], x='Hora', y='Temp', color='ID', markers=True)
            st.plotly_chart(fig, use_container_width=True)
else:
    st.error("No se encontró la carpeta 'Temperatura' o está vacía.")
