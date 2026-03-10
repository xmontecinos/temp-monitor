import streamlit as st
import pandas as pd
import re
import os
import plotly.express as px
import gc

st.set_page_config(page_title="Monitor Red - 100h Estable", layout="wide")

# --- CONFIGURACIÓN ---
UMBRAL_CRITICO = 65 
FOLDER_PATH = 'Temperatura'

st.sidebar.header("🛡️ Control de Red")
if st.sidebar.button("♻️ Forzar Recarga Total"):
    st.cache_data.clear()
    st.session_state.clear()
    st.rerun()

def extraer_solo_temperaturas(path):
    """Extrae datos de forma ultra-ligera línea por línea."""
    data = []
    try:
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            ne_name = "Sitio"
            timestamp = None
            for line in f:
                if "NE Name:" in line:
                    ne_name = line.split(":")[-1].strip()
                if "---    REPORT" in line:
                    m = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', line)
                    if m: timestamp = pd.to_datetime(f"{m.group(1)} {m.group(2)}")
                
                # Solo procesar líneas que parecen datos de board (Sub Slot Temp)
                row = re.match(r'^\s*(\d+)\s+(\d+)\s+(\d+)\s+(\d+)', line)
                if row and timestamp:
                    data.append({
                        "Timestamp": timestamp,
                        "Sitio": ne_name,
                        "Subrack": row.group(2),
                        "Slot": int(row.group(3)),
                        "Temp": int(row.group(4)),
                        "ID_Full": f"{ne_name} (S:{row.group(2)}-L:{row.group(3)})"
                    })
    except: pass
    return data

@st.cache_data(ttl=60)
def listar_archivos(folder):
    if not os.path.exists(folder): return []
    files = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".txt")]
    files.sort(key=lambda x: "".join(re.findall(r'\d+', x)), reverse=True)
    return files

# --- FLUJO ---
archivos = listar_archivos(FOLDER_PATH)

if archivos:
    # Carga rápida para Alertas
    if "df_now" not in st.session_state:
        st.session_state["df_now"] = pd.DataFrame(extraer_solo_temperaturas(archivos[0]))
    
    df_actual = st.session_state["df_now"]
    tab1, tab2, tab3 = st.tabs(["🚨 ALERTA ÚLTIMA", "📍 BUSCADOR", "📈 HISTÓRICO 100H"])

    with tab1:
        st.subheader(f"Reporte: {os.path.basename(archivos[0])}")
        if not df_actual.empty:
            slots = sorted(df_actual['Slot'].unique())
            sel_s = st.multiselect("Filtrar Slots:", slots, default=slots)
            criticos = df_actual[(df_actual['Temp'] >= UMBRAL_CRITICO) & (df_actual['Slot'].isin(sel_s))]
            
            if not criticos.empty:
                cols = st.columns(4)
                for i, (_, r) in enumerate(criticos.sort_values('Temp', ascending=False).iterrows()):
                    with cols[i % 4]:
                        st.markdown(f"""<div style="background-color:#FFC7CE; padding:10px; border-radius:10px; border:2px solid #9C0006; margin-bottom:10px; text-align:center;">
                            <b style="color:#9C0006;">{r['Sitio']}</b><br><span style="font-size:24px; color:#9C0006;">{r['Temp']}°C</span><br>
                            <small style="color:#9C0006;">S:{r['Subrack']} | L:{r['Slot']}</small></div>""", unsafe_allow_html=True)
            else: st.success("✅ Todo normal.")

    with tab2:
        if not df_actual.empty:
            sitio = st.selectbox("Sitio:", sorted(df_actual['Sitio'].unique()))
            st.dataframe(df_actual[df_actual['Sitio'] == sitio], use_container_width=True)

    with tab3:
        st.subheader("Tendencia 100 Horas")
        if st.button("📊 Generar Histórico"):
            all_rows = []
            prog = st.progress(0)
            # Procesar últimos 100 archivos (aprox 4 días)
            limite = archivos[:100] 
            for i, f in enumerate(limite):
                all_rows.extend(extraer_solo_temperaturas(f))
                prog.progress((i + 1) / len(limite))
                if i % 25 == 0: gc.collect() # Limpiar RAM agresivamente
            
            df_h = pd.DataFrame(all_rows)
            if not df_h.empty:
                # Agrupar por hora para que el gráfico no sea pesado
                df_h['Hora'] = df_h['Timestamp'].dt.floor('h')
                st.session_state["df_100"] = df_h.groupby(['Hora', 'Sitio', 'ID_Full'])['Temp'].max().reset_index()
                st.success("✅ Datos cargados.")
            else: st.error("Sin datos.")

        if "df_100" in st.session_state:
            df_p = st.session_state["df_100"]
            s_h = st.selectbox("Elegir Sitio:", sorted(df_p['Sitio'].unique()), key="h1")
            fig = px.line(df_p[df_p['Sitio'] == s_h], x='Hora', y='Temp', color='ID_Full', markers=True)
            st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Esperando archivos...")
