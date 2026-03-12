import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px
import gc

# 1. Configuración de página
st.set_page_config(page_title="Monitor Red Pro", layout="wide")

# UMBRALES
UMBRAL_CRITICO = 78 
UMBRAL_PREVENTIVO = 60
FOLDER_PATH = 'Temperatura'
PARQUET_FILE = 'data_historica.parquet'

# --- FUNCIONES ---
def extraer_datos_masivo(path):
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
                        "Timestamp": ts, "Sitio": nombre_sitio, "Slot": int(r[1]),
                        "Temp": int(r[2]), "ID_Full": f"{nombre_sitio} (S:{r[0]}-L:{r[1]})"
                    })
    except: pass
    return rows

@st.cache_data(ttl=60)
def listar_archivos(folder):
    if not os.path.exists(folder): return []
    fs = [os.path.join(folder, f) for f in os.listdir(folder) if ".txt" in f]
    fs.sort(key=lambda x: "".join(re.findall(r'\d+', x)), reverse=True)
    return fs

# --- PROCESAMIENTO ---
archivos_lista = listar_archivos(FOLDER_PATH)

if archivos_lista:
    if "df_now" not in st.session_state:
        st.session_state["df_now"] = pd.DataFrame(extraer_datos_masivo(archivos_lista[0]))
    
    df_actual = st.session_state["df_now"]
    tab_dash, tab_alertas, tab_busq, tab_hist = st.tabs(["📊 DASHBOARD", "🚨 ALERTAS", "🔍 BUSCADOR", "📈 HISTÓRICO"])

    with tab_dash:
        if not df_actual.empty:
            ultima_hora = df_actual['Timestamp'].max().strftime('%d/%m/%Y %H:%M:%S')
            st.title("📊 Monitor de Salud de Red")
            c1, c2 = st.columns(2)
            c1.info(f"🕒 **Reporte:** {ultima_hora}")
            c2.info(f"📍 **Sitios:** {df_actual['Sitio'].nunique()}")

            # Semáforo con jerarquía
            df_s_max = df_actual.groupby('Sitio')['Temp'].max().reset_index()
            s_crit = df_s_max[df_s_max['Temp'] >= UMBRAL_CRITICO]
            s_prev = df_s_max[(df_s_max['Temp'] >= UMBRAL_PREVENTIVO) & (df_s_max['Temp'] < UMBRAL_CRITICO)]
            s_ok = df_s_max[df_s_max['Temp'] < UMBRAL_PREVENTIVO]

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Total Tarjetas", len(df_actual))
            with m2: st.markdown(f'<div style="background-color:#fee2e2; border:2px solid #dc2626; padding:15px; border-radius:10px; text-align:center;"><h4 style="color:#991b1b; margin:0;">CRÍTICO</h4><p style="color:#dc2626; margin:0;">≥{UMBRAL_CRITICO}°C</p><h1>{len(df_actual[df_actual.Temp>=UMBRAL_CRITICO])}</h1><small>En {len(s_crit)} sitios</small></div>', unsafe_allow_html=True)
            with m3: st.markdown(f'<div style="background-color:#fef9c3; border:2px solid #ca8a04; padding:15px; border-radius:10px; text-align:center;"><h4 style="color:#854d0e; margin:0;">PREVENTIVO</h4><p style="color:#ca8a04; margin:0;">{UMBRAL_PREVENTIVO}-{UMBRAL_CRITICO-1}°C</p><h1>{len(df_actual[(df_actual.Temp>=UMBRAL_PREVENTIVO)&(df_actual.Temp<UMBRAL_CRITICO)])}</h1><small>En {len(s_prev)} sitios</small></div>', unsafe_allow_html=True)
            with m4: st.markdown(f'<div style="background-color:#dcfce7; border:2px solid #16a34a; padding:15px; border-radius:10px; text-align:center;"><h4 style="color:#166534; margin:0;">ÓPTIMO</h4><p style="color:#16a34a; margin:0;"><{UMBRAL_PREVENTIVO}°C</p><h1>{len(df_actual[df_actual.Temp<UMBRAL_PREVENTIVO])}</h1><small>En {len(s_ok)} sitios</small></div>', unsafe_allow_html=True)

    with tab_hist:
        st.subheader("📈 Gestión de Histórico (Optimizado con Parquet)")
        
        c_col1, c_col2 = st.columns(2)
        
        with c_col1:
            num_h = st.slider("Archivos TXT a procesar:", 10, min(300, len(archivos_lista)), 100)
            if st.button("🔥 Procesar TXTs y Guardar Parquet"):
                all_data = []
                bar = st.progress(0)
                msg = st.empty()
                for i, p in enumerate(archivos_lista[:num_h]):
                    msg.text(f"Leyendo: {os.path.basename(p)}")
                    all_data.extend(extraer_datos_masivo(p))
                    bar.progress((i + 1) / num_h)
                
                if all_data:
                    df_h = pd.DataFrame(all_data)
                    # Resumimos para que no pese
                    df_h = df_h.groupby([df_h['Timestamp'].dt.floor('h'), 'Sitio', 'ID_Full'])['Temp'].max().reset_index()
                    df_h.to_parquet(PARQUET_FILE, index=False)
                    st.session_state["df_full"] = df_h
                    msg.success(f"✅ ¡Guardado! {PARQUET_FILE} creado con éxito.")
                    gc.collect()

        with c_col2:
            st.write("¿Ya tienes un archivo Parquet guardado?")
            if st.button("📂 Cargar Parquet instantáneamente"):
                if os.path.exists(PARQUET_FILE):
                    st.session_state["df_full"] = pd.read_parquet(PARQUET_FILE)
                    st.success("✅ Datos cargados desde el disco en milisegundos.")
                else:
                    st.error("No se encontró el archivo Parquet. Procesa los TXT primero.")

        if "df_full" in st.session_state:
            st.divider()
            df_p = st.session_state["df_full"]
            sitio = st.selectbox("Sitio Histórico:", sorted(df_p['Sitio'].unique()))
            df_s = df_p[df_p['Sitio'] == sitio]
            sel = st.multiselect("ID:", sorted(df_s['ID_Full'].unique()), default=sorted(df_s['ID_Full'].unique()))
            if sel:
                fig = px.line(df_s[df_s['ID_Full'].isin(sel)], x='Timestamp', y='Temp', color='ID_Full', markers=True)
                st.plotly_chart(fig, use_container_width=True)
else:
    st.error("No hay archivos.")
