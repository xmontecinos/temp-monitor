import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px
import gc
from io import BytesIO
import pyarrow as pa
import pyarrow.parquet as pq

# 1. CONFIGURACIÓN DE PÁGINA
st.set_page_config(page_title="Monitor Red - Huawei", layout="wide")

# UMBRALES
UMBRAL_CRITICO = 78 
UMBRAL_PREVENTIVO = 65
FOLDER_PATH = 'Temperatura'
PARQUET_FILE = 'base_historica.parquet'

# --- FUNCIONES DE EXTRACCIÓN ---
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
                # NEName junto como solicitaste
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
    if not os.path.exists(folder): 
        os.makedirs(folder, exist_ok=True)
        return []
    fs = [os.path.join(folder, f) for f in os.listdir(folder) if ".txt" in f]
    fs.sort(key=lambda x: "".join(re.findall(r'\d+', x)), reverse=True)
    return fs

# --- PROCESAMIENTO INICIAL ---
archivos_lista = listar_archivos(FOLDER_PATH)

if archivos_lista:
    if "df_now" not in st.session_state:
        st.session_state["df_now"] = pd.DataFrame(extraer_datos_masivo(archivos_lista[0]))
    
    df_actual = st.session_state["df_now"]

    tab_dash, tab_alertas, tab_busq, tab_hist, tab_upgrade = st.tabs([
        "📊 DASHBOARD", "🚨 ALERTAS", "🔍 BUSCADOR", "📈 HISTÓRICO", "🚀 UPGRADE"
    ])

    # --- PESTAÑA: DASHBOARD ---
    with tab_dash:
        if not df_actual.empty:
            ultima_hora = df_actual['Timestamp'].max().strftime('%d/%m/%Y %H:%M')
            st.title("📊 Monitor de Salud de Red")
            
            c_info1, c_info2 = st.columns(2)
            c_info1.info(f"🕒 **Reporte:** {ultima_hora}")
            c_info2.success(f"📍 **Sitios:** {df_actual['Sitio'].nunique()}")

            t_crit = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
            t_prev = df_actual[(df_actual['Temp'] >= UMBRAL_PREVENTIVO) & (df_actual['Temp'] < UMBRAL_CRITICO)]
            t_ok = df_actual[df_actual['Temp'] < UMBRAL_PREVENTIVO]

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Total Tarjetas", f"{len(df_actual):,}")
            with m2:
                st.markdown(f'<div style="background-color:#fee2e2; border:2px solid #dc2626; padding:15px; border-radius:10px; text-align:center;"><h4 style="color:#991b1b; margin:0;">CRÍTICO</h4><h1 style="color:#dc2626; margin:5px 0; font-size:45px;">{len(t_crit)}</h1></div>', unsafe_allow_html=True)
            with m3:
                st.markdown(f'<div style="background-color:#fef9c3; border:2px solid #ca8a04; padding:15px; border-radius:10px; text-align:center;"><h4 style="color:#854d0e; margin:0;">PREVENTIVO</h4><h1 style="color:#ca8a04; margin:5px 0; font-size:45px;">{len(t_prev)}</h1></div>', unsafe_allow_html=True)
            with m4:
                st.markdown(f'<div style="background-color:#dcfce7; border:2px solid #16a34a; padding:15px; border-radius:10px; text-align:center;"><h4 style="color:#166534; margin:0;">ÓPTIMO</h4><h1 style="color:#166534; margin:5px 0; font-size:45px;">{len(t_ok)}</h1></div>', unsafe_allow_html=True)

            if not t_crit.empty:
                st.divider()
                st.subheader("🚨 Nodos Críticos por Slot")
                res_slots = t_crit.groupby('Slot').size().reset_index(name='Cant')
                st.plotly_chart(px.bar(res_slots, x='Slot', y='Cant', color='Cant', color_continuous_scale='Reds'), use_container_width=True)

    # --- PESTAÑA: HISTÓRICO (MOTOR DE ALTA CAPACIDAD) ---
    with tab_hist:
        st.subheader("📈 Gestión Histórica")
        c1, c2 = st.columns([1, 2])
        with c1:
            num = st.slider("Archivos:", 1, len(archivos_lista), min(300, len(archivos_lista)))
            if st.button("🔥 Reconstruir Base Parquet"):
                if os.path.exists(PARQUET_FILE): os.remove(PARQUET_FILE)
                writer = None
                p_bar = st.progress(0)
                for i, p in enumerate(archivos_lista[:num]):
                    data = extraer_datos_masivo(p)
                    if data:
                        df_tmp = pd.DataFrame(data)
                        # Optimización de tipos para ahorrar espacio
                        df_tmp['Slot'] = df_tmp['Slot'].astype('int16')
                        df_tmp['Temp'] = df_tmp['Temp'].astype('int16')
                        table = pa.Table.from_pandas(df_tmp)
                        if writer is None:
                            writer = pq.ParquetWriter(PARQUET_FILE, table.schema, compression='snappy')
                        writer.write_table(table)
                    p_bar.progress((i + 1) / num)
                    if i % 10 == 0: gc.collect() 
                if writer: writer.close()
                st.success("✅ Base reconstruida exitosamente."); st.rerun()
        
        with c2:
            if os.path.exists(PARQUET_FILE) and os.path.getsize(PARQUET_FILE) > 0:
                try:
                    df_h_m = pq.read_table(PARQUET_FILE, columns=['Sitio']).to_pandas()
                    sh = st.selectbox("Buscar Sitio:", sorted(df_h_m['Sitio'].unique()))
                    df_v = pd.read_parquet(PARQUET_FILE, filters=[('Sitio', '==', sh)])
                    fig = px.line(df_v, x='Timestamp', y='Temp', color='ID_Full', markers=True)
                    fig.add_hline(y=UMBRAL_CRITICO, line_dash="dash", line_color="red")
                    st.plotly_chart(fig, use_container_width=True)
                except: st.error("Archivo de base dañado. Presiona 'Reconstruir Base'.")

    # --- PESTAÑA: UPGRADE ---
    with tab_upgrade:
        st.header("🚀 Análisis de Upgrade")
        if os.path.exists(PARQUET_FILE):
            df_full = pd.read_parquet(PARQUET_FILE)
            tiempos = sorted(df_full['Timestamp'].unique(), reverse=True)
            cu1, cu2 = st.columns(2)
            with cu1: f_up = st.file_uploader("Subir lista sitios:", type=['xlsx', 'csv'])
            with cu2:
                ref_sel = st.selectbox("🎯 Punto Referencia:", tiempos, format_func=lambda x: pd.to_datetime(x).strftime('%Y-%m-%d %H:%M'))
            
            if f_up:
                df_l = pd.read_csv(f_up) if f_up.name.endswith('.csv') else pd.read_excel(f_up)
                sitios_up = df_l['Sitio'].astype(str).str.strip().tolist()
                res_up = df_full[df_full['Sitio'].isin(sitios_up)].groupby(['Timestamp', 'Sitio'])['Temp'].max().reset_index()
                fig_up = px.line(res_up, x='Timestamp', y='Temp', color='Sitio')
                # Parche para vline (evita el TypeError de la imagen 5a0afb)
                fig_up.add_vline(x=pd.Timestamp(ref_sel).timestamp() * 1000, line_dash="dash", line_color="orange")
                st.plotly_chart(fig_up, use_container_width=True)

    with tab_alertas:
        st.dataframe(df_actual[df_actual['Temp'] >= UMBRAL_CRITICO], use_container_width=True)
    with tab_busq:
        bus = st.selectbox("Nodo:", sorted(df_actual['Sitio'].unique()))
        st.dataframe(df_actual[df_actual['Sitio'] == bus], use_container_width=True)

else:
    st.warning("Carpeta 'Temperatura' vacía.")
