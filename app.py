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
                # NEName debe ir junto según requerimiento
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
        "📊 DASHBOARD", "🚨 ALERTAS ACTUALES", "🔍 BUSCADOR", "📈 HISTÓRICO", "🚀 ANÁLISIS UPGRADE"
    ])

    # --- PESTAÑA DASHBOARD (VISTA ORIGINAL RECUPERADA) ---
    with tab_dash:
        if not df_actual.empty:
            ultima_hora = df_actual['Timestamp'].max().strftime('%d/%m/%Y %H:%M:%S')
            st.title("📊 Monitor de Salud de Red")
            
            c_info1, c_info2 = st.columns(2)
            c_info1.info(f"🕒 **Horario del Reporte:** {ultima_hora}")
            c_info2.success(f"📍 **Sitios Registrados:** {df_actual['Sitio'].nunique()}")

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
                st.subheader("🔝 Top Slots Críticos")
                res_slots = t_crit.groupby('Slot').size().reset_index(name='Cant').sort_values('Cant', ascending=False).head(10)
                res_slots['Slot_Label'] = "Slot " + res_slots['Slot'].astype(str)
                st.plotly_chart(px.bar(res_slots, x='Slot_Label', y='Cant', color='Cant', color_continuous_scale='Reds', text_auto=True), use_container_width=True)

    # --- PESTAÑA HISTÓRICO (VISTA ORIGINAL RECUPERADA) ---
    with tab_hist:
        st.subheader("📈 Gestión Histórica")
        c1, c2 = st.columns([1, 2])
        with c1:
            num_reportes = st.slider("Archivos a procesar:", 1, len(archivos_lista), min(100, len(archivos_lista)))
            if st.button("🔥 Reconstruir Base Parquet"):
                progreso = st.progress(0)
                if os.path.exists(PARQUET_FILE): os.remove(PARQUET_FILE)
                writer = None
                try:
                    for i, p in enumerate(archivos_lista[:num_reportes]):
                        progreso.progress((i + 1) / num_reportes)
                        data = extraer_datos_masivo(p)
                        if data:
                            temp_df = pd.DataFrame(data)
                            table = pa.Table.from_pandas(temp_df)
                            if writer is None: writer = pq.ParquetWriter(PARQUET_FILE, table.schema)
                            writer.write_table(table)
                    st.success("✅ Base generada.")
                    st.rerun()
                except Exception as e: st.error(f"Error: {e}")
                finally: 
                    if writer: writer.close()
        with c2:
            if os.path.exists(PARQUET_FILE):
                try:
                    tabla_sitios = pq.read_table(PARQUET_FILE, columns=['Sitio'])
                    sitios_disp = sorted(tabla_sitios.to_pandas()['Sitio'].unique())
                    sitio_h = st.selectbox("🔍 Ver Historial de Sitio:", sitios_disp)
                    if sitio_h:
                        df_h = pd.read_parquet(PARQUET_FILE, filters=[('Sitio', '==', sitio_h)])
                        ids = sorted(df_h['ID_Full'].unique())
                        # Recuperamos el comparador de slots que tenías en app (8)
                        sel_slots = st.multiselect("Slots a comparar:", ids, default=ids[:2] if ids else [])
                        if sel_slots:
                            fig_h = px.line(df_h[df_h['ID_Full'].isin(sel_slots)], x='Timestamp', y='Temp', color='ID_Full', markers=True)
                            fig_h.add_hline(y=UMBRAL_CRITICO, line_dash="dash", line_color="red")
                            st.plotly_chart(fig_h, use_container_width=True)
                except Exception as e: st.error(f"Error al cargar: {e}")

    # --- PESTAÑA UPGRADE (VISTA HORARIA NUEVA) ---
    with tab_upgrade:
        st.header("🚀 Análisis de Upgrade (Filtro Masivo)")
        if os.path.exists(PARQUET_FILE):
            subida = st.file_uploader("Sube tu lista de 93 sitios (Excel/CSV):", type=['xlsx', 'csv'])
            sitios_import = []
            if subida:
                try:
                    df_u = pd.read_csv(subida) if subida.name.endswith('.csv') else pd.read_excel(subida)
                    if 'Sitio' in df_u.columns:
                        sitios_import = df_u['Sitio'].astype(str).str.strip().unique().tolist()
                        st.success(f"✅ Se cargaron {len(sitios_import)} sitios.")
                except Exception as e: st.error(f"Error: {e}")

            todas = sorted(pq.read_table(PARQUET_FILE, columns=['Sitio']).to_pandas()['Sitio'].unique())
            sel_final = st.multiselect("Nodos a graficar:", todas, default=[s for s in sitios_import if s in todas])
            if sel_final:
                df_up = pd.read_parquet(PARQUET_FILE, filters=[('Sitio', 'in', sel_final)])
                res = df_up.groupby(['Timestamp', 'Sitio'])['Temp'].max().reset_index()
                fig_up = px.line(res, x='Timestamp', y='Temp', color='Sitio', title="Evolución Horaria por Nodo", markers=True)
                fig_up.add_hline(y=UMBRAL_CRITICO, line_dash="dash", line_color="red")
                st.plotly_chart(fig_up, use_container_width=True)
        else: st.info("Genera el historial primero.")

    # --- OTRAS PESTAÑAS ---
    with tab_alertas:
        crit_all = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
        if not crit_all.empty:
            st.error(f"⚠️ {len(crit_all)} slots críticos.")
            st.dataframe(crit_all[['Sitio', 'Slot', 'Temp']].sort_values('Temp', ascending=False), use_container_width=True)
        else: st.success("✅ Sin alertas.")

    with tab_busq:
        s_busq = st.selectbox("Buscar por Nodo:", sorted(df_actual['Sitio'].unique()))
        st.dataframe(df_actual[df_actual['Sitio'] == s_busq], use_container_width=True)

else:
    st.warning(f"⚠️ No hay archivos en '{FOLDER_PATH}'.")
