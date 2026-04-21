import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px
import gc
import pyarrow as pa
import pyarrow.parquet as pq

# 1. CONFIGURACIÓN DE PÁGINA
st.set_page_config(page_title="Monitor Red - Huawei", layout="wide")

# UMBRALES
UMBRAL_CRITICO = 78 
UMBRAL_PREVENTIVO = 65
FOLDER_PATH = 'Temperatura'
PARQUET_FILE = 'base_historica.parquet'

# --- FUNCIONES DE EXTRACCIÓN (OPTIMIZADAS) ---
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
                # Requerimiento técnico: NEName debe ir junto
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
    except: pass
    return rows

@st.cache_data(ttl=60)
def listar_archivos(folder):
    if not os.path.exists(folder): 
        os.makedirs(folder, exist_ok=True)
        return []
    fs = [os.path.join(folder, f) for f in os.listdir(folder) if ".txt" in f]
    # Ordenar por fecha extraída del nombre
    fs.sort(key=lambda x: "".join(re.findall(r'\d+', x)), reverse=True)
    return fs

# --- INICIO PROCESAMIENTO ---
archivos_lista = listar_archivos(FOLDER_PATH)

if archivos_lista:
    if "df_now" not in st.session_state:
        st.session_state["df_now"] = pd.DataFrame(extraer_datos_masivo(archivos_lista[0]))
    
    df_actual = st.session_state["df_now"]

    tab_dash, tab_alertas, tab_busq, tab_hist, tab_upgrade = st.tabs([
        "📊 DASHBOARD", "🚨 ALERTAS ACTUALES", "🔍 BUSCADOR", "📈 HISTÓRICO", "🚀 ANÁLISIS UPGRADE"
    ])

    # --- PESTAÑA DASHBOARD (VISTA APP 8 ORIGINAL) ---
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
                st.subheader("🚨 Detalle de Sitios Críticos por Slot")
                c_df1, c_df2 = st.columns([1, 2])
                with c_df1:
                    slot_sel = st.selectbox("Filtrar Slot:", sorted(t_crit['Slot'].unique()))
                with c_df2:
                    df_slot_f = t_crit[t_crit['Slot'] == slot_sel].sort_values('Temp', ascending=False)
                    st.write(f"Viendo {len(df_slot_f)} nodos en **Slot {slot_sel}**")
                st.dataframe(df_slot_f[['Sitio', 'Temp', 'ID_Full']], use_container_width=True)

    # --- PESTAÑA HISTÓRICO (MODO SEGURO PARA >150 ARCHIVOS) ---
    with tab_hist:
        st.subheader("📈 Gestión Histórica")
        c1, c2 = st.columns([1, 2])
        with c1:
            num_reportes = st.slider("Cantidad de reportes:", 1, len(archivos_lista), min(250, len(archivos_lista)))
            if st.button("🔥 Reconstruir Base Parquet"):
                if os.path.exists(PARQUET_FILE): os.remove(PARQUET_FILE)
                writer = None
                prog = st.progress(0)
                status = st.empty()
                try:
                    for i, p in enumerate(archivos_lista[:num_reportes]):
                        if i % 10 == 0:
                            prog.progress((i + 1) / num_reportes)
                            status.text(f"Procesando {i+1} de {num_reportes}...")
                        
                        data = extraer_datos_masivo(p)
                        if data:
                            table = pa.Table.from_pandas(pd.DataFrame(data))
                            if writer is None: 
                                writer = pq.ParquetWriter(PARQUET_FILE, table.schema, compression='snappy')
                            writer.write_table(table)
                        
                        if i % 50 == 0: gc.collect() # Vaciar RAM
                    
                    if writer: writer.close()
                    st.success("✅ Base reconstruida con éxito.")
                    st.rerun()
                except Exception as e: st.error(f"Error: {e}")
        with c2:
            if os.path.exists(PARQUET_FILE):
                df_menu = pq.read_table(PARQUET_FILE, columns=['Sitio']).to_pandas()
                sitio_h = st.selectbox("🔍 Historial de:", sorted(df_menu['Sitio'].unique()))
                if sitio_h:
                    df_v = pd.read_parquet(PARQUET_FILE, filters=[('Sitio', '==', sitio_h)])
                    ids = sorted(df_v['ID_Full'].unique())
                    sel_ids = st.multiselect("Comparar slots:", ids, default=ids[:2] if ids else [])
                    if sel_ids:
                        fig_h = px.line(df_v[df_v['ID_Full'].isin(sel_ids)], x='Timestamp', y='Temp', color='ID_Full', markers=True)
                        fig_h.add_hline(y=UMBRAL_CRITICO, line_dash="dash", line_color="red")
                        st.plotly_chart(fig_h, use_container_width=True)

    # --- PESTAÑA ANÁLISIS UPGRADE (REPARADA) ---
    with tab_upgrade:
        st.header("🚀 Análisis de Upgrade")
        if os.path.exists(PARQUET_FILE):
            df_full = pd.read_parquet(PARQUET_FILE)
            tiempos = sorted(df_full['Timestamp'].unique(), reverse=True)
            
            c_up1, c_up2 = st.columns(2)
            with c_up1:
                f_up = st.file_uploader("Subir lista de sitios:", type=['xlsx', 'csv'])
            with c_up2:
                # Selector de tiempo para el Jueves 16 15:00
                referencia = st.selectbox("🎯 Punto Comparación (Antes):", tiempos, format_func=lambda x: x.strftime('%Y-%m-%d %H:%M'))
                ref_ts = pd.Timestamp(referencia)

            sitios_import = []
            if f_up:
                try:
                    df_l = pd.read_csv(f_up) if f_up.name.endswith('.csv') else pd.read_excel(f_up)
                    sitios_import = df_l['Sitio'].astype(str).str.strip().unique().tolist()
                except: st.error("Error al leer Excel/CSV")

            nodos = sorted(df_full['Sitio'].unique())
            sel_up = st.multiselect("Nodos confirmados:", nodos, default=[s for s in sitios_import if s in nodos])
            
            if sel_up:
                res_up = df_full[df_full['Sitio'].isin(sel_up)].groupby(['Timestamp', 'Sitio'])['Temp'].max().reset_index()
                fig_up = px.line(res_up, x='Timestamp', y='Temp', color='Sitio', markers=True)
                # Línea naranja del "Antes" corregida para evitar TypeError
                fig_up.add_vline(x=ref_ts.timestamp() * 1000, line_dash="dash", line_color="orange")
                st.plotly_chart(fig_up, use_container_width=True)

                st.divider()
                st.subheader(f"📉 Mejora Térmica desde {ref_ts.strftime('%d/%m %H:%M')}")
                df_ref = res_up[res_up['Timestamp'] == ref_ts][['Sitio', 'Temp']].rename(columns={'Temp': 'T_Antes'})
                df_now = res_up[res_up['Timestamp'] == res_up['Timestamp'].max()][['Sitio', 'Temp']].rename(columns={'Temp': 'T_Ahora'})
                df_delta = pd.merge(df_ref, df_now, on='Sitio')
                df_delta['Mejora'] = df_delta['T_Antes'] - df_delta['T_Ahora']
                
                bajan_10 = df_delta[df_delta['Mejora'] >= 10].sort_values('Mejora', ascending=False)
                if not bajan_10.empty:
                    st.success(f"Nodos con baja significativa (>10°C): {len(bajan_10)}")
                    st.dataframe(bajan_10, use_container_width=True, hide_index=True)
                else: st.info("No hay bajas > 10°C respecto a esa hora.")
        else: st.info("Genera el historial primero.")

    # --- PESTAÑAS EXTRAS ---
    with tab_alertas:
        st.dataframe(df_actual[df_actual['Temp'] >= UMBRAL_CRITICO][['Sitio', 'Slot', 'Temp']].sort_values('Temp', ascending=False), use_container_width=True)
    with tab_busq:
        sb = st.selectbox("Nodo:", sorted(df_actual['Sitio'].unique()))
        st.dataframe(df_actual[df_actual['Sitio'] == sb], use_container_width=True)

else:
    st.warning("No hay archivos en la carpeta 'Temperatura'.")
