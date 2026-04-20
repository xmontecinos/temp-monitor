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
                # NEName debe ir junto según requerimiento técnico
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

    # --- PESTAÑA DASHBOARD ---
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
                c_f1, c_f2 = st.columns([1, 2])
                with c_f1:
                    slot_s = st.selectbox("Slot:", sorted(t_crit['Slot'].unique()))
                with c_f2:
                    df_s = t_crit[t_crit['Slot'] == slot_s].sort_values('Temp', ascending=False)
                    st.write(f"Sitios en **Slot {slot_s}**")
                st.dataframe(df_s[['Sitio', 'Temp', 'ID_Full']], use_container_width=True)

    # --- PESTAÑA HISTÓRICO ---
    with tab_hist:
        st.subheader("📈 Gestión Histórica")
        c1, c2 = st.columns([1, 2])
        with c1:
            num_reportes = st.slider("Archivos:", 1, len(archivos_lista), min(100, len(archivos_lista)))
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
                sitios_disp = sorted(pq.read_table(PARQUET_FILE, columns=['Sitio']).to_pandas()['Sitio'].unique())
                sitio_h = st.selectbox("🔍 Historial Individual:", sitios_disp)
                if sitio_h:
                    df_h = pd.read_parquet(PARQUET_FILE, filters=[('Sitio', '==', sitio_h)])
                    ids = sorted(df_h['ID_Full'].unique())
                    sel_slots = st.multiselect("Comparar slots:", ids, default=ids[:2] if ids else [])
                    if sel_slots:
                        fig_h = px.line(df_h[df_h['ID_Full'].isin(sel_slots)], x='Timestamp', y='Temp', color='ID_Full', markers=True)
                        st.plotly_chart(fig_h, use_container_width=True)

    # --- PESTAÑA UPGRADE (CON COMPARACIÓN JUEVES 16 15:00) ---
    with tab_upgrade:
        st.header("🚀 Análisis de Upgrade (Comparativa Temporal)")
        if os.path.exists(PARQUET_FILE):
            # Cargar Base Completa para este análisis
            df_hist_full = pd.read_parquet(PARQUET_FILE)
            tiempos_disp = sorted(df_hist_full['Timestamp'].unique(), reverse=True)
            
            c_up1, c_up2 = st.columns(2)
            with c_up1:
                subida = st.file_uploader("Sube lista de 93 sitios:", type=['xlsx', 'csv'])
            with c_up2:
                # SELECTOR PARA EL "ANTES" (Jueves 16 15:00)
                st.write("🎯 **Punto de Referencia (El 'Antes'):**")
                referencia = st.selectbox("Selecciona Fecha/Hora de comparación:", tiempos_disp, 
                                          help="Busca aquí el Jueves 16 a las 15:00")

            sitios_import = []
            if subida:
                try:
                    df_u = pd.read_csv(subida) if subida.name.endswith('.csv') else pd.read_excel(subida)
                    if 'Sitio' in df_u.columns:
                        sitios_import = df_u['Sitio'].astype(str).str.strip().unique().tolist()
                        st.success(f"✅ {len(sitios_import)} sitios cargados.")
                except Exception as e: st.error(f"Error: {e}")

            todas = sorted(df_hist_full['Sitio'].unique())
            sel_final = st.multiselect("Filtrar Nodos:", todas, default=[s for s in sitios_import if s in todas])
            
            if sel_final:
                df_up = df_hist_full[df_hist_full['Sitio'].isin(sel_final)]
                res = df_up.groupby(['Timestamp', 'Sitio'])['Temp'].max().reset_index()
                
                fig_up = px.line(res, x='Timestamp', y='Temp', color='Sitio', title="Evolución Horaria", markers=True)
                fig_up.add_vline(x=referencia, line_dash="dot", line_color="orange", annotation_text="Punto Referencia")
                st.plotly_chart(fig_up, use_container_width=True)

                # --- CÁLCULO DE MEJORA CONTRA REFERENCIA ---
                st.divider()
                st.subheader(f"📉 Mejora respecto a: {referencia}")
                
                # Datos en el momento de referencia
                df_ref = res[res['Timestamp'] == referencia][['Sitio', 'Temp']].rename(columns={'Temp': 'T_Referencia'})
                # Datos actuales (último reporte)
                ultimo_ts = res['Timestamp'].max()
                df_now_up = res[res['Timestamp'] == ultimo_ts][['Sitio', 'Temp']].rename(columns={'Temp': 'T_Actual'})
                
                # Unir y calcular
                df_comparativa = pd.merge(df_ref, df_now_up, on='Sitio')
                df_comparativa['Mejora'] = df_comparativa['T_Referencia'] - df_comparativa['T_Actual']
                
                # Filtrar los de 10 grados o más
                df_baja_10 = df_comparativa[df_comparativa['Mejora'] >= 10].sort_values('Mejora', ascending=False)

                col_res1, col_res2 = st.columns(2)
                col_res1.metric("Sitios con baja > 10°C", len(df_baja_10))
                
                if not df_baja_10.empty:
                    st.success(f"Listado de sitios que bajaron 10°C o más comparado con el jueves 16:")
                    st.dataframe(df_baja_10, use_container_width=True)
                else:
                    st.info("No se detectan bajas superiores a 10°C respecto a esa hora.")

        else: st.info("Genera el historial primero.")

    # --- OTRAS PESTAÑAS ---
    with tab_alertas:
        crit_all = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
        if not crit_all.empty:
            st.error(f"⚠️ {len(crit_all)} registros críticos.")
            st.dataframe(crit_all[['Sitio', 'Slot', 'Temp']].sort_values('Temp', ascending=False), use_container_width=True)
        else: st.success("✅ Sin alertas.")

    with tab_busq:
        s_busq = st.selectbox("Buscar Nodo:", sorted(df_actual['Sitio'].unique()))
        st.dataframe(df_actual[df_actual['Sitio'] == s_busq], use_container_width=True)

else:
    st.warning(f"⚠️ Carpeta '{FOLDER_PATH}' vacía.")
