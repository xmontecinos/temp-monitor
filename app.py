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

    # --- PESTAÑA HISTÓRICO ---
    with tab_hist:
        st.subheader("📈 Gestión Histórica")
        c1, c2 = st.columns([1, 2])
        
        with c1:
            num_reportes = st.slider("Archivos:", 1, len(archivos_lista), min(100, len(archivos_lista)))
            if st.button("🔥 Reconstruir Base Parquet"):
                progreso = st.progress(0)
                if os.path.exists(PARQUET_FILE):
                    try: os.remove(PARQUET_FILE)
                    except: pass
                
                writer = None
                try:
                    for i, p in enumerate(archivos_lista[:num_reportes]):
                        progreso.progress((i + 1) / num_reportes)
                        data = extraer_datos_masivo(p)
                        if data:
                            df_temp = pd.DataFrame(data)
                            df_temp['Slot'] = df_temp['Slot'].astype('int16')
                            df_temp['Temp'] = df_temp['Temp'].astype('int16')
                            
                            table = pa.Table.from_pandas(df_temp)
                            if writer is None:
                                writer = pq.ParquetWriter(PARQUET_FILE, table.schema, compression='snappy')
                            writer.write_table(table)
                            
                            del df_temp, table, data
                            if i % 10 == 0: gc.collect()

                    if writer: writer.close()
                    st.success(f"✅ Base de {num_reportes} archivos lista.")
                except Exception as e:
                    st.error(f"Error: {e}")
                finally:
                    if writer: writer.close()
                    gc.collect()

        with c2:
            if os.path.exists(PARQUET_FILE):
                try:
                    df_h_menu = pq.read_table(PARQUET_FILE, columns=['Sitio']).to_pandas()
                    sitio_sel = st.selectbox("🔍 Ver Historial de:", sorted(df_h_menu['Sitio'].unique()))
                    
                    if sitio_sel:
                        df_s = pd.read_parquet(PARQUET_FILE, filters=[('Sitio', '==', sitio_sel)])
                        df_s['Timestamp'] = pd.to_datetime(df_s['Timestamp'])
                        df_s = df_s.sort_values('Timestamp')

                        ids = sorted(df_s['ID_Full'].unique())
                        sel_ids = st.multiselect("Slots a comparar:", ids, default=ids[:2] if ids else [])
                        
                        if sel_ids:
                            df_plot = df_s[df_s['ID_Full'].isin(sel_ids)].copy()
                            
                            # Limpieza de eje X para unir puntos con líneas
                            df_plot['Timestamp_Str'] = df_plot['Timestamp'].dt.strftime('%Y-%m-%d %H:%M')
                            df_plot = df_plot.drop_duplicates(subset=['Timestamp_Str', 'ID_Full'])
                            
                            fig_h = px.line(
                                df_plot, 
                                x='Timestamp_Str', 
                                y='Temp', 
                                color='ID_Full',
                                markers=True
                            )
                            
                            fig_h.update_layout(xaxis_title="Reporte", yaxis_title="Temp °C", hovermode="x unified")
                            fig_h.add_hline(y=UMBRAL_CRITICO, line_dash="dash", line_color="red")
                            
                            st.plotly_chart(fig_h, use_container_width=True)
                except Exception as e:
                    st.error(f"Error visualización: {e}")

    # --- PESTAÑA ANÁLISIS UPGRADE ---
    with tab_upgrade:
        st.header("🚀 Análisis de Upgrade")
        if os.path.exists(PARQUET_FILE):
            df_full = pd.read_parquet(PARQUET_FILE)
            tiempos = sorted(df_full['Timestamp'].unique(), reverse=True)
            
            c_u1, c_u2 = st.columns(2)
            with c_u1:
                f_up = st.file_uploader("Subir lista de sitios:", type=['xlsx', 'csv'])
            with c_u2:
                referencia = st.selectbox("🎯 Punto de Referencia:", tiempos, format_func=lambda x: pd.to_datetime(x).strftime('%Y-%m-%d %H:%M'))
                ref_ts = pd.Timestamp(referencia)

            sel_up = st.multiselect("Nodos confirmados:", sorted(df_full['Sitio'].unique()))
            
            if sel_up:
                res_up = df_full[df_full['Sitio'].isin(sel_up)].groupby(['Timestamp', 'Sitio'])['Temp'].max().reset_index()
                fig_up = px.line(res_up, x='Timestamp', y='Temp', color='Sitio', markers=True)
                fig_up.add_vline(x=ref_ts.timestamp() * 1000, line_dash="dash", line_color="orange")
                st.plotly_chart(fig_up, use_container_width=True)

    # --- PESTAÑAS RESTANTES ---
    with tab_alertas:
        st.dataframe(df_actual[df_actual['Temp'] >= UMBRAL_CRITICO][['Sitio', 'Slot', 'Temp']].sort_values('Temp', ascending=False), use_container_width=True)
    with tab_busq:
        sb = st.selectbox("Nodo:", sorted(df_actual['Sitio'].unique()), key='busq_sitio')
        st.dataframe(df_actual[df_actual['Sitio'] == sb], use_container_width=True)

else:
    st.warning("No hay archivos en la carpeta.")
