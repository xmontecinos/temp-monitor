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
                # NEName debe ir junto según el ledger
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

def to_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Datos')
    return output.getvalue()

# --- PROCESAMIENTO INICIAL ---
archivos_lista = listar_archivos(FOLDER_PATH)

if archivos_lista:
    if "df_now" not in st.session_state:
        st.session_state["df_now"] = pd.DataFrame(extraer_datos_masivo(archivos_lista[0]))
    
    df_actual = st.session_state["df_now"]

    # Pestañas corregidas (sin errores de indentación)
    tab_dash, tab_alertas, tab_busq, tab_hist, tab_upgrade = st.tabs([
        "📊 DASHBOARD", "🚨 ALERTAS ACTUALES", "🔍 BUSCADOR", "📈 HISTÓRICO", "🚀 ANÁLISIS UPGRADE"
    ])

    with tab_dash:
        if not df_actual.empty:
            ultima_hora = df_actual['Timestamp'].max().strftime('%d/%m/%Y %H:%M:%S')
            total_sitios_red = df_actual['Sitio'].nunique()
            st.title("📊 Monitor de Salud de Red")
            c_info1, c_info2 = st.columns(2)
            c_info1.info(f"🕒 **Último Reporte:** {ultima_hora}")
            c_info2.success(f"📍 **Sitios en Red:** {total_sitios_red}")

            df_sitios_max = df_actual.groupby('Sitio')['Temp'].max().reset_index()
            t_crit = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
            t_prev = df_actual[(df_actual['Temp'] >= UMBRAL_PREVENTIVO) & (df_actual['Temp'] < UMBRAL_CRITICO)]
            t_ok = df_actual[df_actual['Temp'] < UMBRAL_PREVENTIVO]

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Total Tarjetas", f"{len(df_actual):,}")
            with m2: st.markdown(f'<div style="background-color:#fee2e2; border:2px solid #dc2626; padding:10px; border-radius:10px; text-align:center;"><h4 style="color:#991b1b; margin:0;">CRÍTICO</h4><h1 style="color:#dc2626; margin:5px 0;">{len(t_crit)}</h1></div>', unsafe_allow_html=True)
            with m3: st.markdown(f'<div style="background-color:#fef9c3; border:2px solid #ca8a04; padding:10px; border-radius:10px; text-align:center;"><h4 style="color:#854d0e; margin:0;">PREVENTIVO</h4><h1 style="color:#ca8a04; margin:5px 0;">{len(t_prev)}</h1></div>', unsafe_allow_html=True)
            with m4: st.markdown(f'<div style="background-color:#dcfce7; border:2px solid #16a34a; padding:10px; border-radius:10px; text-align:center;"><h4 style="color:#166534; margin:0;">ÓPTIMO</h4><h1 style="color:#166534; margin:5px 0;">{len(t_ok)}</h1></div>', unsafe_allow_html=True)

    with tab_alertas:
        crit_all = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
        if not crit_all.empty:
            st.error(f"⚠️ {len(crit_all)} slots críticos.")
            st.dataframe(crit_all[['Sitio', 'Slot', 'Temp']].sort_values('Temp', ascending=False), width=None)
        else: st.success("✅ Todo bajo control.")

    with tab_busq:
        s_busq = st.selectbox("Buscar por Sitio:", sorted(df_actual['Sitio'].unique()))
        st.dataframe(df_actual[df_actual['Sitio'] == s_busq], width=None)

    with tab_hist:
        st.subheader("📈 Gestión de Base de Datos")
        c1, c2 = st.columns(2)
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
                            temp_df['Slot'] = temp_df['Slot'].astype('int16')
                            temp_df['Temp'] = temp_df['Temp'].astype('int16')
                            table = pa.Table.from_pandas(temp_df)
                            if writer is None: writer = pq.ParquetWriter(PARQUET_FILE, table.schema)
                            writer.write_table(table)
                        if i % 50 == 0: gc.collect()
                    st.success("✅ Base generada.")
                    st.rerun()
                except Exception as e: st.error(f"Error: {e}")
                finally: 
                    if writer: writer.close()
        with c2:
            if os.path.exists(PARQUET_FILE):
                sitios_disp = sorted(pq.read_table(PARQUET_FILE, columns=['Sitio']).to_pandas()['Sitio'].unique())
                s_h = st.selectbox("Historial de Sitio:", sitios_disp)
                if s_h:
                    df_h = pd.read_parquet(PARQUET_FILE, filters=[('Sitio', '==', s_h)])
                    st.plotly_chart(px.line(df_h, x='Timestamp', y='Temp', color='ID_Full', title=f"Tendencia: {s_h}"), width=None)

    with tab_upgrade:
        st.header("🚀 Análisis de Upgrade (Visión Horaria)")
        if os.path.exists(PARQUET_FILE):
            st.write("Sube un Excel/CSV con una columna llamada **'Sitio'** para cargar los 93 nodos.")
            subida = st.file_uploader("Cargar lista de sitios:", type=['xlsx', 'csv'])
            
            sitios_importados = []
            if subida:
                try:
                    # Usar engine='openpyxl' para evitar el error de la imagen
                    if subida.name.endswith('.csv'):
                        df_u = pd.read_csv(subida)
                    else:
                        df_u = pd.read_excel(subida, engine='openpyxl')
                    
                    if 'Sitio' in df_u.columns:
                        sitios_importados = df_u['Sitio'].astype(str).str.strip().unique().tolist()
                        st.success(f"Cargados {len(sitios_importados)} sitios del archivo.")
                    else: st.error("❌ El archivo no tiene la columna 'Sitio'.")
                except Exception as e: st.error(f"Error al leer archivo: {e}")

            todas_opciones = sorted(pq.read_table(PARQUET_FILE, columns=['Sitio']).to_pandas()['Sitio'].unique())
            seleccion = st.multiselect("Verificar sitios a graficar:", todas_opciones, 
                                      default=[s for s in sitios_importados if s in todas_opciones])

            if seleccion:
                df_up = pd.read_parquet(PARQUET_FILE, filters=[('Sitio', 'in', seleccion)])
                # Agrupación por Hora y Sitio para la gráfica masiva
                resumen = df_up.groupby(['Timestamp', 'Sitio'])['Temp'].max().reset_index()
                
                fig = px.line(resumen, x='Timestamp', y='Temp', color='Sitio', 
                             title="Evolución Térmica Horaria del Upgrade", markers=True)
                fig.add_hline(y=UMBRAL_CRITICO, line_dash="dash", line_color="red", annotation_text="CRÍTICO")
                st.plotly_chart(fig, use_container_width=True)
                
                # Tabla de resumen de los más calientes
                st.subheader("⚠️ Sitios con mayor temperatura en el grupo")
                st.dataframe(resumen.sort_values('Temp', ascending=False).head(20), width=None)
        else:
            st.info("⚠️ Primero genera la base histórica en la pestaña 'HISTÓRICO'.")

else:
    st.warning(f"⚠️ Carpeta '{FOLDER_PATH}' vacía.")
