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

    # Definición de pestañas (Indentación corregida)
    tab_dash, tab_alertas, tab_busq, tab_hist, tab_upgrade = st.tabs([
        "📊 DASHBOARD", "🚨 ALERTAS ACTUALES", "🔍 BUSCADOR", "📈 HISTÓRICO", "🚀 ANÁLISIS UPGRADE"
    ])

    # --- PESTAÑA 0: DASHBOARD ---
    with tab_dash:
        if not df_actual.empty:
            ultima_hora = df_actual['Timestamp'].max().strftime('%d/%m/%Y %H:%M:%S')
            total_sitios_red = df_actual['Sitio'].nunique()
            st.title("📊 Monitor de Salud de Red")
            c_info1, c_info2 = st.columns(2)
            c_info1.info(f"🕒 **Horario:** {ultima_hora}")
            c_info2.success(f"📍 **Sitios en Reporte:** {total_sitios_red}")

            df_sitios_max = df_actual.groupby('Sitio')['Temp'].max().reset_index()
            t_crit = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
            t_prev = df_actual[(df_actual['Temp'] >= UMBRAL_PREVENTIVO) & (df_actual['Temp'] < UMBRAL_CRITICO)]
            t_ok = df_actual[df_actual['Temp'] < UMBRAL_PREVENTIVO]

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Total Tarjetas", f"{len(df_actual):,}")
            with m2: st.markdown(f'<div style="background-color:#fee2e2; border:2px solid #dc2626; padding:10px; border-radius:10px; text-align:center;"><h4 style="color:#991b1b; margin:0;">CRÍTICO</h4><h1 style="color:#dc2626; margin:5px 0;">{len(t_crit)}</h1></div>', unsafe_allow_html=True)
            with m3: st.markdown(f'<div style="background-color:#fef9c3; border:2px solid #ca8a04; padding:10px; border-radius:10px; text-align:center;"><h4 style="color:#854d0e; margin:0;">PREVENTIVO</h4><h1 style="color:#ca8a04; margin:5px 0;">{len(t_prev)}</h1></div>', unsafe_allow_html=True)
            with m4: st.markdown(f'<div style="background-color:#dcfce7; border:2px solid #16a34a; padding:10px; border-radius:10px; text-align:center;"><h4 style="color:#166534; margin:0;">ÓPTIMO</h4><h1 style="color:#166534; margin:5px 0;">{len(t_ok)}</h1></div>', unsafe_allow_html=True)

            if not t_crit.empty:
                st.divider()
                st.subheader("⚠️ Detalle Críticos por Slot")
                slot_f = st.selectbox("Selecciona Slot:", sorted(t_crit['Slot'].unique()))
                df_f = t_crit[t_crit['Slot'] == slot_f][['Sitio', 'Temp', 'ID_Full']].sort_values('Temp', ascending=False)
                st.dataframe(df_f, use_container_width=True)

    # --- PESTAÑA 1: ALERTAS ---
    with tab_alertas:
        crit_all = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
        if not crit_all.empty:
            st.error(f"⚠️ {len(crit_all)} slots críticos.")
            # Corregido: eliminando el parámetro 'width' problemático
            st.dataframe(crit_all[['Sitio', 'Slot', 'Temp']].sort_values('Temp', ascending=False), use_container_width=True)
        else: st.success("✅ Todo OK.")

    # --- PESTAÑA 2: BUSCADOR ---
    with tab_busq:
        s_busq = st.selectbox("Buscar Sitio:", sorted(df_actual['Sitio'].unique()))
        st.dataframe(df_actual[df_actual['Sitio'] == s_busq], use_container_width=True)

    # --- PESTAÑA 3: HISTÓRICO ---
    with tab_hist:
        st.subheader("📈 Gestión Histórica")
        c1, c2 = st.columns(2)
        with c1:
            n_rep = st.slider("Archivos:", 1, len(archivos_lista), min(100, len(archivos_lista)))
            if st.button("🔥 Reconstruir Parquet"):
                prog = st.progress(0)
                if os.path.exists(PARQUET_FILE): os.remove(PARQUET_FILE)
                writer = None
                try:
                    for i, p in enumerate(archivos_lista[:n_rep]):
                        prog.progress((i + 1) / n_rep)
                        data = extraer_datos_masivo(p)
                        if data:
                            df_t = pd.DataFrame(data)
                            table = pa.Table.from_pandas(df_t)
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
                s_disp = sorted(pq.read_table(PARQUET_FILE, columns=['Sitio']).to_pandas()['Sitio'].unique())
                sh = st.selectbox("Ver Historial de:", s_disp)
                if sh:
                    df_h = pd.read_parquet(PARQUET_FILE, filters=[('Sitio', '==', sh)])
                    st.plotly_chart(px.line(df_h, x='Timestamp', y='Temp', color='ID_Full'), use_container_width=True)

    # --- PESTAÑA 4: UPGRADE ---
    with tab_upgrade:
        st.header("🚀 Análisis Masivo de Upgrade")
        if os.path.exists(PARQUET_FILE):
            subida = st.file_uploader("Cargar lista de sitios (Excel/CSV):", type=['xlsx', 'csv'])
            sitios_import = []
            if subida:
                try:
                    # Se intenta usar openpyxl si es Excel
                    df_u = pd.read_csv(subida) if subida.name.endswith('.csv') else pd.read_excel(subida)
                    if 'Sitio' in df_u.columns:
                        sitios_import = df_u['Sitio'].astype(str).str.strip().unique().tolist()
                        st.success(f"✅ {len(sitios_import)} sitios cargados.")
                    else: st.error("❌ Falta columna 'Sitio'.")
                except Exception as e: st.error(f"Error: {e}")

            todas = sorted(pq.read_table(PARQUET_FILE, columns=['Sitio']).to_pandas()['Sitio'].unique())
            sel = st.multiselect("Confirmar sitios:", todas, default=[s for s in sitios_import if s in todas])
            if sel:
                df_up = pd.read_parquet(PARQUET_FILE, filters=[('Sitio', 'in', sel)])
                res = df_up.groupby(['Timestamp', 'Sitio'])['Temp'].max().reset_index()
                fig = px.line(res, x='Timestamp', y='Temp', color='Sitio', title="Evolución Horaria", markers=True)
                fig.add_hline(y=UMBRAL_CRITICO, line_dash="dash", line_color="red")
                st.plotly_chart(fig, use_container_width=True)
        else: st.info("Genera el historial primero.")

else:
    st.warning(f"⚠️ Carpeta '{FOLDER_PATH}' sin archivos.")
