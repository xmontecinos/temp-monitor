import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px
import gc
from io import BytesIO

# 1. CONFIGURACIÓN DE PÁGINA
st.set_page_config(page_title="Monitor Red - Huawei", layout="wide")

# UMBRALES Y RUTAS
UMBRAL_CRITICO = 78 
UMBRAL_PREVENTIVO = 65
FOLDER_PATH = 'Temperatura'
PARQUET_FILE = 'base_historica.parquet'

# --- FUNCIONES DE EXTRACCIÓN ---
def extraer_datos_masivo(path):
    rows = []
    nombre_archivo = os.path.basename(path)
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
                        "Subrack": int(r[0]),
                        "Slot": int(r[1]),
                        "Temp": int(r[2]), 
                        "ID_Full": f"{nombre_sitio} (S:{r[0]}-L:{r[1]})",
                        "Archivo_Origen": nombre_archivo
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

# Intentar cargar base histórica automáticamente al iniciar
if "df_full" not in st.session_state and os.path.exists(PARQUET_FILE):
    st.session_state["df_full"] = pd.read_parquet(PARQUET_FILE)

if archivos_lista:
    # El Dashboard usa el reporte más reciente
    if "df_now" not in st.session_state:
        st.session_state["df_now"] = pd.DataFrame(extraer_datos_masivo(archivos_lista[0]))
    
    df_actual = st.session_state["df_now"]

    tab_dash, tab_alertas, tab_busq, tab_hist, tab_red = st.tabs([
        "📊 DASHBOARD", "🚨 ALERTAS ACTUALES", "🔍 BUSCADOR", "📈 HISTÓRICO", "🌐 RED POR SLOT"
    ])

    # --- PESTAÑA 0: DASHBOARD ---
    with tab_dash:
        if not df_actual.empty:
            st.title("📊 Monitor de Salud de Red")
            ultima_hora = df_actual['Timestamp'].max().strftime('%d/%m/%Y %H:%M:%S')
            
            c_info1, c_info2 = st.columns(2)
            c_info1.info(f"🕒 **Último Reporte:** {ultima_hora}")
            c_info2.success(f"📍 **Sitios:** {df_actual['Sitio'].nunique()}")

            t_crit = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
            t_prev = df_actual[(df_actual['Temp'] >= UMBRAL_PREVENTIVO) & (df_actual['Temp'] < UMBRAL_CRITICO)]
            
            m1, m2, m3 = st.columns(3)
            m1.metric("Total Tarjetas", len(df_actual))
            m2.error(f"Críticos: {len(t_crit)}")
            m3.warning(f"Preventivos: {len(t_prev)}")

            if not t_crit.empty:
                st.divider()
                st.subheader("⚠️ Detalle Crítico")
                st.dataframe(t_crit[['Sitio', 'Subrack', 'Slot', 'Temp']].sort_values('Temp', ascending=False), use_container_width=True, hide_index=True)
        else:
            st.warning("No hay datos recientes. Sincroniza en la pestaña Histórico.")

    # --- PESTAÑA 3: HISTÓRICO (INCREMENTAL) ---
    with tab_hist:
        st.subheader("📈 Gestión Histórica Incremental")
        df_hist_base = st.session_state.get("df_full", pd.DataFrame())
        archivos_ya_procesados = set(df_hist_base["Archivo_Origen"].unique()) if not df_hist_base.empty else set()
        
        faltantes = [f for f in archivos_lista if os.path.basename(f) not in archivos_ya_procesados]
        
        if faltantes:
            st.warning(f"Hay {len(faltantes)} archivos nuevos.")
            if st.button("🔥 Sincronizar Faltantes"):
                nuevos_datos = []
                progreso = st.progress(0)
                for i, p in enumerate(faltantes):
                    data = extraer_datos_masivo(p)
                    if data:
                        nuevos_datos.append(pd.DataFrame(data))
                    progreso.progress((i + 1) / len(faltantes))
                
                if nuevos_datos:
                    df_final = pd.concat([df_hist_base] + nuevos_datos, ignore_index=True)
                    df_final.drop_duplicates(subset=['Timestamp', 'ID_Full'], keep='last', inplace=True)
                    df_final.to_parquet(PARQUET_FILE, index=False)
                    st.session_state["df_full"] = df_final
                    st.success("¡Base actualizada!")
                    st.rerun()
        else:
            st.success("✅ Todo está al día.")
            if st.button("📂 Cargar Histórico"):
                st.session_state["df_full"] = pd.read_parquet(PARQUET_FILE)
                st.rerun()

    # --- PESTAÑA 4: RED POR SLOT ---
    with tab_red:
        st.subheader("🌐 Análisis Global de Red")
        if not df_hist_base.empty:
            sub_sel = st.selectbox("Subrack:", sorted(df_hist_base['Subrack'].unique()))
            slot_sel = st.multiselect("Slots:", sorted(df_hist_base['Slot'].unique()), default=[0])
            
            df_hw = df_hist_base[(df_hist_base['Subrack'] == sub_sel) & (df_hist_base['Slot'].isin(slot_sel))]
            if not df_hw.empty:
                df_prom = df_hw.groupby([df_hw['Timestamp'].dt.floor('h'), 'Slot'])['Temp'].mean().reset_index()
                st.plotly_chart(px.line(df_prom, x='Timestamp', y='Temp', color='Slot', markers=True), use_container_width=True)
        else:
            st.info("Sincroniza los datos en la pestaña HISTÓRICO para ver el análisis de red.")
