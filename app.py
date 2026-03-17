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

# Intentar cargar base histórica de forma segura
if "df_full" not in st.session_state:
    st.session_state["df_full"] = pd.DataFrame()
    if os.path.exists(PARQUET_FILE):
        try:
            # Carga optimizada
            st.session_state["df_full"] = pd.read_parquet(PARQUET_FILE)
        except Exception:
            st.error("Archivo Parquet incompatible. Por favor, sincroniza de nuevo.")

if archivos_lista:
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
            m1.metric("Total Tarjetas", f"{len(df_actual):,}")
            m2.error(f"Críticos: {len(t_crit)}")
            m3.warning(f"Preventivos: {len(t_prev)}")

            if not t_crit.empty:
                st.divider()
                st.subheader("⚠️ Detalle de Sitios Críticos")
                st.dataframe(t_crit[['Sitio', 'Subrack', 'Slot', 'Temp']].sort_values('Temp', ascending=False), 
                             use_container_width=True, hide_index=True)

    # --- PESTAÑA 3: HISTÓRICO (PROCESO OPTIMIZADO) ---
    with tab_hist:
        st.subheader("📈 Gestión Histórica Incremental")
        df_hist_base = st.session_state["df_full"]
        
        archivos_ya_procesados = set()
        if not df_hist_base.empty and "Archivo_Origen" in df_hist_base.columns:
            archivos_ya_procesados = set(df_hist_base["Archivo_Origen"].unique())
        
        faltantes = [f for f in archivos_lista if os.path.basename(f) not in archivos_ya_procesados]
        
        c1, c2 = st.columns(2)
        with c1:
            if faltantes:
                st.warning(f"Faltan {len(faltantes)} archivos por procesar.")
                if st.button("🔥 Sincronizar"):
                    nuevos_datos = []
                    progreso = st.progress(0)
                    for i, p in enumerate(faltantes):
                        data = extraer_datos_masivo(p)
                        if data:
                            nuevos_datos.append(pd.DataFrame(data))
                        
                        # Gestión de memoria
                        if i % 50 == 0:
                            progreso.progress((i + 1) / len(faltantes))
                            gc.collect()
                    
                    if nuevos_datos:
                        df_final = pd.concat([df_hist_base] + nuevos_datos, ignore_index=True)
                        # Asegurar tipos antes de guardar
                        df_final['Subrack'] = df_final['Subrack'].astype(int)
                        df_final['Slot'] = df_final['Slot'].astype(int)
                        
                        df_final.drop_duplicates(subset=['Timestamp', 'ID_Full'], keep='last', inplace=True)
                        df_final.to_parquet(PARQUET_FILE, index=False)
                        st.session_state["df_full"] = df_final
                        st.success("Sincronización Exitosa.")
                        st.rerun()
            else:
                st.success("Base de datos al día.")

        with c2:
            if st.button("🗑️ Resetear Base (Limpiar Todo)"):
                if os.path.exists(PARQUET_FILE):
                    os.remove(PARQUET_FILE)
                st.session_state["df_full"] = pd.DataFrame()
                st.rerun()

    # --- PESTAÑA 4: RED POR SLOT ---
    with tab_red:
        st.subheader("🌐 Análisis Global de Red")
        if not df_hist_base.empty:
            sub_sel = st.selectbox("Seleccionar Subrack:", sorted(df_hist_base['Subrack'].unique()))
            slots_disp = sorted(df_hist_base[df_hist_base['Subrack'] == sub_sel]['Slot'].unique())
            slot_sel = st.multiselect("Seleccionar Slots:", slots_disp, default=slots_disp[:1])
            
            df_hw = df_hist_base[(df_hist_base['Subrack'] == sub_sel) & (df_hist_base['Slot'].isin(slot_sel))]
            if not df_hw.empty:
                df_prom = df_hw.groupby([df_hw['Timestamp'].dt.floor('h'), 'Slot'])['Temp'].mean().reset_index()
                fig = px.line(df_prom, x='Timestamp', y='Temp', color='Slot', markers=True, title="Temp Promedio en Red")
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Sincroniza los datos primero.")

else:
    st.warning(f"⚠️ No hay archivos en '{FOLDER_PATH}'.")
