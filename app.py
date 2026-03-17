import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px
import gc

# 1. CONFIGURACIÓN DE LA APLICACIÓN
st.set_page_config(page_title="Monitor Red - Huawei", layout="wide")

# UMBRALES DE TEMPERATURA
UMBRAL_CRITICO = 78 
UMBRAL_PREVENTIVO = 65
FOLDER_PATH = 'Temperatura'
PARQUET_FILE = 'base_historica.parquet'

# --- FUNCIÓN DE EXTRACCIÓN DE DATOS ---
def extraer_datos_masivo(path):
    rows = []
    nombre_archivo = os.path.basename(path)
    try:
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            content = f.read()
            # Extracción de fecha y hora
            ts_match = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', content)
            if not ts_match: return []
            ts = pd.to_datetime(f"{ts_match.group(1)} {ts_match.group(2)}")
            
            # Separación por bloques de sitio (NE Name)
            bloques = re.split(r'NE Name\s*:\s*', content)
            for bloque in bloques[1:]:
                lineas = bloque.split('\n')
                if not lineas: continue
                nombre_sitio = lineas[0].strip().split()[0]
                
                # Búsqueda de Subrack, Slot y Temperatura
                # Formato esperado: ID Subrack Slot Temp
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
    # Ordenar archivos para detectar el más reciente
    fs.sort(key=lambda x: "".join(re.findall(r'\d+', x)), reverse=True)
    return fs

# --- LÓGICA DE CARGA INICIAL ---
archivos_lista = listar_archivos(FOLDER_PATH)

# Inicializar df_full en la sesión (Base Histórica)
if "df_full" not in st.session_state:
    st.session_state["df_full"] = pd.DataFrame()
    if os.path.exists(PARQUET_FILE):
        try:
            temp_df = pd.read_parquet(PARQUET_FILE)
            # Validación de integridad de columnas
            if 'Subrack' in temp_df.columns and 'Timestamp' in temp_df.columns:
                st.session_state["df_full"] = temp_df
        except Exception:
            st.warning("Base de datos antigua detectada. Se recomienda sincronizar de nuevo.")

if archivos_lista:
    # Cargar reporte actual para el Dashboard (Pestaña 0)
    if "df_now" not in st.session_state or st.session_state["df_now"].empty:
        st.session_state["df_now"] = pd.DataFrame(extraer_datos_masivo(archivos_lista[0]))
    
    df_actual = st.session_state["df_now"]
    df_hist = st.session_state["df_full"]

    tabs = st.tabs(["📊 DASHBOARD", "🚨 ALERTAS", "🔍 BUSCADOR", "📈 HISTÓRICO", "🌐 RED POR SLOT"])

    # --- PESTAÑA 0: DASHBOARD ---
    with tabs[0]:
        if not df_actual.empty:
            st.title("📊 Monitor de Salud de Red")
            c_m1, c_m2 = st.columns(2)
            c_m1.metric("Última Actualización", df_actual['Timestamp'].max().strftime('%d/%m/%Y %H:%M:%S'))
            c_m2.metric("Sitios Procesados", df_actual['Sitio'].nunique())

            t_crit = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
            t_prev = df_actual[(df_actual['Temp'] >= UMBRAL_PREVENTIVO) & (df_actual['Temp'] < UMBRAL_CRITICO)]
            
            m1, m2, m3 = st.columns(3)
            m1.metric("Total Tarjetas", f"{len(df_actual):,}")
            m2.error(f"Críticos: {len(t_crit)}")
            m3.warning(f"Preventivos: {len(t_prev)}")

            if not t_crit.empty:
                st.divider()
                st.subheader("⚠️ Detalle de Sitios Críticos (Reporte Actual)")
                st.dataframe(t_crit[['Sitio', 'Subrack', 'Slot', 'Temp', 'ID_Full']].sort_values('Temp', ascending=False), 
                             use_container_width=True, hide_index=True)
        else:
            st.error("No se pudieron cargar datos del Dashboard. Verifique los archivos en la carpeta.")

    # --- PESTAÑA 3: HISTÓRICO (GESTIÓN DE PARQUET) ---
    with tabs[3]:
        st.subheader("📈 Sincronización de Base Histórica")
        
        # Detectar archivos nuevos
        procesados = set(df_hist["Archivo_Origen"].unique()) if not df_hist.empty and "Archivo_Origen" in df_hist.columns else set()
        faltantes = [f for f in archivos_lista if os.path.basename(f) not in procesados]

        if faltantes:
            st.info(f"Se encontraron {len(faltantes)} reportes nuevos pendientes de procesar.")
            if st.button("🔥 Iniciar Sincronización"):
                nuevos_datos = []
                progreso = st.progress(0)
                for i, p in enumerate(faltantes):
                    data = extraer_datos_masivo(p)
                    if data: nuevos_datos.append(pd.DataFrame(data))
                    progreso.progress((i + 1) / len(faltantes))
                
                if nuevos_datos:
                    df_final = pd.concat([df_hist] + nuevos_datos, ignore_index=True)
                    # Limpieza de duplicados por seguridad
                    if not df_final.empty:
                        df_final.drop_duplicates(subset=['Timestamp', 'ID_Full'], keep='last', inplace=True)
                        df_final.to_parquet(PARQUET_FILE, index=False)
                        st.session_state["df_full"] = df_final
                        st.success("Sincronización finalizada correctamente.")
                        st.rerun()
        else:
            st.success("✅ La base de datos está al día.")
            if st.button("🗑️ Resetear y Limpiar Todo"):
                if os.path.exists(PARQUET_FILE): os.remove(PARQUET_FILE)
                st.session_state["df_full"] = pd.DataFrame()
                st.rerun()

    # --- PESTAÑA 4: ANÁLISIS DE RED ---
    with tabs[4]:
        st.subheader("🌐 Análisis de Temperatura por Hardware")
        if not df_hist.empty:
            c1, c2 = st.columns(2)
            with c1:
                sub_sel = st.selectbox("Seleccione Subrack:", sorted(df_hist['Subrack'].unique()))
            with c2:
                slot_sel = st.multiselect("Seleccione Slots:", sorted(df_hist['Slot'].unique()), default=[0])
            
            df_g = df_hist[(df_hist['Subrack'] == sub_sel) & (df_hist['Slot'].isin(slot_sel))]
            if not df_g.empty:
                # Agrupar por hora para mejorar visualización
                df_plot = df_g.groupby([df_g['Timestamp'].dt.floor('h'), 'Slot'])['Temp'].mean().reset_index()
                fig = px.line(df_plot, x='Timestamp', y='Temp', color='Slot', markers=True, 
                             title=f"Promedio de Temperatura - Subrack {sub_sel}")
                fig.add_hline(y=UMBRAL_CRITICO, line_dash="dash", line_color="red")
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Cargue o sincronice la base histórica primero.")

    # --- OTRAS PESTAÑAS (ALERTAS Y BUSCADOR) ---
    with tabs[1]:
        if not t_crit.empty:
            for _, r in t_crit.iterrows():
                st.error(f"⚠️ {r['Sitio']} (Subrack {r['Subrack']}, Slot {r['Slot']}): {r['Temp']}°C")
    
    with tabs[2]:
        busqueda = st.selectbox("Seleccione Sitio:", sorted(df_actual['Sitio'].unique()))
        st.dataframe(df_actual[df_actual['Sitio'] == busqueda], use_container_width=True)

else:
    st.warning("⚠️ No se detectaron archivos .txt en la carpeta 'Temperatura'.")
