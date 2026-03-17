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

    tab_dash, tab_alertas, tab_busq, tab_hist = st.tabs([
        "📊 DASHBOARD", "🚨 ALERTAS ACTUALES", "🔍 BUSCADOR", "📈 HISTÓRICO"
    ])

    # --- PESTAÑA 0: DASHBOARD ---
    with tab_dash:
        if not df_actual.empty:
            ultima_hora = df_actual['Timestamp'].max().strftime('%d/%m/%Y %H:%M:%S')
            st.title("📊 Monitor de Salud de Red")
            
            c_info1, c_info2 = st.columns(2)
            c_info1.info(f"🕒 **Horario del Reporte:** {ultima_hora}")
            c_info2.success(f"📍 **Sitios Registrados:** {df_actual['Sitio'].nunique()}")

            # Métricas rápidas
            t_crit = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
            t_prev = df_actual[(df_actual['Temp'] >= UMBRAL_PREVENTIVO) & (df_actual['Temp'] < UMBRAL_CRITICO)]
            
            m1, m2, m3 = st.columns(3)
            m1.metric("Total Tarjetas", len(df_actual))
            m2.error(f"Críticos: {len(t_crit)}")
            m3.warning(f"Preventivos: {len(t_prev)}")

            if not t_crit.empty:
                st.divider()
                st.subheader("⚠️ Detalle de Sitios Críticos")
                st.dataframe(t_crit[['Sitio', 'Slot', 'Temp', 'ID_Full']].sort_values('Temp', ascending=False), use_container_width=True, hide_index=True)

    # --- PESTAÑA 3: HISTÓRICO (INCREMENTAL Y CORREGIDA) ---
    with tab_hist:
        st.subheader("📈 Gestión Histórica Incremental")
        
        df_hist_base = pd.DataFrame()
        archivos_ya_procesados = set()
        
        # Intentar cargar base existente
        if os.path.exists(PARQUET_FILE):
            try:
                df_hist_base = pd.read_parquet(PARQUET_FILE)
                if not df_hist_base.empty and "Archivo_Origen" in df_hist_base.columns:
                    archivos_ya_procesados = set(df_hist_base["Archivo_Origen"].unique())
                st.info(f"Base actual: {len(df_hist_base)} registros de {len(archivos_ya_procesados)} archivos.")
            except Exception as e:
                st.error(f"Error al cargar base: {e}")

        # Identificar faltantes
        faltantes = [f for f in archivos_lista if os.path.basename(f) not in archivos_ya_procesados]
        
        c1, c2 = st.columns(2)
        with c1:
            if faltantes:
                st.warning(f"Detectados {len(faltantes)} archivos nuevos.")
                if st.button("🔥 Sincronizar Faltantes"):
                    nuevos_datos = []
                    progreso = st.progress(0)
                    texto = st.empty()
                    
                    for i, p in enumerate(faltantes):
                        texto.text(f"Procesando ({i+1}/{len(faltantes)}): {os.path.basename(p)}")
                        data = extraer_datos_masivo(p)
                        if data:
                            temp_df = pd.DataFrame(data)
                            # Optimización de tipos
                            temp_df['Temp'] = temp_df['Temp'].astype('int16')
                            temp_df['Slot'] = temp_df['Slot'].astype('int8')
                            # Agrupar por hora para ahorrar espacio
                            temp_df = temp_df.groupby([temp_df['Timestamp'].dt.floor('h'), 'Sitio', 'ID_Full', 'Archivo_Origen'], as_index=False)['Temp'].max()
                            nuevos_datos.append(temp_df)
                        
                        progreso.progress((i + 1) / len(faltantes))
                        if i % 25 == 0: gc.collect()

                    if nuevos_datos:
                        df_final = pd.concat([df_hist_base] + nuevos_datos, ignore_index=True)
                        
                        # VALIDACIÓN DE SEGURIDAD PARA DROP_DUPLICATES
                        columnas_limpieza = ['Timestamp', 'ID_Full']
                        if all(col in df_final.columns for col in columnas_limpieza):
                            df_final.drop_duplicates(subset=columnas_limpieza, keep='last', inplace=True)
                            df_final.to_parquet(PARQUET_FILE, index=False)
                            st.session_state["df_full"] = df_final
                            st.success("✅ ¡Base actualizada correctamente!")
                            st.rerun()
                        else:
                            st.error("Error: Columnas corruptas en los datos extraídos.")
            else:
                st.success("✅ Todo está al día.")

        with c2:
            if st.button("📂 Cargar Histórico"):
                if os.path.exists(PARQUET_FILE):
                    st.session_state["df_full"] = pd.read_parquet(PARQUET_FILE)
                    st.success("Datos cargados.")

        # Gráfico Histórico
        if "df_full" in st.session_state or not df_hist_base.empty:
            df_p = st.session_state.get("df_full", df_hist_base)
            st.divider()
            sitio_sel = st.selectbox("Seleccionar Sitio:", sorted(df_p['Sitio'].unique()))
            df_s = df_p[df_p['Sitio'] == sitio_sel]
            ids = sorted(df_s['ID_Full'].unique())
            sel = st.multiselect("Comparar Slots:", ids, default=ids[:2] if ids else [])
            
            if sel:
                fig = px.line(df_s[df_s['ID_Full'].isin(sel)], 
                             x='Timestamp', y='Temp', color='ID_Full', markers=True,
                             title=f"Evolución Térmica: {sitio_sel}")
                fig.add_hline(y=UMBRAL_CRITICO, line_dash="dash", line_color="red")
                st.plotly_chart(fig, use_container_width=True)

    # --- OTRAS PESTAÑAS ---
    with tab_alertas:
        crit_all = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
        if not crit_all.empty:
            for _, r in crit_all.iterrows():
                st.error(f"⚠️ {r['Sitio']} - Slot {r['Slot']}: {r['Temp']}°C")
        else: st.success("✅ Red estable.")

    with tab_busq:
        s = st.selectbox("Buscar Sitio:", sorted(df_actual['Sitio'].unique()), key="search_tab")
        st.dataframe(df_actual[df_actual['Sitio'] == s], use_container_width=True)

else:
    st.warning(f"⚠️ No hay archivos en '{FOLDER_PATH}'.")
