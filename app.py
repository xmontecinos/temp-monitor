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
                
                # r[0]=Subrack, r[1]=Slot, r[2]=Temp
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
if "df_full" not in st.session_state:
    if os.path.exists(PARQUET_FILE):
        try:
            st.session_state["df_full"] = pd.read_parquet(PARQUET_FILE)
        except:
            st.session_state["df_full"] = pd.DataFrame()
    else:
        st.session_state["df_full"] = pd.DataFrame()

if archivos_lista:
    # El Dashboard usa el reporte más reciente
    if "df_now" not in st.session_state:
        datos_recientes = extraer_datos_masivo(archivos_lista[0])
        st.session_state["df_now"] = pd.DataFrame(datos_recientes)
    
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
            c_info1.info(f"🕒 **Último Reporte Detectado:** {ultima_hora}")
            c_info2.success(f"📍 **Total Sitios:** {df_actual['Sitio'].nunique()}")

            t_crit = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
            t_prev = df_actual[(df_actual['Temp'] >= UMBRAL_PREVENTIVO) & (df_actual['Temp'] < UMBRAL_CRITICO)]
            
            m1, m2, m3 = st.columns(3)
            m1.metric("Total Tarjetas", f"{len(df_actual):,}")
            m2.error(f"Críticos: {len(t_crit)}")
            m3.warning(f"Preventivos: {len(t_prev)}")

            if not t_crit.empty:
                st.divider()
                st.subheader("⚠️ Detalle de Sitios Críticos")
                # Mostramos Subrack y Slot correctamente
                st.dataframe(t_crit[['Sitio', 'Subrack', 'Slot', 'Temp', 'ID_Full']].sort_values('Temp', ascending=False), 
                             use_container_width=True, hide_index=True)
        else:
            st.error("No se detectaron datos en los archivos de la carpeta '/Temperatura'.")

    # --- PESTAÑA 3: HISTÓRICO (PROCESO SEGURO) ---
    with tab_hist:
        st.subheader("📈 Gestión Histórica Incremental")
        df_hist_base = st.session_state["df_full"]
        
        # Identificar qué archivos ya procesamos para no repetir trabajo
        archivos_ya_procesados = set()
        if not df_hist_base.empty and "Archivo_Origen" in df_hist_base.columns:
            archivos_ya_procesados = set(df_hist_base["Archivo_Origen"].unique())
        
        faltantes = [f for f in archivos_lista if os.path.basename(f) not in archivos_ya_procesados]
        
        if faltantes:
            st.warning(f"Se detectaron {len(faltantes)} archivos nuevos por integrar.")
            if st.button("🔥 Sincronizar Faltantes"):
                nuevos_datos = []
                progreso = st.progress(0)
                status = st.empty()
                
                for i, p in enumerate(faltantes):
                    status.text(f"Procesando: {os.path.basename(p)}")
                    data = extraer_datos_masivo(p)
                    if data:
                        temp_df = pd.DataFrame(data)
                        # Optimización de tipos de datos
                        temp_df['Temp'] = temp_df['Temp'].astype('int16')
                        temp_df['Slot'] = temp_df['Slot'].astype('int8')
                        temp_df['Subrack'] = temp_df['Subrack'].astype('int8')
                        nuevos_datos.append(temp_df)
                    
                    progreso.progress((i + 1) / len(faltantes))
                    if i % 30 == 0: gc.collect()
                
                if nuevos_datos:
                    df_final = pd.concat([df_hist_base] + nuevos_datos, ignore_index=True)
                    
                    # VALIDACIÓN DE SEGURIDAD ANTES DE LIMPIAR
                    if 'Timestamp' in df_final.columns and 'ID_Full' in df_final.columns:
                        df_final.drop_duplicates(subset=['Timestamp', 'ID_Full'], keep='last', inplace=True)
                        df_final.to_parquet(PARQUET_FILE, index=False)
                        st.session_state["df_full"] = df_final
                        st.success("¡Base de datos actualizada con éxito!")
                        st.rerun()
                    else:
                        st.error("Error crítico: Las columnas de datos se perdieron. Reintenta la sincronización.")
        else:
            st.success("✅ La base histórica está totalmente actualizada.")

        if not df_hist_base.empty:
            st.divider()
            sitio_sel = st.selectbox("Buscar Historial de Sitio:", sorted(df_hist_base['Sitio'].unique()))
            df_s = df_hist_base[df_hist_base['Sitio'] == sitio_sel]
            ids = sorted(df_s['ID_Full'].unique())
            sel = st.multiselect("Slots a comparar:", ids, default=ids[:1] if ids else [])
            if sel:
                fig = px.line(df_s[df_s['ID_Full'].isin(sel)], x='Timestamp', y='Temp', color='ID_Full', markers=True)
                fig.add_hline(y=UMBRAL_CRITICO, line_dash="dash", line_color="red")
                st.plotly_chart(fig, use_container_width=True)

    # --- PESTAÑA 4: RED POR SLOT ---
    with tab_red:
        st.subheader("🌐 Análisis Global por Hardware")
        if not df_hist_base.empty:
            col_a, col_b = st.columns(2)
            with col_a:
                sub_sel = st.selectbox("Filtrar Subrack:", sorted(df_hist_base['Subrack'].unique()))
            with col_b:
                slot_sel = st.multiselect("Filtrar Slots:", sorted(df_hist_base['Slot'].unique()), default=[0])
            
            df_hw = df_hist_base[(df_hist_base['Subrack'] == sub_sel) & (df_hist_base['Slot'].isin(slot_sel))]
            if not df_hw.empty:
                df_prom = df_hw.groupby([df_hw['Timestamp'].dt.floor('h'), 'Slot'])['Temp'].mean().reset_index()
                fig_hw = px.line(df_prom, x='Timestamp', y='Temp', color='Slot', markers=True, 
                                title="Temperatura Promedio de la Red")
                st.plotly_chart(fig_hw, use_container_width=True)
        else:
            st.info("Sincroniza los archivos en la pestaña HISTÓRICO para activar este análisis.")

    # --- OTRAS PESTAÑAS ---
    with tab_alertas:
        crit_all = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
        if not crit_all.empty:
            for _, r in crit_all.iterrows():
                st.error(f"⚠️ {r['Sitio']} (Subrack {r['Subrack']}, Slot {r['Slot']}): {r['Temp']}°C")
        else:
            st.success("✅ No hay alertas críticas en el reporte actual.")

    with tab_busq:
        s = st.selectbox("Seleccionar Sitio para inspección:", sorted(df_actual['Sitio'].unique()))
        st.dataframe(df_actual[df_actual['Sitio'] == s], use_container_width=True)

else:
    st.warning(f"⚠️ No se encontraron archivos .txt en la ruta '{FOLDER_PATH}'.")
