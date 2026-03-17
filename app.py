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

def to_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Datos')
    return output.getvalue()

# --- PROCESAMIENTO INICIAL ---
archivos_lista = listar_archivos(FOLDER_PATH)

if archivos_lista:
    # Cargar reporte más reciente para el Dashboard
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
            ultima_hora = df_actual['Timestamp'].max().strftime('%d/%m/%Y %H:%M:%S')
            st.title("📊 Monitor de Salud de Red")
            
            c_info1, c_info2 = st.columns(2)
            c_info1.info(f"🕒 **Horario del Reporte:** {ultima_hora}")
            c_info2.success(f"📍 **Sitios Registrados:** {df_actual['Sitio'].nunique()}")

            df_sitios_max = df_actual.groupby('Sitio')['Temp'].max().reset_index()
            s_crit = df_sitios_max[df_sitios_max['Temp'] >= UMBRAL_CRITICO]
            s_prev = df_sitios_max[(df_sitios_max['Temp'] >= UMBRAL_PREVENTIVO) & (df_sitios_max['Temp'] < UMBRAL_CRITICO)]
            
            t_crit = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
            t_prev = df_actual[(df_actual['Temp'] >= UMBRAL_PREVENTIVO) & (df_actual['Temp'] < UMBRAL_CRITICO)]
            t_ok = df_actual[df_actual['Temp'] < UMBRAL_PREVENTIVO]

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Total Tarjetas", f"{len(df_actual):,}")
            with m2:
                st.markdown(f'<div style="background-color:#fee2e2; border:2px solid #dc2626; padding:15px; border-radius:10px; text-align:center;"><h4 style="color:#991b1b; margin:0;">CRÍTICO</h4><h1 style="color:#dc2626; margin:5px 0; font-size:45px;">{len(t_crit)}</h1><small>En {len(s_crit)} sitios</small></div>', unsafe_allow_html=True)
            with m3:
                st.markdown(f'<div style="background-color:#fef9c3; border:2px solid #ca8a04; padding:15px; border-radius:10px; text-align:center;"><h4 style="color:#854d0e; margin:0;">PREVENTIVO</h4><h1 style="color:#ca8a04; margin:5px 0; font-size:45px;">{len(t_prev)}</h1><small>En {len(s_prev)} sitios</small></div>', unsafe_allow_html=True)
            with m4:
                st.markdown(f'<div style="background-color:#dcfce7; border:2px solid #16a34a; padding:15px; border-radius:10px; text-align:center;"><h4 style="color:#166534; margin:0;">ÓPTIMO</h4><h1 style="color:#166534; margin:5px 0; font-size:45px;">{len(t_ok)}</h1><small>Sitios OK</small></div>', unsafe_allow_html=True)

            if not t_crit.empty:
                st.divider()
                st.subheader("⚠️ Detalle de Sitios Críticos (Actual)")
                st.dataframe(t_crit[['Sitio', 'Subrack', 'Slot', 'Temp', 'ID_Full']].sort_values('Temp', ascending=False), use_container_width=True, hide_index=True)
        else:
            st.error("Dashboard vacío. No se detectaron datos en el último archivo.")

    # --- PESTAÑA 3: HISTÓRICO (INCREMENTAL) ---
    with tab_hist:
        st.subheader("📈 Gestión Histórica Incremental")
        df_hist_base = pd.DataFrame()
        archivos_ya_procesados = set()
        
        if os.path.exists(PARQUET_FILE):
            try:
                df_hist_base = pd.read_parquet(PARQUET_FILE)
                if not df_hist_base.empty and "Archivo_Origen" in df_hist_base.columns:
                    archivos_ya_procesados = set(df_hist_base["Archivo_Origen"].unique())
                st.info(f"Base histórica: {len(df_hist_base)} registros de {len(archivos_ya_procesados)} archivos.")
            except Exception as e:
                st.error(f"Error base Parquet: {e}")

        faltantes = [f for f in archivos_lista if os.path.basename(f) not in archivos_ya_procesados]
        
        c1, c2 = st.columns(2)
        with c1:
            if faltantes:
                st.warning(f"Hay {len(faltantes)} archivos nuevos.")
                if st.button("🔥 Sincronizar Faltantes"):
                    nuevos_datos = []
                    progreso = st.progress(0)
                    texto = st.empty()
                    for i, p in enumerate(faltantes):
                        texto.text(f"Procesando: {os.path.basename(p)}")
                        data = extraer_datos_masivo(p)
                        if data:
                            temp_df = pd.DataFrame(data)
                            temp_df['Temp'] = temp_df['Temp'].astype('int16')
                            temp_df['Slot'] = temp_df['Slot'].astype('int8')
                            temp_df['Subrack'] = temp_df['Subrack'].astype('int8')
                            # Agrupar por hora para optimizar
                            temp_df = temp_df.groupby([temp_df['Timestamp'].dt.floor('h'), 'Sitio', 'ID_Full', 'Subrack', 'Slot', 'Archivo_Origen'], as_index=False)['Temp'].max()
                            nuevos_datos.append(temp_df)
                        progreso.progress((i + 1) / len(faltantes))
                        if i % 30 == 0: gc.collect()

                    if nuevos_datos:
                        df_final = pd.concat([df_hist_base] + nuevos_datos, ignore_index=True)
                        columnas_id = ['Timestamp', 'ID_Full']
                        if all(col in df_final.columns for col in columnas_id):
                            df_final.drop_duplicates(subset=columnas_id, keep='last', inplace=True)
                            df_final.to_parquet(PARQUET_FILE, index=False)
                            st.session_state["df_full"] = df_final
                            st.success("Sincronización completa.")
                            st.rerun()
            else:
                st.success("Base histórica al día.")

        with c2:
            if st.button("📂 Cargar Histórico"):
                if os.path.exists(PARQUET_FILE):
                    st.session_state["df_full"] = pd.read_parquet(PARQUET_FILE)
                    st.success("Datos cargados.")

        if "df_full" in st.session_state or not df_hist_base.empty:
            df_p = st.session_state.get("df_full", df_hist_base)
            st.divider()
            sitio_sel = st.selectbox("Seleccionar Sitio Histórico:", sorted(df_p['Sitio'].unique()))
            df_s = df_p[df_p['Sitio'] == sitio_sel]
            ids = sorted(df_s['ID_Full'].unique())
            sel = st.multiselect("Comparar Slots:", ids, default=ids[:2] if ids else [])
            if sel:
                fig = px.line(df_s[df_s['ID_Full'].isin(sel)], x='Timestamp', y='Temp', color='ID_Full', markers=True)
                fig.add_hline(y=UMBRAL_CRITICO, line_dash="dash", line_color="red")
                st.plotly_chart(fig, use_container_width=True)

    # --- PESTAÑA 4: RED POR HARDWARE (NUEVA) ---
    with tab_red:
        st.subheader("🌐 Análisis por Hardware (Toda la Red)")
        df_r = st.session_state.get("df_full", df_hist_base)
        
        if not df_r.empty:
            col_a, col_b = st.columns(2)
            with col_a:
                sub_sel = st.selectbox("Subrack No.:", sorted(df_r['Subrack'].unique()))
            with col_b:
                slot_sel = st.multiselect("Slot No.:", sorted(df_r['Slot'].unique()), default=[0])

            if slot_sel:
                df_hw = df_r[(df_r['Subrack'] == sub_sel) & (df_r['Slot'].isin(slot_sel))]
                # Comportamiento promedio de ese slot en toda la red
                df_prom = df_hw.groupby([df_hw['Timestamp'].dt.floor('h'), 'Slot'])['Temp'].mean().reset_index()
                
                fig_hw = px.line(df_prom, x='Timestamp', y='Temp', color='Slot', markers=True,
                                title=f"Promedio de Temperatura Red - Subrack {sub_sel}")
                fig_hw.add_hline(y=UMBRAL_CRITICO, line_dash="dash", line_color="red")
                st.plotly_chart(fig_hw, use_container_width=True)
                
                st.subheader("🔥 Sitios con mayor temperatura en este Hardware")
                st.dataframe(df_hw.sort_values('Temp', ascending=False).head(15), hide_index=True)
        else:
            st.warning("Carga el histórico primero para ver datos de la red.")

    # --- PESTAÑAS ALERTAS Y BUSCADOR ---
    with tab_alertas:
        crit_all = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
        if not crit_all.empty:
            for _, r in crit_all.iterrows():
                st.error(f"⚠️ {r['Sitio']} - Slot {r['Slot']}: {r['Temp']}°C")
        else: st.success("✅ Red estable.")

    with tab_busq:
        s = st.selectbox("Buscar Sitio:", sorted(df_actual['Sitio'].unique()), key="search_box")
        st.dataframe(df_actual[df_actual['Sitio'] == s], use_container_width=True)

else:
    st.warning(f"⚠️ No hay archivos en '{FOLDER_PATH}'.")
