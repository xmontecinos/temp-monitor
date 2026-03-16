import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px
import gc
from io import BytesIO

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
            total_sitios_red = df_actual['Sitio'].nunique()
            st.title("📊 Monitor de Salud de Red")
            c_info1, c_info2 = st.columns(2)
            c_info1.info(f"🕒 **Horario del Reporte:** {ultima_hora}")
            c_info2.success(f"📍 **Sitios Registrados en este Reporte:** {total_sitios_red}")

            df_sitios_max = df_actual.groupby('Sitio')['Temp'].max().reset_index()
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
                st.subheader("⚠️ Detalle de Sitios Críticos")
                slot_foco = st.selectbox("Selecciona un Slot:", sorted(t_crit['Slot'].unique()))
                df_foco = t_crit[t_crit['Slot'] == slot_foco][['Sitio', 'Temp', 'ID_Full']].sort_values('Temp', ascending=False)
                st.download_button(label='📥 Excel Slot', data=to_excel(df_foco), file_name=f'criticos_slot_{slot_foco}.xlsx')
                st.dataframe(df_foco, use_container_width=True, hide_index=True)

    # --- PESTAÑA 3: HISTÓRICO ---
    with tab_hist:
        st.subheader("📈 Gestión Histórica (Parquet)")
        c1, c2 = st.columns(2)
        
        with c1:
            num_reportes = st.slider("Archivos a incluir:", 1, len(archivos_lista), min(50, len(archivos_lista)))
            if st.button("🔥 Generar/Actualizar Base Parquet"):
                all_dfs = []
                progreso_bar = st.progress(0)
                for i, p in enumerate(archivos_lista[:num_reportes]):
                    data = extraer_datos_masivo(p)
                    if data:
                        temp_df = pd.DataFrame(data)
                        temp_df = temp_df.groupby([temp_df['Timestamp'].dt.floor('h'), 'Sitio', 'ID_Full'])['Temp'].max().reset_index()
                        all_dfs.append(temp_df)
                    if i % 25 == 0: gc.collect()
                
                if all_dfs:
                    df_final = pd.concat(all_dfs, ignore_index=True)
                    df_final.to_parquet(PARQUET_FILE, index=False)
                    st.session_state["df_full"] = df_final
                    st.success(f"✅ ¡Base Parquet lista con {len(df_final):,} registros!")

        with c2:
            if st.button("📂 Cargar desde Parquet"):
                if os.path.exists(PARQUET_FILE):
                    st.session_state["df_full"] = pd.read_parquet(PARQUET_FILE)
                    st.success(f"✅ Datos cargados: {len(st.session_state['df_full']):,} filas.")
            
            # EXPORTACIÓN MASIVA (MEJORADA)
            if "df_full" in st.session_state:
                st.write("---")
                st.write("📥 **Exportar Histórico**")
                
                # Liberar memoria antes de procesar
                gc.collect()
                
                # Usamos una función lambda para que no se ejecute el to_csv hasta que se haga clic
                # Esto ayuda a Streamlit a no colapsar la memoria al renderizar
                @st.cache_data
                def get_csv_data(df_cache):
                    return df_cache.to_csv(index=False).encode('utf-8')

                try:
                    st.download_button(
                        label="🚀 Descargar CSV (Toda la base)",
                        data=get_csv_data(st.session_state["df_full"]),
                        file_name="historico_temperaturas.csv",
                        mime="text/csv"
                    )
                except Exception:
                    st.error("Error de memoria: El archivo es demasiado grande para Streamlit Cloud. Intenta procesar menos archivos.")

        if "df_full" in st.session_state:
            st.divider()
            df_p = st.session_state["df_full"]
            sitio_sel = st.selectbox("Sitio Histórico:", sorted(df_p['Sitio'].unique()))
            df_s = df_p[df_p['Sitio'] == sitio_sel]
            ids = sorted(df_s['ID_Full'].unique())
            sel = st.multiselect("Comparar Slots:", ids, default=ids[:2] if ids else [])
            if sel:
                fig = px.line(df_s[df_s['ID_Full'].isin(sel)], x='Timestamp', y='Temp', color='ID_Full', markers=True)
                fig.add_hline(y=UMBRAL_CRITICO, line_dash="dash", line_color="red")
                st.plotly_chart(fig, use_container_width=True)

    # --- OTRAS PESTAÑAS ---
    with tab_alertas:
        crit_all = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
        if not crit_all.empty:
            st.dataframe(crit_all, use_container_width=True)
        else: st.success("✅ Red estable.")

    with tab_busq:
        s = st.selectbox("Buscar Sitio:", sorted(df_actual['Sitio'].unique()))
        st.dataframe(df_actual[df_actual['Sitio'] == s], use_container_width=True)
else:
    st.warning(f"⚠️ No hay archivos en '{FOLDER_PATH}'.")
