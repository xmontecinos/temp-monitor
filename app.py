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
                
                # Capturamos Subrack (r[0]), Slot (r[1]) y Temp (r[2])
                filas = re.findall(r'^\s*\d+\s+(\d+)\s+(\d+)\s+(\d+)', bloque, re.MULTILINE)
                for r in filas:
                    rows.append({
                        "Timestamp": ts, 
                        "Sitio": nombre_sitio, 
                        "Subrack": int(r[0]),
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

    # SE AÑADE LA NUEVA PESTAÑA "📈 RED POR SLOT"
    tab_dash, tab_alertas, tab_busq, tab_hist, tab_red = st.tabs([
        "📊 DASHBOARD", "🚨 ALERTAS ACTUALES", "🔍 BUSCADOR", "📈 HISTÓRICO", "📈 RED POR SLOT"
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
            s_crit = df_sitios_max[df_sitios_max['Temp'] >= UMBRAL_CRITICO]
            s_prev = df_sitios_max[(df_sitios_max['Temp'] >= UMBRAL_PREVENTIVO) & (df_sitios_max['Temp'] < UMBRAL_CRITICO)]
            
            t_crit = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
            t_prev = df_actual[(df_actual['Temp'] >= UMBRAL_PREVENTIVO) & (df_actual['Temp'] < UMBRAL_CRITICO)]
            t_ok = df_actual[df_actual['Temp'] < UMBRAL_PREVENTIVO]

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Total Tarjetas", f"{len(df_actual):,}")
            with m2:
                st.markdown(f'<div style="background-color:#fee2e2; border:2px solid #dc2626; padding:15px; border-radius:10px; text-align:center;"><h4 style="color:#991b1b; margin:0;">CRÍTICO</h4><p style="color:#dc2626; margin:0; font-weight:bold;">≥ {UMBRAL_CRITICO}°C</p><h1 style="color:#dc2626; margin:5px 0; font-size:45px;">{len(t_crit)}</h1><small style="color:#991b1b;">En <b>{len(s_crit)}</b> sitios</small></div>', unsafe_allow_html=True)
            with m3:
                st.markdown(f'<div style="background-color:#fef9c3; border:2px solid #ca8a04; padding:15px; border-radius:10px; text-align:center;"><h4 style="color:#854d0e; margin:0;">PREVENTIVO</h4><p style="color:#ca8a04; margin:0; font-weight:bold;">{UMBRAL_PREVENTIVO}-{UMBRAL_CRITICO-1}°C</p><h1 style="color:#ca8a04; margin:5px 0; font-size:45px;">{len(t_prev)}</h1><small style="color:#854d0e;">En <b>{len(s_prev)}</b> sitios</small></div>', unsafe_allow_html=True)
            with m4:
                st.markdown(f'<div style="background-color:#dcfce7; border:2px solid #16a34a; padding:15px; border-radius:10px; text-align:center;"><h4 style="color:#166534; margin:0;">ÓPTIMO</h4><p style="color:#16a34a; margin:0; font-weight:bold;">< {UMBRAL_PREVENTIVO}°C</p><h1 style="color:#166534; margin:5px 0; font-size:45px;">{len(t_ok)}</h1><small style="color:#166534;">En sitios OK</small></div>', unsafe_allow_html=True)

    # --- PESTAÑA 3: HISTÓRICO (POR SITIO) ---
    with tab_hist:
        st.subheader("📈 Gestión Histórica por Sitio")
        c1, c2 = st.columns(2)
        with c1:
            num_reportes = st.slider("Archivos a incluir:", 1, len(archivos_lista), min(50, len(archivos_lista)), key="slider_hist")
            if st.button("🔥 Generar/Actualizar Base Parquet", key="btn_gen"):
                all_dfs = []
                progreso_bar = st.progress(0)
                texto_estado = st.empty()
                for i, p in enumerate(archivos_lista[:num_reportes]):
                    texto_estado.text(f"Procesando ({i+1}/{num_reportes}): {os.path.basename(p)}")
                    progreso_bar.progress((i + 1) / num_reportes)
                    data = extraer_datos_masivo(p)
                    if data:
                        temp_df = pd.DataFrame(data)
                        # Agrupación por sitio
                        temp_df = temp_df.groupby([temp_df['Timestamp'].dt.floor('h'), 'Sitio', 'ID_Full', 'Subrack', 'Slot'])['Temp'].max().reset_index()
                        all_dfs.append(temp_df)
                    if i % 25 == 0: gc.collect()
                if all_dfs:
                    df_final = pd.concat(all_dfs, ignore_index=True)
                    df_final.to_parquet(PARQUET_FILE, index=False)
                    st.session_state["df_full"] = df_final
                    texto_estado.success(f"✅ ¡Base Parquet lista!")

        with c2:
            if st.button("📂 Cargar desde Parquet", key="btn_load"):
                if os.path.exists(PARQUET_FILE):
                    st.session_state["df_full"] = pd.read_parquet(PARQUET_FILE)
                    st.success("✅ Datos cargados.")

        if "df_full" in st.session_state:
            st.divider()
            df_p = st.session_state["df_full"]
            sitio_sel = st.selectbox("Seleccionar Sitio:", sorted(df_p['Sitio'].unique()))
            df_s = df_p[df_p['Sitio'] == sitio_sel]
            ids = sorted(df_s['ID_Full'].unique())
            sel = st.multiselect("Comparar Slots del sitio:", ids, default=ids[:2] if ids else [])
            if sel:
                fig = px.line(df_s[df_s['ID_Full'].isin(sel)], x='Timestamp', y='Temp', color='ID_Full', markers=True)
                fig.add_hline(y=UMBRAL_CRITICO, line_dash="dash", line_color="red")
                st.plotly_chart(fig, use_container_width=True)

    # --- PESTAÑA 4: RED POR SLOT (NUEVA) ---
    with tab_red:
        st.subheader("🌐 Análisis Global de Red (Promedio por Hardware)")
        if "df_full" in st.session_state:
            df_r = st.session_state["df_full"]
            
            col_a, col_b = st.columns(2)
            with col_a:
                sub_sel = st.selectbox("Seleccionar Subrack No.:", sorted(df_r['Subrack'].unique()))
            with col_b:
                slot_sel = st.multiselect("Seleccionar Slot No.:", sorted(df_r['Slot'].unique()), default=[0, 1])

            if slot_sel:
                # Filtramos la red por el hardware específico (independiente del sitio)
                df_filtro_red = df_r[(df_r['Subrack'] == sub_sel) & (df_r['Slot'].isin(slot_sel))]
                
                # Agrupamos por hora y Slot para ver el comportamiento promedio de la red
                df_resumen_red = df_filtro_red.groupby([df_filtro_red['Timestamp'].dt.floor('h'), 'Slot'])['Temp'].mean().reset_index()
                
                st.info(f"Mostrando el promedio de temperatura en toda la red para Subrack {sub_sel} y Slots {slot_sel}")
                
                fig_red = px.line(df_resumen_red, x='Timestamp', y='Temp', color='Slot', 
                                 title=f"Comportamiento Histórico Red: Subrack {sub_sel}",
                                 labels={'Temp': 'Temp Promedio (°C)'}, markers=True)
                fig_red.add_hline(y=UMBRAL_CRITICO, line_dash="dash", line_color="red")
                st.plotly_chart(fig_red, use_container_width=True)
                
                # Tabla de máximos detectados en ese hardware
                st.subheader("🔥 Máximos detectados en este Hardware (Top Sitios)")
                df_max_hw = df_filtro_red.sort_values('Temp', ascending=False).head(10)
                st.table(df_max_hw[['Timestamp', 'Sitio', 'Subrack', 'Slot', 'Temp']])
        else:
            st.warning("Por favor, carga o genera la base de datos en la pestaña HISTÓRICO primero.")

    # --- OTRAS PESTAÑAS ---
    with tab_alertas:
        crit_all = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
        if not crit_all.empty:
            for _, r in crit_all.iterrows():
                st.error(f"⚠️ {r['Sitio']} - Slot {r['Slot']}: {r['Temp']}°C")
        else: st.success("✅ Red estable.")

    with tab_busq:
        s = st.selectbox("Buscar Sitio:", sorted(df_actual['Sitio'].unique()), key="busq_red")
        st.dataframe(df_actual[df_actual['Sitio'] == s], use_container_width=True)
else:
    st.warning(f"⚠️ No hay archivos en '{FOLDER_PATH}'.")
