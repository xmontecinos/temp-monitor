import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px
import gc

# 1. Configuración de página
st.set_page_config(page_title="Monitor Red", layout="wide")

UMBRAL_CRITICO = 65 
UMBRAL_PREVENTIVO = 55
FOLDER_PATH = 'Temperatura'

# --- FUNCIONES DE EXTRACCIÓN ---
def extraer_datos_masivo(path):
    rows = []
    try:
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            content = f.read()
            # Extraer fecha y hora del encabezado del archivo
            ts_match = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', content)
            if not ts_match: return []
            ts = pd.to_datetime(f"{ts_match.group(1)} {ts_match.group(2)}")
            
            bloques = re.split(r'NE Name\s*:\s*', content)
            for bloque in bloques[1:]:
                lineas = bloque.split('\n')
                if not lineas: continue
                nombre_sitio = lineas[0].strip().split()[0]
                # Regex para capturar: Subrack, Slot, Temperatura
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
    if not os.path.exists(folder): return []
    fs = [os.path.join(folder, f) for f in os.listdir(folder) if ".txt" in f]
    # Ordenar por fecha contenida en el nombre del archivo (asumiendo formato estándar)
    fs.sort(key=lambda x: "".join(re.findall(r'\d+', x)), reverse=True)
    return fs

# --- PROCESAMIENTO INICIAL ---
archivos_lista = listar_archivos(FOLDER_PATH)

if archivos_lista:
    # Sidebar minimalista
    with st.sidebar:
        st.title("⚙️ Sistema")
        if st.button("♻️ Limpiar Caché"):
            st.cache_data.clear()
            st.rerun()

    # Cargar datos del reporte más reciente
    if "df_now" not in st.session_state:
        st.session_state["df_now"] = pd.DataFrame(extraer_datos_masivo(archivos_lista[0]))
    
    df_actual = st.session_state["df_now"]

    # --- DISEÑO DE PESTAÑAS ---
    tab_dash, tab_alertas, tab_busq, tab_hist = st.tabs([
        "📊 DASHBOARD", "🚨 ALERTAS ACTUALES", "🔍 BUSCADOR", "📈 HISTÓRICO"
    ])

    # --- PESTAÑA 0: DASHBOARD ---
    with tab_dash:
        if not df_actual.empty:
            # Cálculos de metadatos solicitados
            ultima_hora = df_actual['Timestamp'].max().strftime('%d/%m/%Y %H:%M:%S')
            total_sitios_red = df_actual['Sitio'].nunique()
            
            st.title("📊 Monitor de Salud de Red")
            
            # Banners de información superior
            c_info1, c_info2 = st.columns(2)
            c_info1.info(f"🕒 **Último reporte:** {ultima_hora}")
            c_info2.info(f"📍 **Sitios procesados:** {total_sitios_red}")

            # Clasificación para el semáforo
            crit_df = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
            prev_df = df_actual[(df_actual['Temp'] >= UMBRAL_PREVENTIVO) & (df_actual['Temp'] < UMBRAL_CRITICO)]
            ok_df = df_actual[df_actual['Temp'] < UMBRAL_PREVENTIVO]

            # Semáforo de métricas
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Total Tarjetas", len(df_actual))
            
            with m2:
                html_crit = f'<div style="background-color:#fee2e2; border:1px solid #dc2626; padding:15px; border-radius:10px; text-align:center;"><h4 style="color:#991b1b; margin:0;">CRÍTICO</h4><h1 style="color:#dc2626; margin:0;">{len(crit_df)}</h1><small>En {crit_df["Sitio"].nunique()} sitios</small></div>'
                st.markdown(html_crit, unsafe_allow_html=True)
            
            with m3:
                html_prev = f'<div style="background-color:#fef9c3; border:1px solid #ca8a04; padding:15px; border-radius:10px; text-align:center;"><h4 style="color:#854d0e; margin:0;">PREVENTIVO</h4><h1 style="color:#ca8a04; margin:0;">{len(prev_df)}</h1><small>En {prev_df["Sitio"].nunique()} sitios</small></div>'
                st.markdown(html_prev, unsafe_allow_html=True)
            
            with m4:
                html_ok = f'<div style="background-color:#dcfce7; border:1px solid #16a34a; padding:15px; border-radius:10px; text-align:center;"><h4 style="color:#166534; margin:0;">ÓPTIMO</h4><h1 style="color:#16a34a; margin:0;">{len(ok_df)}</h1><small>En {ok_df["Sitio"].nunique()} sitios</small></div>'
                st.markdown(html_ok, unsafe_allow_html=True)

            st.divider()
            
            # Análisis Top 10 Slots
            if not crit_df.empty:
                st.subheader("🔝 Top 10 Slots con Mayor Incidencia Crítica")
                res_slots = crit_df.groupby('Slot').size().reset_index(name='Cant').sort_values('Cant', ascending=False).head(10)
                res_slots['Slot_Label'] = "Slot " + res_slots['Slot'].astype(str)
                
                fig_bar = px.bar(res_slots, x='Slot_Label', y='Cant', text='Cant', color='Cant', color_continuous_scale='Reds')
                st.plotly_chart(fig_bar, use_container_width=True)

                st.subheader("🔍 Detalle de Sitios por Slot")
                slot_sel = st.selectbox("Seleccione un Slot para ver sitios afectados:", res_slots['Slot'].tolist())
                if slot_sel:
                    sitios_det = crit_df[crit_df['Slot'] == slot_sel].sort_values('Temp', ascending=False)
                    st.dataframe(sitios_det[['Sitio', 'Temp']], hide_index=True, use_container_width=True)
            else:
                st.success("✅ No hay incidencias críticas registradas en este reporte.")

    # --- PESTAÑA 1: ALERTAS ---
    with tab_alertas:
        st.subheader("🚨 Listado de Alertas Críticas")
        crit_all = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
        if not crit_all.empty:
            cols = st.columns(4)
            for i, (_, r) in enumerate(crit_all.sort_values('Temp', ascending=False).iterrows()):
                with cols[i % 4]:
                    card = f'<div style="background-color:#fee2e2; border:1px solid #dc2626; padding:10px; border-radius:8px; margin-bottom:10px; text-align:center;"><strong style="color:#991b1b;">{r["Sitio"]}</strong><br><span style="font-size:24px; font-weight:bold; color:#dc2626;">{r["Temp"]}°C</span><br><small>Slot: {r["Slot"]}</small></div>'
                    st.markdown(card, unsafe_allow_html=True)
        else: st.success("✅ La red se encuentra operando en temperaturas normales.")

    # --- PESTAÑA 2: BUSCADOR ---
    with tab_busq:
        sitio_busq = st.selectbox("Seleccionar Sitio para inspección:", sorted(df_actual['Sitio'].unique()))
        st.dataframe(df_actual[df_actual['Sitio'] == sitio_busq], use_container_width=True)

    # --- PESTAÑA 3: HISTÓRICO ---
    with tab_hist:
        st.subheader("📈 Análisis de Tendencia")
        num_reportes = st.slider("Cantidad de horas/reportes a procesar:", 10, min(180, len(archivos_lista)), 100)
        if st.button("📊 Cargar Datos Históricos"):
            all_data = []
            progress_bar = st.progress(0)
            for i, p in enumerate(archivos_lista[:num_reportes]):
                all_data.extend(extraer_datos_masivo(p))
                progress_bar.progress((i + 1) / num_reportes)
            
            if all_data:
                df_h = pd.DataFrame(all_data)
                st.session_state["df_full"] = df_h.groupby([df_h['Timestamp'].dt.floor('h'), 'Sitio', 'ID_Full'])['Temp'].max().reset_index()
                st.success("Histórico cargado correctamente.")

        if "df_full" in st.session_state:
            df_p = st.session_state["df_full"]
            sitio_sel = st.selectbox("Sitio Histórico:", sorted(df_p['Sitio'].unique()))
            df_s = df_p[df_p['Sitio'] == sitio_sel]
            ids = sorted(df_s['ID_Full'].unique())
            
            c1, c2 = st.columns(2)
            if c1.button("✅ Seleccionar Todos los Slots"): st.session_state["sel"] = ids
            if c2.button("❌ Limpiar Selección"): st.session_state["sel"] = []
            
            sel = st.multiselect("Filtrar por ID_Full:", ids, default=st.session_state.get("sel", ids))
            if sel:
                fig_l = px.line(df_s[df_s['ID_Full'].isin(sel)], x='Timestamp', y='Temp', color='ID_Full', markers=True)
                st.plotly_chart(fig_l, use_container_width=True)
else:
    st.error("No se detectaron archivos .txt en la carpeta 'Temperatura'. Por favor, cargue archivos para comenzar.")
