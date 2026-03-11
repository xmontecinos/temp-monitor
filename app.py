import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px
import gc

# 1. Configuración de página
st.set_page_config(page_title="Monitor Red - Full Histórico", layout="wide")

UMBRAL_CRITICO = 79 
UMBRAL_PREVENTIVO = 60
FOLDER_PATH = 'Temperatura'

# --- FUNCIONES DE EXTRACCIÓN ---
def extraer_datos_masivo(path):
    """Escaneo profundo por bloques de sitio."""
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
    if not os.path.exists(folder): return []
    fs = [os.path.join(folder, f) for f in os.listdir(folder) if ".txt" in f]
    fs.sort(key=lambda x: "".join(re.findall(r'\d+', x)), reverse=True)
    return fs

# --- LÓGICA DE NAVEGACIÓN ---
if "active_tab" not in st.session_state:
    st.session_state["active_tab"] = "📊 DASHBOARD"

def cambiar_tab(nombre_tab):
    st.session_state["active_tab"] = nombre_tab
    st.rerun()

# --- INTERFAZ ---
archivos_lista = listar_archivos(FOLDER_PATH)

if archivos_lista:
    # Sidebar: Control de navegación y RAM
    with st.sidebar:
        st.title("🕹️ Navegación")
        seleccion_sidebar = st.radio("Ir a:", 
                                   ["📊 DASHBOARD", "🚨 ALERTAS ACTUALES", "🔍 BUSCADOR", "📈 HISTÓRICO"],
                                   index=["📊 DASHBOARD", "🚨 ALERTAS ACTUALES", "🔍 BUSCADOR", "📈 HISTÓRICO"].index(st.session_state["active_tab"]))
        
        if seleccion_sidebar != st.session_state["active_tab"]:
            st.session_state["active_tab"] = seleccion_sidebar
            st.rerun()

        st.divider()
        if st.button("♻️ Limpiar Memoria"):
            st.cache_data.clear()
            st.session_state.clear()
            st.rerun()

    # Carga inicial de datos actuales
    if "df_now" not in st.session_state:
        st.session_state["df_now"] = pd.DataFrame(extraer_datos_masivo(archivos_lista[0]))
    
    df_actual = st.session_state["df_now"]

    # Definición de Pestañas
    tab_dash, tab_alertas, tab_busq, tab_hist = st.tabs(["📊 DASHBOARD", "🚨 ALERTAS ACTUALES", "🔍 BUSCADOR", "📈 HISTÓRICO"])

    # --- PESTAÑA 0: DASHBOARD (Actualizada con conteo de sitios) ---
    with tab_dash:
        if not df_actual.empty:
            st.title("Monitor de Salud de Red")
            
            # 1. Cálculos de Tarjetas
            criticas_df = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
            prev_df = df_actual[(df_actual['Temp'] >= UMBRAL_PREVENTIVO) & (df_actual['Temp'] < UMBRAL_CRITICO)]
            ok_df = df_actual[df_actual['Temp'] < UMBRAL_PREVENTIVO]

            # 2. Cálculos de Sitios Únicos
            sitios_criticos = criticas_df['Sitio'].nunique()
            sitios_prev = prev_df['Sitio'].nunique()
            sitios_ok = ok_df['Sitio'].nunique()

            # 3. Diseño de Métricas y Semáforo
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Total Tarjetas", len(df_actual), f"{df_actual['Sitio'].nunique()} Sitios")
            
            with m2:
                st.markdown(f"""<div style="background-color:#fee2e2; border:1px solid #dc2626; padding:15px; border-radius:10px; text-align:center;">
                    <h4 style="color:#991b1b; margin:0;">CRÍTICO</h4>
                    <h1 style="color:#dc2626; margin:0; line-height:1;">{len(criticas_df)}</h1>
                    <small style="color:#991b1b;">Tarjetas en <b>{sitios_criticos}</b> sitios</small>
                </div>""", unsafe_allow_html=True)
                if len(criticas_df) > 0:
                    st.write("") # Espaciador
                    if st.button("Ver Detalle Alertas ➔", key="btn_ir_alertas_new"):
                        cambiar_tab("🚨 ALERTAS ACTUALES")

            with m3:
                st.markdown(f"""<div style="background-color:#fef9c3; border:1px solid #ca8a04; padding:15px; border-radius:10px; text-align:center;">
                    <h4 style="color:#854d0e; margin:0;">PREVENTIVO</h4>
                    <h1 style="color:#ca8a04; margin:0; line-height:1;">{len(prev_df)}</h1>
                    <small style="color:#854d0e;">Tarjetas en <b>{sitios_prev}</b> sitios</small>
                </div>""", unsafe_allow_html=True)

            with m4:
                st.markdown(f"""<div style="background-color:#dcfce7; border:1px solid #16a34a; padding:15px; border-radius:10px; text-align:center;">
                    <h4 style="color:#166534; margin:0;">OPTIMO</h4>
                    <h1 style="color:#16a34a; margin:0; line-height:1;">{len(ok_df)}</h1>
                    <small style="color:#166534;">Tarjetas en <b>{sitios_ok}</b> sitios</small>
                </div>""", unsafe_allow_html=True)

            # Gráfico de Torta
            df_pie = pd.DataFrame({
                "Estado": ["Crítico", "Preventivo", "Normal"], 
                "Cant": [len(criticas_df), len(prev_df), len(ok_df)]
            })
            fig_pie = px.pie(df_pie, values='Cant', names='Estado', hole=0.5,
                           color='Estado', color_discrete_map={'Crítico':'#dc2626', 'Preventivo':'#facc15', 'Normal':'#22c55e'},
                           title="Distribución de Carga Térmica")
            st.plotly_chart(fig_pie, use_container_width=True)

    # --- PESTAÑA 1: ALERTAS ---
    with tab_alertas:
        if not df_actual.empty:
            st.subheader("Tarjetas con Temperatura Elevada")
            slots = sorted(df_actual['Slot'].unique())
            sel_slots = st.multiselect("Filtrar por Slots:", slots, default=slots)
            
            criticos = df_actual[(df_actual['Temp'] >= UMBRAL_CRITICO) & (df_actual['Slot'].isin(sel_slots))]
            if not criticos.empty:
                cols = st.columns(4)
                for i, (_, r) in enumerate(criticos.sort_values('Temp', ascending=False).iterrows()):
                    with cols[i % 4]:
                        st.markdown(f"""<div style="background-color:#fee2e2; border:1px solid #dc2626; padding:10px; border-radius:8px; margin-bottom:10px; text-align:center;">
                            <strong style="color:#991b1b;">{r['Sitio']}</strong><br>
                            <span style="font-size:24px; font-weight:bold; color:#dc2626;">{r['Temp']}°C</span><br>
                            <small>Slot: {r['Slot']}</small></div>""", unsafe_allow_html=True)
            else: st.success("✅ No hay alertas críticas en los slots seleccionados.")

    # --- PESTAÑA 2: BUSCADOR ---
    with tab_busq:
        if not df_actual.empty:
            sitio_busq = st.selectbox("Buscar Sitio:", sorted(df_actual['Sitio'].unique()))
            st.dataframe(df_actual[df_actual['Sitio'] == sitio_busq], use_container_width=True)

    # --- PESTAÑA 3: HISTÓRICO ---
    with tab_hist:
        st.subheader("Análisis de Tendencia Histórica")
        num_reportes = st.slider("Horas hacia atrás:", 10, min(180, len(archivos_lista)), 100)
        
        if st.button(f"📊 Procesar {num_reportes} reportes"):
            all_data = []
            bar = st.progress(0)
            for i, p in enumerate(archivos_lista[:num_reportes]):
                all_data.extend(extraer_datos_masivo(p))
                bar.progress((i + 1) / num_reportes)
            
            if all_data:
                df_h = pd.DataFrame(all_data)
                st.session_state["df_full"] = df_h.groupby([df_h['Timestamp'].dt.floor('h'), 'Sitio', 'ID_Full'])['Temp'].max().reset_index()
                st.success("Datos cargados correctamente.")

        if "df_full" in st.session_state:
            df_p = st.session_state["df_full"]
            sitio_sel = st.selectbox("Sitio para Graficar:", sorted(df_p['Sitio'].unique()))
            
            df_sitio = df_p[df_p['Sitio'] == sitio_sel]
            ids_disponibles = sorted(df_sitio['ID_Full'].unique())

            # Selección Masiva
            col_a, col_b = st.columns(2)
            if col_a.button("✅ Seleccionar Todos los Slots"):
                st.session_state["selected_ids"] = ids_disponibles
            if col_b.button("❌ Deseleccionar Todos"):
                st.session_state["selected_ids"] = []

            seleccionados = st.multiselect("ID_Full:", ids_disponibles, 
                                          default=st.session_state.get("selected_ids", ids_disponibles))

            if seleccionados:
                fig = px.line(df_sitio[df_sitio['ID_Full'].isin(seleccionados)], 
                             x='Timestamp', y='Temp', color='ID_Full', markers=True)
                st.plotly_chart(fig, use_container_width=True)

else:
    st.error("No se detectaron archivos .txt en 'Temperatura'.")
