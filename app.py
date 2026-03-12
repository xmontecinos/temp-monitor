import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px
import gc

# 1. Configuración de página
st.set_page_config(page_title="Monitor Red", layout="wide")

# UMBRALES
UMBRAL_CRITICO = 78 
UMBRAL_PREVENTIVO = 60
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
                        "Timestamp": ts, "Sitio": nombre_sitio, "Slot": int(r[1]),
                        "Temp": int(r[2]), "ID_Full": f"{nombre_sitio} (S:{r[0]}-L:{r[1]})"
                    })
    except: pass
    return rows

@st.cache_data(ttl=60)
def listar_archivos(folder):
    if not os.path.exists(folder): return []
    fs = [os.path.join(folder, f) for f in os.listdir(folder) if ".txt" in f]
    fs.sort(key=lambda x: "".join(re.findall(r'\d+', x)), reverse=True)
    return fs

# --- PROCESAMIENTO INICIAL ---
archivos_lista = listar_archivos(FOLDER_PATH)

if archivos_lista:
    if "df_now" not in st.session_state:
        st.session_state["df_now"] = pd.DataFrame(extraer_datos_masivo(archivos_lista[0]))
    
    df_actual = st.session_state["df_now"]
    tab_dash, tab_alertas, tab_busq, tab_hist = st.tabs(["📊 DASHBOARD", "🚨 ALERTAS", "🔍 BUSCADOR", "📈 HISTÓRICO"])

    with tab_dash:
        if not df_actual.empty:
            st.title("📊 Monitor de Salud de Red")
            
            # Cintillo de información (como en tu imagen)
            c_info1, c_info2 = st.columns(2)
            c_info1.info(f"🕒 **Último reporte:** {df_actual['Timestamp'].max().strftime('%d/%m/%Y %H:%M:%S')}")
            c_info2.info(f"📍 **Sitios procesados:** {df_actual['Sitio'].nunique()}")

            # --- LÓGICA DE CONTEO (RESTAURADA) ---
            df_s_max = df_actual.groupby('Sitio')['Temp'].max().reset_index()
            
            # Clasificación de sitios
            s_crit = df_s_max[df_s_max['Temp'] >= UMBRAL_CRITICO]
            s_prev = df_s_max[(df_s_max['Temp'] >= UMBRAL_PREVENTIVO) & (df_s_max['Temp'] < UMBRAL_CRITICO)]
            s_ok = df_s_max[df_s_max['Temp'] < UMBRAL_PREVENTIVO]

            # Clasificación de tarjetas individuales
            t_crit = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
            t_prev = df_actual[(df_actual['Temp'] >= UMBRAL_PREVENTIVO) & (df_actual['Temp'] < UMBRAL_CRITICO)]
            t_ok = df_actual[df_actual['Temp'] < UMBRAL_PREVENTIVO]

            # --- SEMÁFORO VISUAL (RESTAURADO) ---
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Total Tarjetas", f"{len(df_actual):,}")
            
            with m2:
                st.markdown(f'''<div style="background-color:#fee2e2; border:2px solid #dc2626; padding:15px; border-radius:10px; text-align:center;">
                    <h4 style="color:#991b1b; margin:0;">CRÍTICO</h4>
                    <h1 style="color:#dc2626; margin:10px 0;">{len(t_crit)}</h1>
                    <p style="color:#991b1b; margin:0; font-size:0.8em;">En {len(s_crit)} sitios</p></div>''', unsafe_allow_html=True)
            
            with m3:
                st.markdown(f'''<div style="background-color:#fef9c3; border:2px solid #ca8a04; padding:15px; border-radius:10px; text-align:center;">
                    <h4 style="color:#854d0e; margin:0;">PREVENTIVO</h4>
                    <h1 style="color:#ca8a04; margin:10px 0;">{len(t_prev)}</h1>
                    <p style="color:#854d0e; margin:0; font-size:0.8em;">En {len(s_prev)} sitios</p></div>''', unsafe_allow_html=True)
            
            with m4:
                st.markdown(f'''<div style="background-color:#dcfce7; border:2px solid #16a34a; padding:15px; border-radius:10px; text-align:center;">
                    <h4 style="color:#166534; margin:0;">ÓPTIMO</h4>
                    <h1 style="color:#16a34a; margin:10px 0;">{len(t_ok)}</h1>
                    <p style="color:#166534; margin:0; font-size:0.8em;">En {len(s_ok)} sitios</p></div>''', unsafe_allow_html=True)

            st.divider()

            # --- GRÁFICO CON DEGRADADO (RESTAURADO) ---
            if not t_crit.empty:
                st.subheader("🔝 Top 10 Slots Críticos")
                res_slots = t_crit.groupby('Slot').size().reset_index(name='Cant').sort_values('Cant', ascending=False).head(10)
                res_slots['Slot_Label'] = "Slot " + res_slots['Slot'].astype(str)
                
                fig_bar = px.bar(res_slots, x='Slot_Label', y='Cant', color='Cant', 
                                 color_continuous_scale='Reds', text_auto=True)
                # Actualizar para quitar la barra de color lateral si molesta
                fig_bar.update_layout(coloraxis_showscale=False)
                st.plotly_chart(fig_bar, use_container_width=True)

    # --- PESTAÑA HISTÓRICO (CON MEJORA VISUAL) ---
    with tab_hist:
        st.subheader("📈 Gestión de Histórico")
        c1, c2 = st.columns(2)
        with c1:
            num = st.slider("Archivos a procesar:", 1, len(archivos_lista), len(archivos_lista))
            if st.button("🔥 Generar Parquet"):
                all_dfs = []
                bar = st.progress(0)
                for i, p in enumerate(archivos_lista[:num]):
                    data = extraer_datos_masivo(p)
                    if data:
                        df_tmp = pd.DataFrame(data)
                        df_tmp = df_tmp.groupby([df_tmp['Timestamp'].dt.floor('h'), 'Sitio', 'ID_Full'])['Temp'].max().reset_index()
                        all_dfs.append(df_tmp)
                    bar.progress((i + 1) / num)
                pd.concat(all_dfs).to_parquet(PARQUET_FILE, index=False)
                st.success("¡Base Parquet creada!")
        
        with c2:
            if st.button("📂 Cargar desde Parquet"):
                if os.path.exists(PARQUET_FILE):
                    st.session_state["df_full"] = pd.read_parquet(PARQUET_FILE)
                    st.toast("Cargado correctamente")

        if "df_full" in st.session_state:
            st.divider()
            df_p = st.session_state["df_full"]
            s_sel = st.selectbox("Sitio:", sorted(df_p['Sitio'].unique()))
            df_s = df_p[df_p['Sitio'] == s_sel]
            ids = sorted(df_s['ID_Full'].unique())
            sel = st.multiselect("Filtrar Slots:", ids, default=ids[:5])
            
            if sel:
                # Gráfica lineal mejorada para que no sea solo una mancha
                fig_l = px.line(df_s[df_s['ID_Full'].isin(sel)], 
                                x='Timestamp', y='Temp', color='ID_Full',
                                markers=True, title=f"Evolución de Temperatura - {s_sel}")
                fig_l.update_layout(hovermode="x unified")
                st.plotly_chart(fig_l, use_container_width=True)

    # Mantener Alertas y Buscador simples
    with tab_alertas:
        st.subheader("🚨 Detalle de Alertas")
        st.dataframe(df_actual[df_actual['Temp'] >= UMBRAL_CRITICO], use_container_width=True)
    
    with tab_busq:
        st.subheader("🔍 Buscador")
        sitio_b = st.selectbox("Seleccione Sitio:", sorted(df_actual['Sitio'].unique()))
        st.dataframe(df_actual[df_actual['Sitio'] == sitio_b], use_container_width=True)

else:
    st.error("No se encontraron archivos en la carpeta 'Temperatura'.")
