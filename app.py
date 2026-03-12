import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px
import gc

# 1. Configuración de página
st.set_page_config(page_title="Monitor Red", layout="wide")

# UMBRALES DEFINIDOS
UMBRAL_CRITICO = 78 
UMBRAL_PREVENTIVO = 60
FOLDER_PATH = 'Temperatura'
PARQUET_FILE = 'base_historica.parquet'  # Nombre del archivo optimizado

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
    if not os.path.exists(folder): return []
    fs = [os.path.join(folder, f) for f in os.listdir(folder) if ".txt" in f]
    fs.sort(key=lambda x: "".join(re.findall(r'\d+', x)), reverse=True)
    return fs

# --- PROCESAMIENTO INICIAL ---
archivos_lista = listar_archivos(FOLDER_PATH)

if archivos_lista:
    with st.sidebar:
        st.title("⚙️ Sistema")
        if st.button("♻️ Limpiar Caché"):
            st.cache_data.clear()
            st.rerun()

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
            c_info1.info(f"🕒 **Último reporte:** {ultima_hora}")
            c_info2.info(f"📍 **Sitios únicos:** {total_sitios_red}")

            df_sitios_max = df_actual.groupby('Sitio')['Temp'].max().reset_index()
            s_crit = df_sitios_max[df_sitios_max['Temp'] >= UMBRAL_CRITICO]
            s_prev = df_sitios_max[(df_sitios_max['Temp'] >= UMBRAL_PREVENTIVO) & (df_sitios_max['Temp'] < UMBRAL_CRITICO)]
            s_ok = df_sitios_max[df_sitios_max['Temp'] < UMBRAL_PREVENTIVO]

            t_crit = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
            t_prev = df_actual[(df_actual['Temp'] >= UMBRAL_PREVENTIVO) & (df_actual['Temp'] < UMBRAL_CRITICO)]
            t_ok = df_actual[df_actual['Temp'] < UMBRAL_PREVENTIVO]

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Total Tarjetas", f"{len(df_actual):,}")
            
            with m2:
                st.markdown(f'<div style="background-color:#fee2e2; border:2px solid #dc2626; padding:15px; border-radius:10px; text-align:center;"><h4 style="color:#991b1b; margin:0;">CRÍTICO</h4><p style="color:#dc2626; margin:0; font-weight:bold;">≥ {UMBRAL_CRITICO}°C</p><h1 style="color:#dc2626; margin:5px 0;">{len(t_crit)}</h1><small style="color:#991b1b;">En <b>{len(s_crit)}</b> sitios</small></div>', unsafe_allow_html=True)
            with m3:
                st.markdown(f'<div style="background-color:#fef9c3; border:2px solid #ca8a04; padding:15px; border-radius:10px; text-align:center;"><h4 style="color:#854d0e; margin:0;">PREVENTIVO</h4><p style="color:#ca8a04; margin:0; font-weight:bold;">{UMBRAL_PREVENTIVO}-{UMBRAL_CRITICO-1}°C</p><h1 style="color:#ca8a04; margin:5px 0;">{len(t_prev)}</h1><small style="color:#854d0e;">En <b>{len(s_prev)}</b> sitios</small></div>', unsafe_allow_html=True)
            with m4:
                st.markdown(f'<div style="background-color:#dcfce7; border:2px solid #16a34a; padding:15px; border-radius:10px; text-align:center;"><h4 style="color:#166534; margin:0;">ÓPTIMO</h4><p style="color:#16a34a; margin:0; font-weight:bold;">< {UMBRAL_PREVENTIVO}°C</p><h1 style="color:#16a34a; margin:5px 0;">{len(t_ok)}</h1><small style="color:#166534;">En <b>{len(s_ok)}</b> sitios</small></div>', unsafe_allow_html=True)

            st.divider()
           if not t_crit.empty:
            st.subheader("🔝 Top 10 Tarjetas con Mayor Temperatura")
            
            # Ordenamos por temperatura de forma descendente y tomamos las 10 primeras
            top_criticos = t_crit.sort_values('Temp', ascending=False).head(10)
            
            fig_top = px.bar(
                top_criticos, 
                x='Temp', 
                y='ID_Full', 
                orientation='h',
                text='Temp',
                color='Temp',
                color_continuous_scale='Reds',
                labels={'ID_Full': 'Sitio y Slot', 'Temp': 'Temperatura (°C)'}
            )
            fig_top.update_layout(yaxis={'categoryorder':'total ascending'}) # Lo más caliente arriba
            st.plotly_chart(fig_top, use_container_width=True)
        else:
            st.info("No hay tarjetas en nivel crítico para mostrar en el ranking.")

    # --- PESTAÑA 3: HISTÓRICO (OPTIMIZADA CON PARQUET) ---
    with tab_hist:
        st.subheader("📈 Gestión de Base de Datos Histórica")
        
        c1, c2 = st.columns(2)
        with c1:
            st.write("### 1. Procesar Archivos TXT")
            # Slider que permite seleccionar TODOS los archivos por defecto
            num_reportes = st.slider("Archivos a incluir:", 1, len(archivos_lista), len(archivos_lista))
            if st.button("🔥 Generar/Actualizar Parquet"):
                all_dfs = []
                p_bar = st.progress(0)
                p_text = st.empty()
                for i, p in enumerate(archivos_lista[:num_reportes]):
                    p_text.text(f"Procesando ({i+1}/{num_reportes}): {os.path.basename(p)}")
                    data = extraer_datos_masivo(p)
                    if data:
                        # Agregación inmediata para no colapsar la RAM
                        temp_df = pd.DataFrame(data)
                        temp_df = temp_df.groupby([temp_df['Timestamp'].dt.floor('h'), 'Sitio', 'ID_Full'])['Temp'].max().reset_index()
                        all_dfs.append(temp_df)
                    p_bar.progress((i + 1) / num_reportes)
                    if i % 20 == 0: gc.collect() # Limpiar memoria
                
                if all_dfs:
                    df_final = pd.concat(all_dfs, ignore_index=True)
                    df_final.to_parquet(PARQUET_FILE, index=False)
                    st.session_state["df_full"] = df_final
                    p_text.success(f"✅ ¡Base de datos Parquet creada con {num_reportes} reportes!")
        
        with c2:
            st.write("### 2. Cargar o Limpiar")
            if st.button("📂 Cargar desde Parquet"):
                if os.path.exists(PARQUET_FILE):
                    st.session_state["df_full"] = pd.read_parquet(PARQUET_FILE)
                    st.success("✅ Datos cargados instantáneamente.")
                else: st.error("No existe el archivo Parquet.")
            
            if st.button("🗑️ Eliminar Parquet"):
                if os.path.exists(PARQUET_FILE):
                    os.remove(PARQUET_FILE)
                    st.warning("Archivo eliminado.")

        if "df_full" in st.session_state:
            st.divider()
            df_p = st.session_state["df_full"]
            sitio_sel = st.selectbox("Sitio Histórico:", sorted(df_p['Sitio'].unique()))
            df_s = df_p[df_p['Sitio'] == sitio_sel]
            ids = sorted(df_s['ID_Full'].unique())
            sel = st.multiselect("Filtrar por Slot:", ids, default=ids)
            if sel:
                st.plotly_chart(px.line(df_s[df_s['ID_Full'].isin(sel)], x='Timestamp', y='Temp', color='ID_Full', markers=True), use_container_width=True)

    # --- PESTAÑAS ALERTAS Y BUSCADOR (Igual a tu código) ---
    with tab_alertas:
        crit_all = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
        if not crit_all.empty:
            cols = st.columns(4)
            for i, (_, r) in enumerate(crit_all.sort_values('Temp', ascending=False).iterrows()):
                with cols[i % 4]:
                    st.markdown(f'<div style="background-color:#fee2e2; border:1px solid #dc2626; padding:10px; border-radius:8px; margin-bottom:10px; text-align:center;"><strong style="color:#991b1b;">{r["Sitio"]}</strong><br><span style="font-size:24px; font-weight:bold; color:#dc2626;">{r["Temp"]}°C</span><br><small>Slot: {r["Slot"]}</small></div>', unsafe_allow_html=True)
        else: st.success("✅ Red estable.")

    with tab_busq:
        sitio_busq = st.selectbox("Seleccionar Sitio:", sorted(df_actual['Sitio'].unique()))
        st.dataframe(df_actual[df_actual['Sitio'] == sitio_busq], use_container_width=True)
else:
    st.error("No hay archivos en 'Temperatura'.")
