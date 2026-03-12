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

# --- FUNCION DE EXTRACCIÓN CORREGIDA ---
def extraer_datos_masivo(path):
    rows = []
    try:
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            content = f.read()
            # Extraer fecha y hora
            ts_match = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', content)
            if not ts_match: return []
            ts = pd.to_datetime(f"{ts_match.group(1)} {ts_match.group(2)}")
            
            # Dividir por bloques de NE Name
            bloques = re.split(r'NE Name\s*:\s*', content)
            for bloque in bloques[1:]:
                lineas = bloque.split('\n')
                if not lineas: continue
                nombre_sitio = lineas[0].strip().split()[0]
                
                # Buscamos la tabla de datos. 
                # La regex busca: Subrack, Slot, Temperatura (3 columnas numéricas seguidas)
                # Ajustamos para capturar el valor de temperatura real
                filas = re.findall(r'^\s*(\d+)\s+(\d+)\s+(\d+)', bloque, re.MULTILINE)
                
                for r in filas:
                    subrack = r[0]
                    slot = r[1]
                    temp_valor = int(r[2])
                    
                    # FILTRO DE SEGURIDAD: Evita valores basura como 300 o 0
                    if 10 < temp_valor < 120: 
                        rows.append({
                            "Timestamp": ts, 
                            "Sitio": nombre_sitio, 
                            "Slot": int(slot),
                            "Temp": temp_valor, 
                            "ID_Full": f"{nombre_sitio} (S:{subrack}-L:{slot})"
                        })
    except: pass
    return rows

@st.cache_data(ttl=60)
def listar_archivos(folder):
    if not os.path.exists(folder): return []
    fs = [os.path.join(folder, f) for f in os.listdir(folder) if ".txt" in f]
    fs.sort(key=lambda x: "".join(re.findall(r'\d+', x)), reverse=True)
    return fs

# --- PROCESAMIENTO ---
archivos_lista = listar_archivos(FOLDER_PATH)

if archivos_lista:
    if "df_now" not in st.session_state:
        st.session_state["df_now"] = pd.DataFrame(extraer_datos_masivo(archivos_lista[0]))
    
    df_actual = st.session_state["df_now"]
    tab_dash, tab_alertas, tab_busq, tab_hist = st.tabs(["📊 DASHBOARD", "🚨 ALERTAS", "🔍 BUSCADOR", "📈 HISTÓRICO"])

    with tab_dash:
        if not df_actual.empty:
            st.title("📊 Monitor de Salud de Red")
            c_info1, c_info2 = st.columns(2)
            c_info1.info(f"🕒 Reporte: {df_actual['Timestamp'].max()}")
            c_info2.info(f"📍 Sitios: {df_actual['Sitio'].nunique()}")

            # Cálculos de semáforo
            df_s_max = df_actual.groupby('Sitio')['Temp'].max().reset_index()
            num_t_crit = len(df_actual[df_actual['Temp'] >= UMBRAL_CRITICO])
            num_t_prev = len(df_actual[(df_actual['Temp'] >= UMBRAL_PREVENTIVO) & (df_actual['Temp'] < UMBRAL_CRITICO)])
            num_t_ok = len(df_actual[df_actual['Temp'] < UMBRAL_PREVENTIVO])

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Total Tarjetas", f"{len(df_actual):,}")
            
            # Forzamos visualización de colores
            with m2:
                st.markdown(f'<div style="background-color:#fee2e2; border:2px solid #dc2626; padding:15px; border-radius:10px; text-align:center;"><b style="color:#991b1b;">CRÍTICO</b><h1 style="color:#dc2626; margin:0;">{num_t_crit}</h1></div>', unsafe_allow_html=True)
            with m3:
                st.markdown(f'<div style="background-color:#fef9c3; border:2px solid #ca8a04; padding:15px; border-radius:10px; text-align:center;"><b style="color:#854d0e;">PREVENTIVO</b><h1 style="color:#ca8a04; margin:0;">{num_t_prev}</h1></div>', unsafe_allow_html=True)
            with m4:
                st.markdown(f'<div style="background-color:#dcfce7; border:2px solid #16a34a; padding:15px; border-radius:10px; text-align:center;"><b style="color:#166534;">ÓPTIMO</b><h1 style="color:#16a34a; margin:0;">{num_t_ok}</h1></div>', unsafe_allow_html=True)

            if num_t_crit > 0:
                st.subheader("🔝 Top 10 Slots Críticos")
                res_slots = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO].groupby('Slot').size().reset_index(name='Cant').sort_values('Cant', ascending=False).head(10)
                res_slots['Slot_Label'] = "Slot " + res_slots['Slot'].astype(str)
                fig_bar = px.bar(res_slots, x='Slot_Label', y='Cant', color='Cant', color_continuous_scale='Reds', text_auto=True)
                st.plotly_chart(fig_bar, use_container_width=True)

    with tab_hist:
        st.subheader("📈 Gestión de Histórico")
        c1, c2 = st.columns(2)
        with c1:
            num = st.slider("Archivos TXT:", 1, len(archivos_lista), len(archivos_lista))
            if st.button("🔥 Generar Parquet (Limpiar Datos)"):
                all_dfs = []
                bar = st.progress(0)
                for i, p in enumerate(archivos_lista[:num]):
                    data = extraer_datos_masivo(p)
                    if data:
                        df_tmp = pd.DataFrame(data)
                        # Agregamos por hora para que el gráfico sea fluido
                        df_tmp = df_tmp.groupby([df_tmp['Timestamp'].dt.floor('h'), 'Sitio', 'ID_Full'])['Temp'].max().reset_index()
                        all_dfs.append(df_tmp)
                    bar.progress((i + 1) / num)
                if all_dfs:
                    pd.concat(all_dfs).to_parquet(PARQUET_FILE, index=False)
                    st.success("¡Base de datos regenerada y limpia!")
        
        with c2:
            if st.button("📂 Cargar Histórico"):
                if os.path.exists(PARQUET_FILE):
                    st.session_state["df_full"] = pd.read_parquet(PARQUET_FILE)
                    st.toast("Datos cargados")

        if "df_full" in st.session_state:
            df_p = st.session_state["df_full"]
            s_sel = st.selectbox("Sitio:", sorted(df_p['Sitio'].unique()))
            df_s = df_p[df_p['Sitio'] == s_sel]
            ids = sorted(df_s['ID_Full'].unique())
            sel = st.multiselect("Filtrar Slots:", ids, default=ids[:3])
            if sel:
                # El gráfico ahora tendrá una escala de temperatura lógica (ej. 20 a 90)
                fig_l = px.line(df_s[df_s['ID_Full'].isin(sel)], x='Timestamp', y='Temp', color='ID_Full', markers=True)
                fig_l.update_yaxes(range=[10, 100]) # Forzamos el eje Y a valores normales
                st.plotly_chart(fig_l, use_container_width=True)

    with tab_alertas:
        st.dataframe(df_actual[df_actual['Temp'] >= UMBRAL_CRITICO])
    with tab_busq:
        sitio_b = st.selectbox("Buscar:", sorted(df_actual['Sitio'].unique()))
        st.dataframe(df_actual[df_actual['Sitio'] == sitio_b])
else:
    st.error("No hay archivos.")
