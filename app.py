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

# --- EXTRACCIÓN CON FILTRO ANTI-ERRORES ---
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
                filas = re.findall(r'^\s*(\d+)\s+(\d+)\s+(\d+)', bloque, re.MULTILINE)
                
                for r in filas:
                    t_val = int(r[2])
                    # Solo aceptamos valores reales (ej: entre 15°C y 110°C)
                    if 15 <= t_val <= 110:
                        rows.append({
                            "Timestamp": ts, "Sitio": nombre_sitio, "Slot": int(r[1]),
                            "Temp": t_val, "ID_Full": f"{nombre_sitio} (S:{r[0]}-L:{r[1]})"
                        })
    except: pass
    return rows

@st.cache_data(ttl=60)
def listar_archivos(folder):
    if not os.path.exists(folder): return []
    fs = [os.path.join(folder, f) for f in os.listdir(folder) if ".txt" in f]
    fs.sort(key=lambda x: "".join(re.findall(r'\d+', x)), reverse=True)
    return fs

# --- INICIO APP ---
archivos_lista = listar_archivos(FOLDER_PATH)

if archivos_lista:
    if "df_now" not in st.session_state:
        data = extraer_datos_masivo(archivos_lista[0])
        st.session_state["df_now"] = pd.DataFrame(data) if data else pd.DataFrame(columns=["Timestamp", "Sitio", "Slot", "Temp", "ID_Full"])
    
    df_actual = st.session_state["df_now"]
    tab_dash, tab_alertas, tab_busq, tab_hist = st.tabs(["📊 DASHBOARD", "🚨 ALERTAS", "🔍 BUSCADOR", "📈 HISTÓRICO"])

    # --- DASHBOARD ---
    with tab_dash:
        if not df_actual.empty:
            st.title("📊 Monitor de Salud de Red")
            c_info1, c_info2 = st.columns(2)
            c_info1.info(f"🕒 Reporte: {df_actual['Timestamp'].max()}")
            c_info2.info(f"📍 Sitios totales: {df_actual['Sitio'].nunique()}")

            # Cálculos de Sitios (Jerarquía correcta)
            df_s_max = df_actual.groupby('Sitio')['Temp'].max().reset_index()
            s_crit = len(df_s_max[df_s_max['Temp'] >= UMBRAL_CRITICO])
            s_prev = len(df_s_max[(df_s_max['Temp'] >= UMBRAL_PREVENTIVO) & (df_s_max['Temp'] < UMBRAL_CRITICO)])
            s_ok = len(df_s_max[df_s_max['Temp'] < UMBRAL_PREVENTIVO])

            # Cálculos de Tarjetas
            t_crit = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
            n_t_crit, n_t_prev, n_t_ok = len(t_crit), len(df_actual[(df_actual['Temp'] >= UMBRAL_PREVENTIVO) & (df_actual['Temp'] < UMBRAL_CRITICO)]), len(df_actual[df_actual['Temp'] < UMBRAL_PREVENTIVO])

            # SEMÁFORO (Restaurando "En X sitios")
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Total Tarjetas", f"{len(df_actual):,}")
            with m2: st.markdown(f'<div style="background-color:#fee2e2; border:2px solid #dc2626; padding:15px; border-radius:10px; text-align:center;"><b style="color:#991b1b;">CRÍTICO</b><h1 style="color:#dc2626; margin:0;">{n_t_crit}</h1><small style="color:#991b1b;">En {s_crit} sitios</small></div>', unsafe_allow_html=True)
            with m3: st.markdown(f'<div style="background-color:#fef9c3; border:2px solid #ca8a04; padding:15px; border-radius:10px; text-align:center;"><b style="color:#854d0e;">PREVENTIVO</b><h1 style="color:#ca8a04; margin:0;">{n_t_prev}</h1><small style="color:#854d0e;">En {s_prev} sitios</small></div>', unsafe_allow_html=True)
            with m4: st.markdown(f'<div style="background-color:#dcfce7; border:2px solid #16a34a; padding:15px; border-radius:10px; text-align:center;"><b style="color:#166534;">ÓPTIMO</b><h1 style="color:#16a34a; margin:0;">{n_t_ok}</h1><small style="color:#166534;">En {s_ok} sitios</small></div>', unsafe_allow_html=True)

            if not t_crit.empty:
                st.subheader("🔝 Top 10 Slots Críticos")
                res = t_crit.groupby('Slot').size().reset_index(name='Cant').sort_values('Cant', ascending=False).head(10)
                res['Slot_Label'] = "Slot " + res['Slot'].astype(str)
                # Restaurando el degradado rojo
                st.plotly_chart(px.bar(res, x='Slot_Label', y='Cant', color='Cant', color_continuous_scale='Reds', text_auto=True), use_container_width=True)

    # --- ALERTAS (Blindado contra KeyError) ---
    with tab_alertas:
        st.subheader("🚨 Detalle de Alertas Críticas")
        if not df_actual.empty and 'Temp' in df_actual.columns:
            alertas = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO].sort_values('Temp', ascending=False)
            if not alertas.empty:
                st.dataframe(alertas[["Sitio", "Slot", "Temp", "Timestamp"]], use_container_width=True)
            else: st.success("No hay alertas críticas.")
        else: st.warning("No hay datos disponibles para mostrar alertas.")

    # --- HISTÓRICO ---
    with tab_hist:
        st.subheader("📈 Gestión de Base de Datos")
        c1, c2 = st.columns(2)
        with c1:
            num = st.slider("TXTs a incluir:", 1, len(archivos_lista), len(archivos_lista))
            if st.button("🔥 Actualizar Parquet"):
                all_dfs = []
                bar = st.progress(0)
                for i, p in enumerate(archivos_lista[:num]):
                    data = extraer_datos_masivo(p)
                    if data:
                        df_tmp = pd.DataFrame(data)
                        df_tmp = df_tmp.groupby([df_tmp['Timestamp'].dt.floor('h'), 'Sitio', 'ID_Full'])['Temp'].max().reset_index()
                        all_dfs.append(df_tmp)
                    bar.progress((i + 1) / num)
                if all_dfs:
                    pd.concat(all_dfs).to_parquet(PARQUET_FILE, index=False)
                    st.success("¡Base Parquet Limpia y Actualizada!")
        
        with c2:
            if st.button("📂 Cargar Datos"):
                if os.path.exists(PARQUET_FILE):
                    st.session_state["df_full"] = pd.read_parquet(PARQUET_FILE)
                    st.toast("Histórico cargado")

        if "df_full" in st.session_state:
            df_p = st.session_state["df_full"]
            s_sel = st.selectbox("Sitio Histórico:", sorted(df_p['Sitio'].unique()))
            df_s = df_p[df_p['Sitio'] == s_sel]
            ids = sorted(df_s['ID_Full'].unique())
            sel = st.multiselect("Filtrar por Slot:", ids, default=ids[:3])
            if sel:
                fig = px.line(df_s[df_s['ID_Full'].isin(sel)], x='Timestamp', y='Temp', color='ID_Full', markers=True)
                fig.update_yaxes(range=[20, 100]) # Escala lógica
                st.plotly_chart(fig, use_container_width=True)

    with tab_busq:
        sitio_b = st.selectbox("Seleccionar Sitio:", sorted(df_actual['Sitio'].unique()) if not df_actual.empty else [])
        if sitio_b: st.dataframe(df_actual[df_actual['Sitio'] == sitio_b], use_container_width=True)
else:
    st.error("No se encontraron archivos TXT.")
