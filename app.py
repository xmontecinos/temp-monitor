import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px

# 1. Configuración de página
st.set_page_config(page_title="Monitor Red", layout="wide")

# UMBRALES
UMBRAL_CRITICO = 78 
UMBRAL_PREVENTIVO = 60
FOLDER_PATH = 'Temperatura'
PARQUET_FILE = 'base_historica.parquet'

# --- FUNCIÓN DE EXTRACCIÓN ROBUSTA ---
def extraer_datos_masivo(path):
    rows = []
    try:
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            content = f.read()
            ts_match = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', content)
            if not ts_match: return []
            ts = pd.to_datetime(f"{ts_match.group(1)} {ts_match.group(2)}")
            
            # Dividir por bloques de sitios
            bloques = re.split(r'NE Name\s*:\s*', content)
            for bloque in bloques[1:]:
                lineas = bloque.split('\n')
                nombre_sitio = lineas[0].strip().split()[0]
                
                # Buscamos: Subrack(D) Slot(D) Temperatura(D)
                filas = re.findall(r'^\s*(\d+)\s+(\d+)\s+(\d+)', bloque, re.MULTILINE)
                for r in filas:
                    t_val = int(r[2])
                    # FILTRO CRÍTICO: Ignora basura (0, 300, etc)
                    if 15 <= t_val <= 110:
                        rows.append({
                            "Timestamp": ts, "Sitio": nombre_sitio, "Slot": int(r[1]),
                            "Temp": t_val, "ID_Full": f"{nombre_sitio} (S:{r[0]}-L:{r[1]})"
                        })
    except: pass
    return rows

# --- CARGA DE ARCHIVOS ---
if not os.path.exists(FOLDER_PATH):
    st.error(f"No se encuentra la carpeta '{FOLDER_PATH}'")
    st.stop()

archivos_lista = [os.path.join(FOLDER_PATH, f) for f in os.listdir(FOLDER_PATH) if f.endswith('.txt')]
archivos_lista.sort(reverse=True)

# --- INTERFAZ ---
tab_dash, tab_alertas, tab_busq, tab_hist = st.tabs(["📊 DASHBOARD", "🚨 ALERTAS", "🔍 BUSCADOR", "📈 HISTÓRICO"])

# --- PESTAÑA HISTÓRICO (PROCESAMIENTO) ---
with tab_hist:
    st.subheader("⚙️ Gestión de Base de Datos")
    c1, c2 = st.columns(2)
    with c1:
        num = st.slider("TXTs a procesar:", 1, len(archivos_lista), len(archivos_lista))
        if st.button("🔥 Regenerar Parquet (Limpieza Profunda)"):
            all_dfs = []
            bar = st.progress(0)
            for i, p in enumerate(archivos_lista[:num]):
                data = extraer_datos_masivo(p)
                if data:
                    df_tmp = pd.DataFrame(data)
                    # Agrupamos por hora para limpiar duplicados
                    df_tmp = df_tmp.groupby([df_tmp['Timestamp'].dt.floor('h'), 'Sitio', 'ID_Full'])['Temp'].max().reset_index()
                    all_dfs.append(df_tmp)
                bar.progress((i + 1) / num)
            
            if all_dfs:
                df_final = pd.concat(all_dfs, ignore_index=True)
                df_final.to_parquet(PARQUET_FILE, index=False)
                st.session_state["df_full"] = df_final
                st.success("✅ ¡Base de datos limpia y guardada!")
            else:
                st.error("No se pudieron extraer datos válidos.")
    
    with c2:
        if st.button("📂 Cargar desde Parquet"):
            if os.path.exists(PARQUET_FILE):
                st.session_state["df_full"] = pd.read_parquet(PARQUET_FILE)
                st.toast("Datos cargados correctamente")
            else:
                st.error("Primero debes generar el archivo Parquet.")

    # Gráfico Histórico
    if "df_full" in st.session_state:
        df_p = st.session_state["df_full"]
        st.divider()
        s_sel = st.selectbox("Seleccionar Sitio Histórico:", sorted(df_p['Sitio'].unique()))
        df_s = df_p[df_p['Sitio'] == s_sel]
        ids = sorted(df_s['ID_Full'].unique())
        sel = st.multiselect("Filtrar Slots:", ids, default=ids[:3])
        if sel:
            fig_l = px.line(df_s[df_s['ID_Full'].isin(sel)], x='Timestamp', y='Temp', color='ID_Full', markers=True)
            fig_l.update_yaxes(range=[20, 100]) # Escala fija para evitar el error de 300
            st.plotly_chart(fig_l, use_container_width=True)

# --- DASHBOARD (SÓLO ÚLTIMO REPORTE) ---
with tab_dash:
    if archivos_lista:
        # Siempre leer el archivo más nuevo para el Dash
        data_now = extraer_datos_masivo(archivos_lista[0])
        if data_now:
            df_now = pd.DataFrame(data_now)
            st.title("📊 Monitor de Salud de Red")
            
            # Cintillo
            c_i1, c_i2 = st.columns(2)
            c_i1.info(f"🕒 Reporte: {df_now['Timestamp'].max()}")
            c_i2.info(f"📍 Sitios: {df_now['Sitio'].nunique()}")

            # Cálculos
            df_s_max = df_now.groupby('Sitio')['Temp'].max().reset_index()
            t_crit = df_now[df_now['Temp'] >= UMBRAL_CRITICO]
            t_prev = df_now[(df_now['Temp'] >= UMBRAL_PREVENTIVO) & (df_now['Temp'] < UMBRAL_CRITICO)]
            t_ok = df_now[df_now['Temp'] < UMBRAL_PREVENTIVO]

            # Semáforo con "En X sitios"
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Total Tarjetas", f"{len(df_now):,}")
            
            with m2:
                s_c = len(df_s_max[df_s_max['Temp'] >= UMBRAL_CRITICO])
                st.markdown(f'<div style="background-color:#fee2e2; border:2px solid #dc2626; padding:15px; border-radius:10px; text-align:center;"><b style="color:#991b1b;">CRÍTICO</b><h1 style="color:#dc2626; margin:0;">{len(t_crit)}</h1><small style="color:#991b1b;">En {s_c} sitios</small></div>', unsafe_allow_html=True)
            with m3:
                s_p = len(df_s_max[(df_s_max['Temp'] >= UMBRAL_PREVENTIVO) & (df_s_max['Temp'] < UMBRAL_CRITICO)])
                st.markdown(f'<div style="background-color:#fef9c3; border:2px solid #ca8a04; padding:15px; border-radius:10px; text-align:center;"><b style="color:#854d0e;">PREVENTIVO</b><h1 style="color:#ca8a04; margin:0;">{len(t_prev)}</h1><small style="color:#854d0e;">En {s_p} sitios</small></div>', unsafe_allow_html=True)
            with m4:
                s_o = len(df_s_max[df_s_max['Temp'] < UMBRAL_PREVENTIVO])
                st.markdown(f'<div style="background-color:#dcfce7; border:2px solid #16a34a; padding:15px; border-radius:10px; text-align:center;"><b style="color:#166534;">ÓPTIMO</b><h1 style="color:#16a34a; margin:0;">{len(t_ok)}</h1><small style="color:#166534;">En {s_o} sitios</small></div>', unsafe_allow_html=True)

            # Gráfico Rojo
            if not t_crit.empty:
                st.subheader("🔝 Top 10 Slots Críticos")
                res = t_crit.groupby('Slot').size().reset_index(name='Cant').sort_values('Cant', ascending=False).head(10)
                res['Slot_Label'] = "Slot " + res['Slot'].astype(str)
                fig_bar = px.bar(res, x='Slot_Label', y='Cant', color='Cant', color_continuous_scale='Reds', text_auto=True)
                fig_bar.update_layout(coloraxis_showscale=False)
                st.plotly_chart(fig_bar, use_container_width=True)
        else:
            st.warning("El archivo más reciente no contiene datos válidos.")

# Pestañas Secundarias
with tab_alertas:
    if 'df_now' in locals() and not df_now.empty:
        st.dataframe(df_now[df_now['Temp'] >= UMBRAL_CRITICO], use_container_width=True)
with tab_busq:
    if 'df_now' in locals() and not df_now.empty:
        s_b = st.selectbox("Buscar Sitio:", sorted(df_now['Sitio'].unique()))
        st.dataframe(df_now[df_now['Sitio'] == s_b], use_container_width=True)
