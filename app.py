import streamlit as st
import pandas as pd
import re
import os
import plotly.express as px

st.set_page_config(page_title="Monitor de Red Crítico", layout="wide")

# --- GESTIÓN DE MEMORIA (INCREMENTAL) ---
if 'df_historico' not in st.session_state:
    st.session_state.df_historico = pd.DataFrame()
if 'archivos_leidos' not in st.session_state:
    st.session_state.archivos_leidos = set()

st.sidebar.header("🛡️ Control de Red")
UMBRAL_CRITICO = 65  # Umbral fijo solicitado

if st.sidebar.button("♻️ Reiniciar Monitor"):
    st.session_state.df_historico = pd.DataFrame()
    st.session_state.archivos_leidos = set()
    st.cache_data.clear()
    st.rerun()

FOLDER_PATH = 'Temperatura'

def procesar_red_incremental(folder):
    if not os.path.exists(folder): return st.session_state.df_historico
    
    todos = [f for f in os.listdir(folder) if f.endswith(".txt")]
    nuevos = [f for f in todos if f not in st.session_state.archivos_leidos]
    
    if not nuevos: return st.session_state.df_historico

    nuevas_filas = []
    for nombre_f in nuevos:
        path = os.path.join(folder, nombre_f)
        try:
            with open(path, 'r', encoding='latin-1', errors='ignore') as f:
                content = f.read()
                blocks = re.findall(r'NE Name:\s+([^\n\r]+).*?(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2}).*?Display Board Temperature(.*?)\n---    END', content, re.DOTALL)
                for ne_name, fecha, hora, table_text in blocks:
                    rows = re.findall(r'(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(?:\d+|NULL)', table_text)
                    for r in rows:
                        nuevas_filas.append({
                            "Timestamp": pd.to_datetime(f"{fecha} {hora}"),
                            "Sitio": ne_name.strip(),
                            "Subrack": r[1],
                            "Slot": int(r[2]),
                            "ID_Full": f"{ne_name.strip()} (S:{r[1]}-L:{r[2]})",
                            "Temp": int(r[3])
                        })
            st.session_state.archivos_leidos.add(nombre_f)
        except: continue

    if nuevas_filas:
        df_nuevo = pd.DataFrame(nuevas_filas)
        # CORRECCIÓN DE ERROR DE SINTAXIS AQUÍ:
        df_final = pd.concat([st.session_state.df_historico, df_nuevo]).drop_duplicates()
        st.session_state.df_historico = df_final
    return st.session_state.df_historico

# --- EJECUCIÓN ---
df = procesar_red_incremental(FOLDER_PATH)

if df is not None and not df.empty:
    # Obtener el último estado reportado de cada componente
    df_actual = df.sort_values('Timestamp').groupby(['Sitio', 'Subrack', 'Slot']).last().reset_index()
    
    # Filtrar elementos críticos de TODA la red
    criticos = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
    
    st.title("🚦 Semáforo de Estado de Red")
    
    # Resumen rápido arriba
    c1, c2 = st.columns(2)
    c1.metric("🔴 COMPONENTES CRÍTICOS (>=65°C)", len(criticos))
    c2.metric("✅ COMPONENTES NORMALES", len(df_actual) - len(criticos))

    tab1, tab2, tab3 = st.tabs(["🚨 ALERTAS CRÍTICAS", "📍 BUSCADOR POR SITIO", "📈 HISTÓRICO"])

    with tab1:
        st.subheader(f"Elementos en estado Crítico (>= {UMBRAL_CRITICO}°C)")
        if not criticos.empty:
            cols = st.columns(4)
            for i, (_, row) in enumerate(criticos.sort_values('Temp', ascending=False).iterrows()):
                with cols[i % 4]:
                    st.markdown(f"""
                        <div style="background-color:#FFC7CE; padding:20px; border-radius:15px; text-align:center; border:2px solid #9C0006; margin-bottom:15px;">
                            <p style="margin:0; font-weight:bold; color:#9C0006;">{row['Sitio']}</p>
                            <small style="color:#9C0006;">SUB {row['Subrack']} | SLOT {row['Slot']}</small>
                            <h1 style="margin:10px 0; color:#9C0006;">{row['Temp']}°C</h1>
                            <p style="margin:0; font-weight:bold; color:#9C0006;">🔴 ALTA TEMPERATURA</p>
                        </div>
                    """, unsafe_allow_html=True)
        else:
            st.success("✅ Todos los sitios operan bajo los 65°C.")

    with tab2:
        lista_s = sorted(df_actual['Sitio'].unique())
        s_sel = st.selectbox("Elegir Sitio para ver todos sus Slots", lista_s)
        df_s = df_actual[df_actual['Sitio'] == s_sel].sort_values(['Subrack', 'Slot'])
        
        cols_det = st.columns(4)
        for i, (_, r) in enumerate(df_s.iterrows()):
            color = "#FFC7CE" if r['Temp'] >= UMBRAL_CRITICO else "#C6EFCE"
            t_col = "#9C0006" if r['Temp'] >= UMBRAL_CRITICO else "#006100"
            with cols_det[i % 4]:
                st.markdown(f"""<div style="background-color:{color}; color:{t_col}; padding:10px; border-radius:10px; text-align:center; border:1px solid {t_col}30;">
                            <b>Slot {r['Slot']}</b><br><span style="font-size:22px;">{r['Temp']}°C</span><br><small>Sub {r['Subrack']}</small></div>""", unsafe_allow_html=True)

    with tab3:
        s_hist = st.selectbox("Elegir sitio para ver tendencias", lista_s, key="shist")
        df_h = df[df['Sitio'] == s_hist]
        slots_h = st.multiselect("Filtrar componentes", sorted(df_h['ID_Full'].unique()), default=sorted(df_h['ID_Full'].unique()))
        df_hf = df_h[df_h['ID_Full'].isin(slots_h)]
        
        if not df_hf.empty:
            fig = px.line(df_hf, x='Timestamp', y='Temp', color='ID_Full', markers=True)
            st.plotly_chart(fig, use_container_width=True)

else:
    st.info("Monitor iniciado. Suba archivos .txt para ver el estado de la red.")
