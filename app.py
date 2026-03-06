import streamlit as st
import pandas as pd
import re
import os
import plotly.express as px

st.set_page_config(page_title="Monitor de Red Crítico", layout="wide")

st.title("🚦 Monitor de Red: Alertas Críticas")

# --- FILTROS LATERALES ---
st.sidebar.header("🛡️ Control de Red")
UMBRAL_CRITICO = 65 

if st.sidebar.button("♻️ Forzar Recarga"):
    st.cache_data.clear()
    st.rerun()

FOLDER_PATH = 'Temperatura'

@st.cache_data(ttl=60) # Cache corto para actualizar rápido
def procesar_red_veloz(folder):
    rows_list = []
    if not os.path.exists(folder): return None
    
    # Listar y ordenar archivos por los más nuevos
    archivos = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".txt")]
    archivos.sort(key=os.path.getmtime, reverse=True)
    
    # LEER SOLO LOS 5 MÁS RECIENTES para evitar que se quede pegado
    for path in archivos[:5]:
        try:
            with open(path, 'r', encoding='latin-1', errors='ignore') as f:
                content = f.read()
                blocks = re.findall(r'NE Name:\s+([^\n\r]+).*?(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2}).*?Display Board Temperature(.*?)\n---    END', content, re.DOTALL)
                for ne_name, fecha, hora, table_text in blocks:
                    rows = re.findall(r'(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(?:\d+|NULL)', table_text)
                    for r in rows:
                        rows_list.append({
                            "Timestamp": pd.to_datetime(f"{fecha} {hora}"),
                            "Sitio": ne_name.strip(),
                            "Subrack": r[1],
                            "Slot": int(r[2]),
                            "Temp": int(r[3]),
                            "ID_Full": f"{ne_name.strip()} (S:{r[1]}-L:{r[2]})"
                        })
        except: continue
    return pd.DataFrame(rows_list) if rows_list else None

# --- EJECUCIÓN ---
with st.spinner('Cargando alertas recientes...'):
    df = procesar_red_veloz(FOLDER_PATH)

if df is not None and not df.empty:
    # Obtener el último estado reportado de cada sitio
    df_actual = df.sort_values('Timestamp').groupby(['Sitio', 'Subrack', 'Slot']).last().reset_index()
    
    # Filtrar solo los Críticos
    criticos = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]

    tab1, tab2, tab3 = st.tabs(["🚨 ALERTAS CRÍTICAS", "📍 BUSCADOR SITIOS", "📈 TENDENCIAS"])

    with tab1:
        st.subheader(f"Temperaturas >= {UMBRAL_CRITICO}°C")
        if not criticos.empty:
            cols = st.columns(4)
            for i, (_, row) in enumerate(criticos.sort_values('Temp', ascending=False).iterrows()):
                with cols[i % 4]:
                    st.markdown(f"""
                        <div style="background-color:#FFC7CE; padding:15px; border-radius:10px; text-align:center; border:2px solid #9C0006; margin-bottom:10px;">
                            <p style="margin:0; font-weight:bold; color:#9C0006;">{row['Sitio']}</p>
                            <h2 style="margin:5px 0; color:#9C0006;">{row['Temp']}°C</h2>
                            <p style="margin:0; font-size:12px; color:#9C0006;">SUB {row['Subrack']} | SLOT {row['Slot']}</p>
                            <b style="color:#9C0006;">🔴 ALERTA ROJA</b>
                        </div>
                    """, unsafe_allow_html=True)
        else:
            st.success("✅ Red Estable. No hay componentes críticos.")

    with tab2:
        lista_s = sorted(df_actual['Sitio'].unique())
        s_sel = st.selectbox("Elegir Sitio", lista_s)
        df_s = df_actual[df_actual['Sitio'] == s_sel]
        st.dataframe(df_s[['Subrack', 'Slot', 'Temp', 'Timestamp']], use_container_width=True)

    with tab3:
        if len(df) > 1:
            fig = px.line(df[df['Sitio'] == s_sel], x='Timestamp', y='Temp', color='ID_Full', markers=True)
            st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("No se detectan datos nuevos. Verifique que haya archivos recientes en la carpeta 'Temperatura'.")
