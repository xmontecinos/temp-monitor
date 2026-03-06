import streamlit as st
import pandas as pd
import re
import os
import plotly.express as px

st.set_page_config(page_title="Monitor de Temperaturas", layout="wide")

# --- BOTÓN DE LIMPIEZA EN LA BARRA LATERAL ---
if st.sidebar.button("Limpiar Memoria (Caché)"):
    st.cache_data.clear()
    st.rerun()

st.title("🌡️ Monitor de Temperaturas de Red")

FOLDER_PATH = 'Temperatura' 

@st.cache_data 
def procesar_datos(folder):
    rows_list = []
    if not os.path.exists(folder): return None
    
    files = [f for f in os.listdir(folder) if f.endswith(".txt")]
    for file_name in files:
        path = os.path.join(folder, file_name)
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            content = f.read()
            # Capturamos bloques por NE Name y Fecha
            blocks = re.findall(r'NE Name:\s+([^\n\r]+).*?\+\+\+\s+\S+\s+(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2}).*?Display Board Temperature(.*?)\n---    END', content, re.DOTALL)
            
            for ne_name, fecha, hora, table_text in blocks:
                # REGEX CORREGIDA: Captura las 5 columnas, permitiendo la palabra NULL
                rows = re.findall(r'(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+|NULL)', table_text)
                for r in rows:
                    # r es una tupla, nos aseguramos de tener los 5 valores
                    if len(r) == 5:
                        cab, sub, slot, b_temp, h_temp = r
                        rows_list.append({
                            "Timestamp": pd.to_datetime(f"{fecha} {hora}"),
                            "Sitio": ne_name.strip(),
                            "Board_Temp": int(b_temp), 
                            "Slot": int(slot) 
                        })
    return pd.DataFrame(rows_list)

df = procesar_datos(FOLDER_PATH)

if df is not None and not df.empty:
    lista_sitios = sorted(df['Sitio'].unique())
    
    tab1, tab2 = st.tabs(["📈 Gráficos Históricos", "🚦 Semáforo de Estado"])

    with tab2:
        st.subheader("Estado Actual (Última Lectura detectada)")
        sitio_semaforo = st.selectbox("Elegir Sitio", lista_sitios, key="sem_unico")
        
        # FILTRO CRÍTICO: Solo mostrar slots que aparecen en la ÚLTIMA lectura de este sitio
        ultimo_ts = df[df['Sitio'] == sitio_semaforo]['Timestamp'].max()
        df_ahora = df[(df['Sitio'] == sitio_semaforo) & (df['Timestamp'] == ultimo_ts)].sort_values('Slot')

        # Tarjetas de colores
        cols = st.columns(4)
        for i, (_, row) in enumerate(df_ahora.iterrows()):
            temp = row['Board_Temp']
            # Lógica de colores del semáforo
            if temp < 45: color, msg, icon = "#d4edda", "Normal", "🟢"
            elif temp < 65: color, msg, icon = "#fff3cd", "Precaución", "🟡"
            else: color, msg, icon = "#f8d7da", "CRÍTICO", "🔴"

            with cols[i % 4]:
                st.markdown(f"""
                <div style="background-color:{color}; padding:20px; border-radius:10px; text-align:center; border: 1px solid #00000020; margin-bottom:10px;">
                    <p style="margin:0; font-weight:bold; color:#333;">SLOT {row['Slot']}</p>
                    <h2 style="margin:5px 0; color:black;">{temp}°C</h2>
                    <p style="margin:0; font-size:1.2em;">{icon} <b>{msg}</b></p>
                </div>
                """, unsafe_allow_html=True)
        
        st.info(f"Mostrando datos del: **{ultimo_ts}**")

    with tab1:
        df_hist = df[df['Sitio'] == sitio_semaforo]
        df_hist['Slot'] = df_hist['Slot'].astype(str)
        fig = px.line(df_hist, x='Timestamp', y='Board_Temp', color='Slot', markers=True, title=f"Histórico de {sitio_semaforo}")
        st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("Esperando datos... Si subiste archivos nuevos, pulsa 'Limpiar Memoria' en el menú lateral.")
