import streamlit as st
import pandas as pd
import re
import os
import plotly.express as px

st.set_page_config(page_title="Monitor de Temperaturas", layout="wide")

# --- LIMPIEZA TOTAL DE CACHÉ ---
# Agregamos un botón en el lateral para forzar la limpieza si los datos fallan
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
            # Buscamos bloques específicos por sitio y fecha
            blocks = re.findall(r'NE Name:\s+([^\n\r]+).*?\+\+\+\s+\S+\s+(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2}).*?Display Board Temperature(.*?)\n---    END', content, re.DOTALL)
            
            for ne_name, fecha, hora, table_text in blocks:
                # Regex estricta para evitar capturar basura
                rows = re.findall(r'(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(?:[A-Z0-9|NULL]+)', table_text)
                for cab, sub, slot, b_temp, h_temp in rows:
                    rows_list.append({
                        "Timestamp": pd.to_datetime(f"{fecha} {hora}"),
                        "Sitio": ne_name.strip(),
                        "Board_Temp": int(b_temp), 
                        "Slot": int(slot) 
                    })
    return pd.DataFrame(rows_list)

df = procesar_datos(FOLDER_PATH)

if df is not None and not df.empty:
    # --- FILTROS ---
    lista_sitios = sorted(df['Sitio'].unique())
    
    tab1, tab2 = st.tabs(["📈 Gráficos Históricos", "🚦 Semáforo de Estado"])

    with tab2:
        st.subheader("Estado Actual (Última Lectura Real)")
        sitio_semaforo = st.selectbox("Elegir Sitio", lista_sitios, key="sem_unico")
        
        # OBTENER SOLO LA ÚLTIMA FOTO DEL SITIO (Evita mostrar slots viejos)
        ultimo_timestamp = df[df['Sitio'] == sitio_semaforo]['Timestamp'].max()
        df_ahora = df[(df['Sitio'] == sitio_semaforo) & (df['Timestamp'] == ultimo_timestamp)]
        df_ahora = df_ahora.sort_values('Slot')

        # Visualización de Tarjetas
        cols = st.columns(4)
        for i, (_, row) in enumerate(df_ahora.iterrows()):
            temp = row['Board_Temp']
            if temp < 45:
                color = "🟢"
                msg = "Normal"
            elif temp < 65:
                color = "🟡"
                msg = "Precaución"
            else:
                color = "🔴"
                msg = "CRÍTICO"

            with cols[i % 4]:
                # Usamos un contenedor con color para simular el semáforo
                st.markdown(f"""
                <div style="padding:15px; border-radius:10px; border:2px solid #f0f2f6; background-color:#ffffff; text-align:center;">
                    <h3 style="margin:0; font-size:16px;">Slot {row['Slot']}</h3>
                    <h1 style="margin:10px 0; color:{'red' if temp >=65 else 'black'};">{temp}°C</h1>
                    <p style="margin:0; font-weight:bold;">{color} {msg}</p>
                </div>
                """, unsafe_allow_html=True)
        
        st.caption(f"Datos extraídos de la lectura: {ultimo_timestamp}")

    with tab1:
        # (Código de los gráficos aquí...)
        df_hist = df[df['Sitio'] == sitio_semaforo]
        fig = px.line(df_hist, x='Timestamp', y='Board_Temp', color=df_hist['Slot'].astype(str))
        st.plotly_chart(fig, use_container_width=True)

else:
    st.error("No hay datos.")
