import streamlit as st
import pandas as pd
import re
import os
import plotly.express as px

st.set_page_config(page_title="Monitor de Temperaturas", layout="wide")
st.title("🌡️ Monitor de Temperaturas (Lectura Automática)")

FOLDER_PATH = 'Temperatura' 

@st.cache_data
def procesar_datos(folder):
    rows_list = []
    if not os.path.exists(folder):
        return None
    files = [f for f in os.listdir(folder) if f.endswith(".txt")]
    for file_name in files:
        path = os.path.join(folder, file_name)
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            content = f.read()
            blocks = re.findall(r'NE Name:\s+([^\n\r]+).*?\+\+\+\s+\S+\s+(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2}).*?Display Board Temperature(.*?)\n---    END', content, re.DOTALL)
            for ne_name, fecha, hora, table_text in blocks:
                rows = re.findall(r'(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+([A-Z0-9]+)', table_text)
                for cab, sub, slot, b_temp, h_temp in rows:
                    rows_list.append({
                        "Fecha": fecha, "Hora": hora, "Sitio": ne_name.strip(),
                        "Board_Temp": int(b_temp), "Slot": slot
                    })
    return pd.DataFrame(rows_list)

df = procesar_datos(FOLDER_PATH)

if df is not None and not df.empty:
    df['Timestamp'] = pd.to_datetime(df['Fecha'] + ' ' + df['Hora'])
    # ORDENAR PARA EVITAR GRÁFICO ENMARAÑADO
    df = df.sort_values(by='Timestamp')
    
    sitio = st.sidebar.selectbox("Selecciona Sitio", sorted(df['Sitio'].unique()))
    df_sitio = df[df['Sitio'] == sitio]
    
    st.subheader(f"Análisis térmico: {sitio}")
    fig = px.line(df_sitio, x='Timestamp', y='Board_Temp', color='Slot',
                  title=f"Histórico de Temperaturas en {sitio}",
                  labels={'Board_Temp': 'Temp °C', 'Timestamp': 'Fecha y Hora'},
                  markers=True)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.error("No se encontraron datos. Revisa el nombre de la carpeta.")
