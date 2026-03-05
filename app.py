import streamlit as st
import pandas as pd
import re
import os
import plotly.express as px

st.set_page_config(page_title="Monitor de Temperaturas", layout="wide")
st.title("🌡️ Monitor de Temperaturas de Red")

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
    # Preparar datos
    df['Timestamp'] = pd.to_datetime(df['Fecha'] + ' ' + df['Hora'])
    df = df.sort_values(by='Timestamp')

    # CREACIÓN DE PESTAÑAS
    tab1, tab2 = st.tabs(["📈 Gráficos Históricos", "🚦 Semáforo de Estado"])

    with tab1:
        st.subheader("Análisis de Tendencias")
        sitio = st.sidebar.selectbox("Selecciona Sitio", sorted(df['Sitio'].unique()))
        df_sitio = df[df['Sitio'] == sitio]
        
        fig = px.line(df_sitio, x='Timestamp', y='Board_Temp', color='Slot',
                      title=f"Histórico de Temperaturas en {sitio}",
                      labels={'Board_Temp': 'Temp °C', 'Timestamp': 'Fecha y Hora'},
                      markers=True)
        st.plotly_chart(fig, use_container_width=True)

    with tab2:
        st.subheader("Último Estado Reportado por Sitio")
        
        # Obtener el último registro de cada sitio/slot
        df_ultimo = df.sort_values('Timestamp').groupby(['Sitio', 'Slot']).last().reset_index()
        
        # Función para asignar colores de semáforo
        def color_semaforo(val):
            if val < 45: color = '🟢 Normal'
            elif val < 65: color = '🟡 Precaución'
            else: color = '🔴 Crítico'
            return color

        df_ultimo['Estado'] = df_ultimo['Board_Temp'].apply(color_semaforo)
        
        # Mostrar métricas rápidas
        col1, col2, col3 = st.columns(3)
        col1.metric("Sitios Críticos (>65°C)", len(df_ultimo[df_ultimo['Board_Temp'] >= 55]))
        col2.metric("Temperatura Máxima Actual", f"{df_ultimo['Board_Temp'].max()} °C")
        col3.metric("Total de Sitios Monitoreados", len(df_ultimo['Sitio'].unique()))

        # Tabla con formato
        st.dataframe(df_ultimo[['Sitio', 'Slot', 'Board_Temp', 'Estado', 'Timestamp']].sort_values(by='Board_Temp', ascending=False), 
                     use_container_width=True)

else:
    st.error("No se encontraron datos. Verifica que la carpeta 'Temperatura' tenga los archivos .txt")
