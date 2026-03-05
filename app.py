import streamlit as st
import pandas as pd
import re
import os
import plotly.express as px

st.set_page_config(page_title="Monitor Automático de Red", layout="wide")

st.title("🌡️ Monitor de Temperaturas (Lectura Automática)")

# --- CONFIGURACIÓN DE CARPETA ---
# Asegúrate de que esta carpeta exista en tu repositorio de GitHub
FOLDER_PATH = 'temperaturas' 

def procesar_datos(folder):
    rows_list = []
    if not os.path.exists(folder):
        return None
    
    files = [f for f in os.listdir(folder) if f.endswith(".txt")]
    
    for file_name in files:
        path = os.path.join(folder, file_name)
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            content = f.read()
            
            # Tu Regex original
            blocks = re.findall(r'NE Name:\s+([^\n\r]+).*?\+\+\+\s+\S+\s+(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2}).*?Display Board Temperature(.*?)\n---    END', content, re.DOTALL)
            
            for ne_name, fecha, hora, table_text in blocks:
                rows = re.findall(r'(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+([A-Z0-9]+)', table_text)
                for cab, sub, slot, b_temp, h_temp in rows:
                    rows_list.append({
                        "Fecha": fecha,
                        "Hora": hora,
                        "Sitio": ne_name.strip(),
                        "Board_Temp": int(b_temp),
                        "Slot": slot
                    })
    return pd.DataFrame(rows_list)

# 2. Ejecución del proceso
if os.path.exists(FOLDER_PATH):
    df = procesar_datos(FOLDER_PATH)
    
    if not df.empty:
        df['Timestamp'] = pd.to_datetime(df['Fecha'] + ' ' + df['Hora'])
        
        # --- FILTROS ---
        st.sidebar.header("Filtros de Red")
        sitio = st.sidebar.selectbox("Selecciona Sitio", sorted(df['Sitio'].unique()))
        
        df_sitio = df[df['Sitio'] == sitio]
        
        # --- GRÁFICA ---
        st.subheader(f"Análisis térmico: {sitio}")
        
        fig = px.line(df_sitio, 
                      x='Timestamp', 
                      y='Board_Temp', 
                      color='Slot', # Separa líneas por Slot automáticamente
                      title=f"Histórico de Temperaturas en {sitio}",
                      labels={'Board_Temp': 'Temp °C', 'Timestamp': 'Fecha y Hora'},
                      markers=True)
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Métrica rápida
        max_temp = df_sitio['Board_Temp'].max()
        st.metric(label="Temperatura Máxima Registrada", value=f"{max_temp} °C")
        
    else:
        st.error("La carpeta existe pero no se encontraron datos válidos.")
else:
    st.error(f"No se encontró la carpeta '{FOLDER_PATH}'. Asegúrate de subirla a GitHub.")
