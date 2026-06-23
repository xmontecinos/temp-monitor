import streamlit as st
import pandas as pd
import re
import plotly.express as px

st.set_page_config(page_title="Monitor de Temperatura de Sitios", layout="wide")

st.title("🌡️ Reporte Gráfico de Temperaturas")

# Configuración en el Sidebar
st.sidebar.header("Configuración")
target_sites = st.sidebar.text_input("Sitios a buscar (separados por |)", "01_314")
uploaded_files = st.sidebar.file_uploader("Sube tus archivos .txt", accept_multiple_files=True)

data_rows = []

if uploaded_files:
    for uploaded_file in uploaded_files:
        content = uploaded_file.read().decode('latin-1', errors='ignore')
        
        # Tu lógica de Regex (se mantiene igual)
        regex_bloque = r'NE Name:\s+(' + target_sites + r').*?\+\+\+\s+\S+\s+(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2}).*?RETCODE = 0.*?Display Board Temperature(.*?)\n---    END'
        blocks = re.findall(regex_bloque, content, re.DOTALL)
        
        for ne_name, fecha, hora, tabla in blocks:
            filas = re.findall(r'(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+([A-Z0-9]+)', tabla)
            for cab, sub, slot, temp, hpa in filas:
                data_rows.append({
                    "Fecha_Hora": f"{fecha} {hora}",
                    "Sitio": ne_name.strip(),
                    "Slot": slot,
                    "Temp_Board": int(temp),
                    "Temp_HPA": hpa
                })

    if data_rows:
        df = pd.DataFrame(data_rows)
        df['Fecha_Hora'] = pd.to_datetime(df['Fecha_Hora'])

        # --- VISUALIZACIÓN ---
        st.subheader(f"Datos del Sitio: {target_sites}")
        
        # Gráfico de Líneas con Plotly
        fig = px.line(df, x="Fecha_Hora", y="Temp_Board", color="Slot", 
                     title="Evolución de Temperatura por Slot",
                     markers=True)
        st.plotly_chart(fig, use_container_width=True)

        # Tabla de datos interactiva
        st.dataframe(df)
    else:
        st.warning("No se encontraron coincidencias con esos sitios.")
