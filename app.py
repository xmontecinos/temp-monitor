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
            # Regex para extraer NE Name, Fecha, Hora y la tabla de temperaturas
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
    # Preparar datos generales
    df['Timestamp'] = pd.to_datetime(df['Fecha'] + ' ' + df['Hora'])
    df = df.sort_values(by='Timestamp')

    # --- FILTROS GLOBALES (Barra Lateral) ---
    st.sidebar.header("Filtros Globales")
    
    # Filtro de Sitio (para la Pestaña 1)
    lista_sitios = sorted(df['Sitio'].unique())
    sitio_seleccionado = st.sidebar.selectbox("Selecciona Sitio", lista_sitios)
    
    # Filtro de Slot (para AMBAS pestañas)
    lista_slots = sorted(df['Slot'].unique(), key=int)
    slots_seleccionados = st.sidebar.multiselect("Seleccionar Slots", lista_slots, default=lista_slots)

    # CREACIÓN DE PESTAÑAS
    tab1, tab2 = st.tabs(["📈 Gráficos Históricos", "🚦 Semáforo por Slot"])

    with tab1:
        st.subheader(f"Análisis de Tendencias: {sitio_seleccionado}")
        # Filtrar por sitio y por los slots seleccionados
        df_sitio = df[(df['Sitio'] == sitio_seleccionado) & (df['Slot'].isin(slots_seleccionados))]
        
        if not df_sitio.empty:
            fig = px.line(df_sitio, x='Timestamp', y='Board_Temp', color='Slot',
                          title=f"Histórico de Temperaturas en {sitio_seleccionado}",
                          labels={'Board_Temp': 'Temp °C', 'Timestamp': 'Fecha y Hora'},
                          markers=True)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No hay datos para los Slots seleccionados en este sitio.")

    with tab2:
        st.subheader("Estado Actual por Slot Seleccionado")
        
        # Obtener el último registro de cada combinación Sitio/Slot
        df_ultimo = df.sort_values('Timestamp').groupby(['Sitio', 'Slot']).last().reset_index()
        
        # Aplicar el filtro de Slots seleccionados en el semáforo
        df_semaforo = df_ultimo[df_ultimo['Slot'].isin(slots_seleccionados)]
        
        # Función para asignar colores
        def get_status(val):
            if val < 45: return '🟢 Normal'
            elif val < 65: return '🟡 Precaución'
            else: return '🔴 Crítico'

        df_semaforo['Estado'] = df_semaforo['Board_Temp'].apply(get_status)
        
        # Métricas resumidas del filtro actual
        m1, m2, m3 = st.columns(3)
        m1.metric("Alertas Críticas", len(df_semaforo[df_semaforo['Board_Temp'] >= 65]))
        m2.metric("Temp Máxima en Selección", f"{df_semaforo['Board_Temp'].max() if not df_semaforo.empty else 0} °C")
        m3.metric("Slots Monitoreados", len(df_semaforo))

        # Tabla dinámica
        st.write("Vista resumida (ordenada por temperatura más alta):")
        st.dataframe(
            df_semaforo[['Sitio', 'Slot', 'Board_Temp', 'Estado', 'Timestamp']].sort_values(by='Board_Temp', ascending=False),
            use_container_width=True,
            hide_index=True
        )

else:
    st.error("No se encontraron datos en la carpeta 'Temperatura'.")
