import streamlit as st
import pandas as pd
import re
import os
import plotly.express as px

st.set_page_config(page_title="Monitor de Temperaturas", layout="wide")
st.title("🌡️ Monitor de Temperaturas de Red")

FOLDER_PATH = 'Temperatura' 

# --- MEMORIA CACHE PARA VELOCIDAD ---
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
            # Regex mejorada para capturar el bloque de datos correctamente
            blocks = re.findall(r'NE Name:\s+([^\n\r]+).*?\+\+\+\s+\S+\s+(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2}).*?Display Board Temperature(.*?)\n---    END', content, re.DOTALL)
            
            for ne_name, fecha, hora, table_text in blocks:
                # Regex corregida: permite números y la palabra 'NULL' en las columnas finales
                rows = re.findall(r'(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+([A-Z0-9|NULL]+)', table_text)
                for cab, sub, slot, b_temp, h_temp in rows:
                    rows_list.append({
                        "Fecha": fecha, 
                        "Hora": hora, 
                        "Sitio": ne_name.strip(),
                        "Board_Temp": int(b_temp), 
                        "Slot": int(slot) # Convertimos a entero para ordenar correctamente
                    })
    
    return pd.DataFrame(rows_list) if rows_list else None

df = procesar_datos(FOLDER_PATH)

if df is not None and not df.empty:
    df['Timestamp'] = pd.to_datetime(df['Fecha'] + ' ' + df['Hora'])
    df = df.sort_values(by='Timestamp')

    # --- FILTROS GLOBALES ---
    st.sidebar.header("Filtros Globales")
    lista_sitios = sorted(df['Sitio'].unique())
    lista_slots = sorted(df['Slot'].unique())
    slots_seleccionados = st.sidebar.multiselect("Seleccionar Slots", lista_slots, default=lista_slots)

    tab1, tab2 = st.tabs(["📈 Gráficos Históricos", "🚦 Semáforo de Estado"])

    with tab1:
        sitio_hist = st.selectbox("Selecciona Sitio para Histórico", lista_sitios, key="hist")
        df_sitio = df[(df['Sitio'] == sitio_hist) & (df['Slot'].isin(slots_seleccionados))]
        
        if not df_sitio.empty:
            # Aseguramos que Slot sea tratado como categoría para el color del gráfico
            df_sitio['Slot'] = df_sitio['Slot'].astype(str)
            fig = px.line(df_sitio, x='Timestamp', y='Board_Temp', color='Slot',
                          title=f"Tendencia Térmica en {sitio_hist}",
                          labels={'Board_Temp': 'Temp °C', 'Timestamp': 'Fecha y Hora'},
                          markers=True)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No hay datos para los filtros seleccionados.")

    with tab2:
        st.subheader("Estado Actual (Última Lectura)")
        
        modo = st.radio("Visualización:", ["Ver Todos los Sitios", "Ver Solo un Sitio"], horizontal=True)
        
        # Obtener la última lectura por Sitio y Slot
        df_ultimo = df.sort_values('Timestamp').groupby(['Sitio', 'Slot']).last().reset_index()
        df_ultimo = df_ultimo[df_ultimo['Slot'].isin(slots_seleccionados)]

        def get_status(val):
            if val < 45: return 'Normal'
            elif val < 55: return 'Precaución'
            else: return 'Crítico'

        if modo == "Ver Solo un Sitio":
            sitio_semaforo = st.selectbox("Elegir Sitio", lista_sitios, key="sem_unico")
            df_filtro = df_ultimo[df_ultimo['Sitio'] == sitio_semaforo].sort_values('Slot')
            
            if not df_filtro.empty:
                # Grid dinámico: máximo 4 tarjetas por fila
                rows_metrics = [df_filtro.iloc[i:i+4] for i in range(0, len(df_filtro), 4)]
                for row_data in rows_metrics:
                    cols = st.columns(4)
                    for i, (index, row) in enumerate(row_data.iterrows()):
                        status_label = get_status(row['Board_Temp'])
                        # Usamos el color inverso para que >55 sea rojo
                        st.metric(
                            label=f"Slot {row['Slot']}", 
                            value=f"{row['Board_Temp']} °C", 
                            delta=status_label, 
                            delta_color="inverse" if status_label == 'Crítico' else "normal"
                        )
                
                st.info(f"**Última actualización detectada:** {df_filtro['Timestamp'].max()}")
            else:
                st.warning("No hay datos recientes para este sitio.")
        
        else:
            df_ultimo['Estado'] = df_ultimo['Board_Temp'].apply(get_status)
            st.dataframe(
                df_ultimo[['Sitio', 'Slot', 'Board_Temp', 'Estado', 'Timestamp']].sort_values(by=['Sitio', 'Slot']),
                use_container_width=True, hide_index=True
            )

else:
    st.error("No se encontraron datos en la carpeta 'Temperatura'. Verifique que los archivos .txt existan.")
