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
    df = df.sort_values(by='Timestamp')

    # --- FILTROS GLOBALES ---
    st.sidebar.header("Filtros Globales")
    lista_sitios = sorted(df['Sitio'].unique())
    lista_slots = sorted(df['Slot'].unique(), key=int)
    slots_seleccionados = st.sidebar.multiselect("Seleccionar Slots", lista_slots, default=lista_slots)

    tab1, tab2 = st.tabs(["📈 Gráficos Históricos", "🚦 Semáforo de Estado"])

    with tab1:
        sitio_hist = st.selectbox("Selecciona Sitio para Histórico", lista_sitios, key="hist")
        df_sitio = df[(df['Sitio'] == sitio_hist) & (df['Slot'].isin(slots_seleccionados))]
        
        fig = px.line(df_sitio, x='Timestamp', y='Board_Temp', color='Slot',
                      title=f"Tendencia Térmica en {sitio_hist}",
                      labels={'Board_Temp': 'Temp °C', 'Timestamp': 'Fecha y Hora'},
                      markers=True)
        st.plotly_chart(fig, use_container_width=True)

    with tab2:
        st.subheader("Estado Actual (Última Lectura)")
        
        # Modo de visualización
        modo = st.radio("Visualización:", ["Ver Todos los Sitios", "Ver Solo un Sitio"], horizontal=True)
        
        df_ultimo = df.sort_values('Timestamp').groupby(['Sitio', 'Slot']).last().reset_index()
        df_ultimo = df_ultimo[df_ultimo['Slot'].isin(slots_seleccionados)]

        def get_status(val):
            if val < 45: return '🟢 Normal'
            elif val < 55: return '🟡 Precaución'
            else: return '🔴 Crítico'

        if modo == "Ver Solo un Sitio":
            sitio_semaforo = st.selectbox("Elegir Sitio", lista_sitios, key="sem_unico")
            df_filtro = df_ultimo[df_ultimo['Sitio'] == sitio_semaforo]
            
            # Mostrar como tarjetas (Métricas)
            cols = st.columns(len(df_filtro) if len(df_filtro) > 0 else 1)
            for i, (index, row) in enumerate(df_filtro.iterrows()):
                with cols[i % len(cols)]:
                    status = get_status(row['Board_Temp'])
                    st.metric(label=f"Slot {row['Slot']}", value=f"{row['Board_Temp']} °C", delta=status, delta_color="off")
            
            st.write(f"**Última actualización de este sitio:** {df_filtro['Timestamp'].max()}")
        
        else:
            # Vista de tabla general
            df_ultimo['Estado'] = df_ultimo['Board_Temp'].apply(get_status)
            st.dataframe(
                df_ultimo[['Sitio', 'Slot', 'Board_Temp', 'Estado', 'Timestamp']].sort_values(by='Board_Temp', ascending=False),
                use_container_width=True, hide_index=True
            )

else:
    st.error("No se encontraron datos.")
