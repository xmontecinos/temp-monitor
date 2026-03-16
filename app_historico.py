import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px

st.set_page_config(page_title="Histórico Global de Red", layout="wide")

FOLDER_PATH = 'Temperatura'
PARQUET_FILE = 'red_historico_slots.parquet'

def extraer_datos_red(path):
    rows = []
    try:
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            content = f.read()
            ts_match = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', content)
            if not ts_match: return []
            ts = pd.to_datetime(f"{ts_match.group(1)} {ts_match.group(2)}")
            
            # Buscamos Subrack (r[0]), Slot (r[1]) y Temp (r[2]) en todo el archivo
            filas = re.findall(r'^\s*(\d+)\s+(\d+)\s+(\d+)', content, re.MULTILINE)
            for r in filas:
                rows.append({
                    "Timestamp": ts, 
                    "Slot_ID": f"Slot {r[1]}", # Agrupamos por número de slot
                    "Temp": int(r[2])
                })
    except: pass
    return rows

# --- LÓGICA DE DATOS ---
with st.sidebar:
    st.header("Configuración de Red")
    if st.button("🔄 Actualizar Base de Red"):
        archivos = [os.path.join(FOLDER_PATH, f) for f in os.listdir(FOLDER_PATH) if f.endswith('.txt')]
        if archivos:
            all_data = []
            progreso = st.progress(0)
            for i, arc in enumerate(archivos):
                all_data.extend(extraer_datos_red(arc))
                progreso.progress((i + 1) / len(archivos))
            
            df_global = pd.DataFrame(all_data)
            # Redondeamos a la hora para sincronizar todos los sitios de la red
            df_global['Timestamp'] = df_global['Timestamp'].dt.floor('H')
            df_global.to_parquet(PARQUET_FILE, index=False)
            st.success("Base de red lista.")
            st.rerun()

# --- GENERACIÓN DEL GRÁFICO TIPO "SITIO" PERO DE RED ---
if os.path.exists(PARQUET_FILE):
    df = pd.read_parquet(PARQUET_FILE)
    
    st.title("🌐 Evolución Térmica Histórica: Red Global")
    
    # 1. Selector de métrica (Promedio es mejor para ver la tendencia de red)
    metrica = st.radio("Visualizar por:", ["Promedio de Red", "Máximo de Red"], horizontal=True)
    
    # 2. Selector de Slots (como en tu imagen)
    slots_disponibles = sorted(df['Slot_ID'].unique(), key=lambda x: int(re.findall(r'\d+', x)[0]))
    seleccion = st.multiselect("Comparar Slots de la Red:", slots_disponibles, default=slots_disponibles[:5])
    
    if seleccion:
        # AGREGACIÓN: Aquí es donde ocurre la magia de "Red"
        if metrica == "Promedio de Red":
            df_resumen = df.groupby(['Timestamp', 'Slot_ID'])['Temp'].mean().reset_index()
        else:
            df_resumen = df.groupby(['Timestamp', 'Slot_ID'])['Temp'].max().reset_index()
            
        # Filtramos por los slots elegidos
        df_plot = df_resumen[df_resumen['Slot_ID'].isin(seleccion)]
        
        # 3. Gráfico idéntico al de tu imagen
        fig = px.line(
            df_plot, 
            x='Timestamp', 
            y='Temp', 
            color='Slot_ID',
            title=f"Comportamiento de {metrica} por Slot",
            labels={'Temp': 'Temperatura °C', 'Timestamp': 'Línea de Tiempo'},
            markers=True # Para que se vea con los puntos como en tu captura
        )
        
        # Estética similar a tu imagen
        fig.update_layout(
            hovermode="x unified",
            legend_title="ID_Slot (Red)",
            yaxis_title="Temp (°C)"
        )
        # Añadimos la línea roja de umbral crítico
        fig.add_hline(y=78, line_dash="dash", line_color="red", annotation_text="UMBRAL CRÍTICO")
        
        st.plotly_chart(fig, use_container_width=True)
        
        # --- TABLA DE APOYO ---
        st.subheader("📋 Resumen de Desempeño de Red")
        tabla = df_plot.groupby('Slot_ID')['Temp'].agg(['max', 'mean']).reset_index()
        tabla.columns = ['Slot', 'Temp Máxima Global', 'Temp Promedio Global']
        st.dataframe(tabla.style.format(precision=1), use_container_width=True)

else:
    st.info("👈 Por favor, haz clic en 'Actualizar Base de Red' en el panel lateral para procesar los archivos.")
