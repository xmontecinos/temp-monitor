import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="Histórico de Red por Slot", layout="wide")

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
            
            # Extraemos Slot y Temperatura de todo el archivo (todos los sitios)
            filas = re.findall(r'^\s*(\d+)\s+(\d+)\s+(\d+)', content, re.MULTILINE)
            for r in filas:
                rows.append({
                    "Timestamp": ts, 
                    "Slot_Num": int(r[1]),
                    "Temp": int(r[2])
                })
    except: pass
    return rows

# --- INTERFAZ ---
st.title("🌐 Histórico Global: Comportamiento por Slot")
st.markdown("Esta vista consolida todos los sitios para analizar si un Slot específico (ej. Slot 4) calienta más que el resto en toda la red.")

# Sidebar - Procesamiento
with st.sidebar:
    st.header("Admin de Datos")
    if st.button("🚀 Actualizar Histórico de Red"):
        # Importante: Buscamos en la carpeta 'Temperatura' que está al mismo nivel
        archivos = [os.path.join(FOLDER_PATH, f) for f in os.listdir(FOLDER_PATH) if f.endswith('.txt')]
        if archivos:
            all_data = []
            bar = st.progress(0)
            for i, arc in enumerate(archivos):
                all_data.extend(extraer_datos_red(arc))
                bar.progress((i + 1) / len(archivos))
            
            df_global = pd.DataFrame(all_data)
            # Redondeamos a la hora para que los puntos de diferentes sitios coincidan
            df_global['Timestamp'] = df_global['Timestamp'].dt.floor('h')
            df_global.to_parquet(PARQUET_FILE, index=False)
            st.success("¡Base de red actualizada!")
            st.rerun()
        else:
            st.error(f"No se encontraron archivos en la carpeta '{FOLDER_PATH}'")

# --- VISUALIZACIÓN DE RED ---
if os.path.exists(PARQUET_FILE):
    df = pd.read_parquet(PARQUET_FILE)
    
    st.subheader("📈 Tendencia Temporal de la Red")
    
    col_a, col_b = st.columns([1, 4])
    
    with col_a:
        metrica = st.selectbox("Calcular por:", ["Máxima Global", "Promedio Global"])
        todos_slots = sorted(df['Slot_Num'].unique())
        seleccion = st.multiselect("Comparar Slots:", todos_slots, default=todos_slots[:4])
    
    with col_b:
        if seleccion:
            # Agrupamos por TIEMPO y SLOT (esto elimina la distinción por sitio)
            if "Máxima" in metrica:
                df_resumen = df.groupby(['Timestamp', 'Slot_Num'])['Temp'].max().reset_index()
            else:
                df_resumen = df.groupby(['Timestamp', 'Slot_Num'])['Temp'].mean().reset_index()
            
            # Filtrar solo slots seleccionados
            df_plot = df_resumen[df_resumen['Slot_Num'].isin(seleccion)].copy()
            df_plot['Slot'] = df_plot['Slot_Num'].apply(lambda x: f"Slot {x}")

            fig = px.line(
                df_plot, 
                x='Timestamp', 
                y='Temp', 
                color='Slot',
                title=f"Evolución de Temperatura ({metrica}) en toda la Red",
                labels={'Temp': 'Temperatura °C', 'Timestamp': 'Fecha y Hora'},
                markers=True
            )
            
            fig.update_layout(hovermode="x unified")
            st.plotly_chart(fig, use_container_width=True)
            
    st.divider()
    
    # --- BOXPLOT DE DISPERSIÓN ---
    st.subheader("📊 Salud de Slots: Dispersión en la Red")
    st.info("Este gráfico muestra qué tan 'parejos' están los slots en todos los sitios. Si un Slot tiene muchos puntos aislados (outliers) arriba, significa que hay sitios específicos con problemas.")
    
    if seleccion:
        fig_box = px.box(
            df[df['Slot_Num'].isin(seleccion)], 
            x='Slot_Num', 
            y='Temp', 
            color='Slot_Num',
            points="outliers",
            title="Distribución Térmica por Slot (Toda la Red)"
        )
        st.plotly_chart(fig_box, use_container_width=True)

else:
    st.warning("Primero debes actualizar la base de datos desde el panel lateral.")
