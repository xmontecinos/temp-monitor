import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px
import gc

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Histórico Global de Red", layout="wide")

# Ruta absoluta corregida
FOLDER_PATH = r'D:\Temperaturas\temperaturas\temp-monitor\Temperatura\temp-monitor\Temperatura'
PARQUET_FILE = 'red_historico_hardware.parquet'
UMBRAL_CRITICO = 78

def extraer_datos_estrictos(path):
    rows = []
    try:
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            content = f.read()
            
            # 1. Extraer Timestamp del encabezado del archivo
            ts_match = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', content)
            if not ts_match: return []
            ts = pd.to_datetime(f"{ts_match.group(1)} {ts_match.group(2)}")
            
            # 2. Buscar líneas que tengan exactamente el formato de la tabla de temperaturas
            # Buscamos: [Espacios] Numero [Espacios] Numero [Espacios] Numero
            # Esto captura: Subrack No. | Slot No. | Board Temperature
            filas = re.findall(r'^\s*(\d+)\s+(\d+)\s+(\d+)', content, re.MULTILINE)
            
            for r in filas:
                subrack = r[0]
                slot = r[1]
                temp = int(r[2])
                
                # Solo guardamos si la temperatura es lógica (ej: > 5°C) 
                # para evitar capturar IDs de slots como si fueran temperaturas
                if temp > 5:
                    rows.append({
                        "Timestamp": ts, 
                        "Hardware_ID": f"S{subrack}-L{slot}", 
                        "Temp": temp
                    })
    except Exception:
        pass
    return rows

# --- INTERFAZ ---
st.title("🌐 Histórico de Red por Subrack y Slot")
st.markdown(f"**Origen de datos:** `{FOLDER_PATH}`")

with st.sidebar:
    st.header("⚙️ Gestión de Base de Datos")
    if st.button("🚀 Regenerar Base de Red (Desde Cero)"):
        if os.path.exists(PARQUET_FILE):
            os.remove(PARQUET_FILE) # Borramos el anterior para limpiar errores
            
        archivos = [os.path.join(FOLDER_PATH, f) for f in os.listdir(FOLDER_PATH) if f.endswith('.txt')]
        if archivos:
            all_data = []
            bar = st.progress(0)
            for i, arc in enumerate(archivos):
                all_data.extend(extraer_datos_estrictos(arc))
                bar.progress((i + 1) / len(archivos))
            
            if all_data:
                df_global = pd.DataFrame(all_data)
                # Redondeo horario para que todos los sitios se unan en el mismo punto del tiempo
                df_global['Timestamp'] = df_global['Timestamp'].dt.floor('H')
                df_global.to_parquet(PARQUET_FILE, index=False)
                st.success("✅ Base de red sincronizada.")
                st.rerun()
            else:
                st.error("No se encontraron datos válidos. Revisa el formato de los archivos TXT.")

# --- VISUALIZACIÓN ---
if os.path.exists(PARQUET_FILE):
    df = pd.read_parquet(PARQUET_FILE)
    
    st.subheader("📈 Tendencia Térmica de la Red")
    
    # Métrica: Promedio de todos los sitios o el máximo de la red
    metrica = st.radio("Cálculo para la Red:", ["Promedio de Red", "Máximo de Red"], horizontal=True)
    
    hw_ids = sorted(df['Hardware_ID'].unique())
    seleccion = st.multiselect("Seleccione Hardware (Subrack-Slot):", hw_ids, default=hw_ids[:5] if len(hw_ids) > 5 else hw_ids)

    if seleccion:
        if "Promedio" in metrica:
            df_plot = df.groupby(['Timestamp', 'Hardware_ID'])['Temp'].mean().reset_index()
        else:
            df_plot = df.groupby(['Timestamp', 'Hardware_ID'])['Temp'].max().reset_index()
            
        df_plot = df_plot[df_plot['Hardware_ID'].isin(seleccion)]
        
        fig = px.line(
            df_plot, x='Timestamp', y='Temp', color='Hardware_ID',
            markers=True, title=f"Evolución de {metrica} por Componente",
            labels={'Temp': 'Grados Celsius (°C)', 'Hardware_ID': 'ID (Subrack-Slot)'}
        )
        fig.add_hline(y=UMBRAL_CRITICO, line_dash="dash", line_color="red", annotation_text="CRÍTICO")
        fig.update_layout(hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)

        # Resumen informativo
        st.subheader("📋 Resumen de Hardware")
        resumen = df_plot.groupby('Hardware_ID')['Temp'].agg(['max', 'mean']).reset_index()
        resumen.columns = ['ID Hardware', 'Temp Máxima Global', 'Temp Promedio Global']
        st.dataframe(resumen.style.format(precision=1), use_container_width=True)
else:
    st.warning("👈 Haz clic en el botón de la izquierda para procesar la red por primera vez.")
