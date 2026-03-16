import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px
import gc

# --- CONFIGURACIÓN ---
st.set_page_config(page_title="Histórico Global de Red por Hardware", layout="wide")

# Rutas basadas en tus capturas de pantalla
FOLDER_PATH = r'D:\Temperaturas\temperaturas\temp-monitor\Temperatura\temp-monitor\Temperatura'
PARQUET_FILE = 'red_historico_consolidado.parquet'
UMBRAL_CRITICO = 78

def extraer_datos_red(path):
    rows = []
    try:
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            content = f.read()
            # Extraer Timestamp
            ts_match = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', content)
            if not ts_match: return []
            ts = pd.to_datetime(f"{ts_match.group(1)} {ts_match.group(2)}")
            
            # Capturar Subrack, Slot y Temperatura
            filas = re.findall(r'^\s*(\d+)\s+(\d+)\s+(\d+)', content, re.MULTILINE)
            for r in filas:
                subrack, slot, temp = r[0], r[1], int(r[2])
                # Filtro de seguridad para evitar datos erróneos de IDs capturados como temperaturas
                if temp > 15:
                    rows.append({
                        "Timestamp": ts, 
                        "Hardware_ID": f"S:{subrack}-L:{slot}", 
                        "Temp": temp
                    })
    except: pass
    return rows

# --- INTERFAZ ---
st.title("🌐 Evolución Térmica Histórica: Red Global")
st.info(f"Procesando datos de: {FOLDER_PATH}")

with st.sidebar:
    st.header("Gestión de Base de Datos")
    if st.button("🔥 Generar/Actualizar Base Global"):
        archivos = [os.path.join(FOLDER_PATH, f) for f in os.listdir(FOLDER_PATH) if f.endswith('.txt')]
        if archivos:
            all_data = []
            bar = st.progress(0)
            for i, arc in enumerate(archivos):
                all_data.extend(extraer_datos_red(arc))
                bar.progress((i + 1) / len(archivos))
            
            if all_data:
                df_global = pd.DataFrame(all_data)
                # Sincronizamos por hora para unir todos los sitios en los mismos puntos temporales
                df_global['Timestamp'] = df_global['Timestamp'].dt.floor('H')
                df_global.to_parquet(PARQUET_FILE, index=False)
                st.success("✅ Base de red sincronizada.")
                st.rerun()

# --- VISUALIZACIÓN ---
if os.path.exists(PARQUET_FILE):
    df_full = pd.read_parquet(PARQUET_FILE)
    
    st.divider()
    
    # 1. Selección de Métrica Global
    metrica = st.radio("Cálculo para la Red:", ["Promedio de Red", "Máximo de Red"], horizontal=True)
    
    # 2. Agrupación por Hardware y Tiempo (Promediando o Maximizando todos los sitios)
    if "Promedio" in metrica:
        df_plot_base = df_full.groupby(['Timestamp', 'Hardware_ID'])['Temp'].mean().reset_index()
    else:
        df_plot_base = df_full.groupby(['Timestamp', 'Hardware_ID'])['Temp'].max().reset_index()

    # 3. Selector de Hardware (Estilo segunda imagen)
    hw_ids = sorted(df_plot_base['Hardware_ID'].unique(), key=lambda x: [int(c) for c in re.findall(r'\d+', x)])
    seleccion = st.multiselect("Comparar Hardware de la Red (Subrack-Slot):", 
                               hw_ids, 
                               default=hw_ids[:5] if len(hw_ids) > 5 else hw_ids)

    if seleccion:
        df_final = df_plot_base[df_plot_base['Hardware_ID'].isin(seleccion)]
        
        # 4. Gráfico idéntico a la segunda imagen
        fig = px.line(
            df_final, 
            x='Timestamp', 
            y='Temp', 
            color='Hardware_ID',
            title=f"Tendencia Térmica de Red ({metrica})",
            markers=True,
            template="plotly_white"
        )
        
        fig.add_hline(y=UMBRAL_CRITICO, line_dash="dash", line_color="red", annotation_text="CRÍTICO")
        fig.update_layout(
            hovermode="x unified", 
            yaxis_title="Temperatura (°C)",
            legend_title="Hardware ID"
        )
        
        st.plotly_chart(fig, use_container_width=True)

        # 5. Tabla de Resumen Global
        st.subheader("📋 Resumen de Desempeño por Componente (Toda la Red)")
        resumen = df_final.groupby('Hardware_ID')['Temp'].agg(['max', 'mean', 'min']).reset_index()
        resumen.columns = ['Hardware', 'Máx Global', 'Promedio Global', 'Mín Global']
        st.dataframe(resumen.style.format(precision=1).highlight_max(subset=['Máx Global'], color='#ffcccc'), use_container_width=True)
else:
    st.warning("👈 Pulsa en 'Generar/Actualizar Base Global' para procesar los archivos de red.")
