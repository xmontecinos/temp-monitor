import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px
import gc

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Histórico Global de Red", layout="wide")

# Ruta absoluta basada en tus capturas
FOLDER_PATH = r'D:\Temperaturas\temperaturas\temp-monitor\Temperatura\temp-monitor\Temperatura'
PARQUET_FILE = 'red_historico_hardware.parquet'
UMBRAL_CRITICO = 78

def extraer_datos_precisos(path):
    rows = []
    try:
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            content = f.read()
            
            # 1. Extraer Timestamp (Fecha y Hora)
            ts_match = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', content)
            if not ts_match: return []
            ts = pd.to_datetime(f"{ts_match.group(1)} {ts_match.group(2)}")
            
            # 2. Buscar las filas de datos. 
            # El patrón busca líneas que tengan al menos 3 columnas numéricas.
            # Usamos una regex que valide que la temperatura sea el tercer número.
            filas = re.findall(r'^\s*(\d+)\s+(\d+)\s+(\d+)', content, re.MULTILINE)
            
            for r in filas:
                subrack = int(r[0])
                slot = int(r[1])
                temp = int(r[2])
                
                # FILTRO CRÍTICO: 
                # En tus archivos, si el tercer número es menor a 15, 
                # probablemente capturó un ID y no una temperatura real.
                if temp > 15 and slot < 30: 
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
st.markdown(f"**Análisis de Flota en:** `{FOLDER_PATH}`")

with st.sidebar:
    st.header("⚙️ Gestión de Datos")
    if st.button("🚀 Regenerar Base de Red (Desde Cero)"):
        # Borramos el parquet anterior para evitar datos residuales erróneos
        if os.path.exists(PARQUET_FILE):
            os.remove(PARQUET_FILE)
            
        archivos = [os.path.join(FOLDER_PATH, f) for f in os.listdir(FOLDER_PATH) if f.endswith('.txt')]
        
        if archivos:
            all_data = []
            bar = st.progress(0)
            status = st.empty()
            
            for i, arc in enumerate(archivos):
                status.text(f"Procesando {i+1}/{len(archivos)}")
                all_data.extend(extraer_datos_precisos(arc))
                bar.progress((i + 1) / len(archivos))
            
            if all_data:
                df_global = pd.DataFrame(all_data)
                # Sincronización horaria para unir todos los sitios
                df_global['Timestamp'] = df_global['Timestamp'].dt.floor('H')
                df_global.to_parquet(PARQUET_FILE, index=False)
                st.success("✅ ¡Base de datos reconstruida!")
                st.rerun()
            else:
                st.error("No se encontraron datos válidos. Verifica el contenido de los TXT.")

# --- VISUALIZACIÓN ---
if os.path.exists(PARQUET_FILE):
    df = pd.read_parquet(PARQUET_FILE)
    
    st.subheader("📈 Tendencia Térmica de la Red")
    
    col1, col2 = st.columns([1, 4])
    
    with col1:
        metrica = st.radio("Cálculo Global:", ["Promedio de Red", "Máximo de Red"])
        hw_ids = sorted(df['Hardware_ID'].unique(), key=lambda x: [int(c) for c in re.findall(r'\d+', x)])
        seleccion = st.multiselect("Seleccione Hardware (S=Subrack, L=Slot):", hw_ids, default=hw_ids[:3])

    with col2:
        if seleccion:
            # Agregamos por Hardware y Tiempo
            if "Promedio" in metrica:
                df_plot = df.groupby(['Timestamp', 'Hardware_ID'])['Temp'].mean().reset_index()
            else:
                df_plot = df.groupby(['Timestamp', 'Hardware_ID'])['Temp'].max().reset_index()
            
            df_plot = df_plot[df_plot['Hardware_ID'].isin(seleccion)]
            
            fig = px.line(
                df_plot, x='Timestamp', y='Temp', color='Hardware_ID',
                markers=True, title=f"Evolución de {metrica}",
                labels={'Temp': 'Grados Celsius (°C)', 'Timestamp': 'Fecha (Sincronizada)'},
                color_discrete_sequence=px.colors.qualitative.Safe
            )
            
            fig.add_hline(y=UMBRAL_CRITICO, line_dash="dash", line_color="red", annotation_text="UMBRAL CRÍTICO")
            fig.update_layout(hovermode="x unified", yaxis_range=[20, 90]) # Ajuste de escala para ver variaciones
            st.plotly_chart(fig, use_container_width=True)

    # --- TABLA DE RESUMEN ---
    st.divider()
    st.subheader("📋 Resumen de Hardware de Red")
    resumen = df[df['Hardware_ID'].isin(seleccion)].groupby('Hardware_ID')['Temp'].agg(['max', 'mean']).reset_index()
    resumen.columns = ['Componente', 'Máximo Histórico (°C)', 'Promedio General (°C)']
    st.dataframe(resumen.style.format(precision=1), use_container_width=True)

else:
    st.info("👈 Presiona el botón para procesar los archivos y ver el histórico.")
