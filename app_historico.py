import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px
import gc

# --- 1. CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Histórico Global de Red", layout="wide")

# --- 2. PARÁMETROS Y RUTAS ---
# Ruta absoluta según tu configuración
FOLDER_PATH = r'D:\Temperaturas\temperaturas\temp-monitor\Temperatura\temp-monitor\Temperatura'
PARQUET_FILE = 'red_historico_slots.parquet'
UMBRAL_CRITICO = 78

# --- 3. MOTOR DE EXTRACCIÓN ---
def extraer_datos_red(path):
    rows = []
    try:
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            content = f.read()
            # Extraer Timestamp
            ts_match = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', content)
            if not ts_match: return []
            ts = pd.to_datetime(f"{ts_match.group(1)} {ts_match.group(2)}")
            
            # Regex flexible para capturar Slot (r[0]) y Temperatura (r[1])
            # Saltamos la primera columna (índice interno) y capturamos las dos siguientes
            filas = re.findall(r'^\s*\d+\s+(\d+)\s+(\d+)', content, re.MULTILINE)
            for r in filas:
                slot_n = int(r[0])
                temp_v = int(r[1])
                
                # Filtramos slots lógicos/sensores secundarios (normalmente slots > 20)
                if slot_n < 20:
                    rows.append({
                        "Timestamp": ts, 
                        "Slot_ID": f"Slot {slot_n}", 
                        "Temp": temp_v
                    })
    except Exception:
        pass
    return rows

# --- 4. INTERFAZ DE USUARIO ---
st.title("🌐 Evolución Térmica Histórica: Red Global")
st.markdown(f"**Directorio de datos:** `{FOLDER_PATH}`")

# Sidebar para gestión de datos
with st.sidebar:
    st.header("⚙️ Configuración de Red")
    if st.button("🔄 Actualizar Base de Red"):
        if not os.path.exists(FOLDER_PATH):
            st.error("La ruta especificada no existe.")
        else:
            archivos = [os.path.join(FOLDER_PATH, f) for f in os.listdir(FOLDER_PATH) if f.endswith('.txt')]
            if not archivos:
                st.warning("No se encontraron archivos .txt en la carpeta.")
            else:
                all_data = []
                progreso = st.progress(0)
                status = st.empty()
                
                for i, arc in enumerate(archivos):
                    status.text(f"Procesando: {os.path.basename(arc)}")
                    data = extraer_datos_red(arc)
                    if data:
                        all_data.extend(data)
                    progreso.progress((i + 1) / len(archivos))
                    if i % 50 == 0: gc.collect()
                
                if all_data:
                    df_global = pd.DataFrame(all_data)
                    # Sincronización horaria para agrupar todos los sitios de la red
                    df_global['Timestamp'] = df_global['Timestamp'].dt.floor('H')
                    df_global.to_parquet(PARQUET_FILE, index=False)
                    st.success(f"✅ ¡Base de red lista con {len(df_global)} registros!")
                    st.rerun()
                else:
                    st.error("No se pudieron extraer datos válidos.")

# --- 5. VISUALIZACIÓN ---
if os.path.exists(PARQUET_FILE):
    df = pd.read_parquet(PARQUET_FILE)
    
    # Filtros superiores
    col_fil1, col_fil2 = st.columns([1, 3])
    
    with col_fil1:
        metrica = st.radio("Visualizar por:", ["Promedio de Red", "Máximo de Red"], horizontal=True)
        
        # Ordenar slots numéricamente para la lista
        slots_disponibles = sorted(df['Slot_ID'].unique(), 
                                  key=lambda x: int(re.findall(r'\d+', x)[0]))
        
        seleccion = st.multiselect(
            "Comparar Slots de la Red:", 
            slots_disponibles, 
            default=slots_disponibles[:4] if len(slots_disponibles) > 4 else slots_disponibles
        )

    with col_fil2:
        if seleccion:
            # Agregación según métrica seleccionada
            if metrica == "Promedio de Red":
                df_resumen = df.groupby(['Timestamp', 'Slot_ID'])['Temp'].mean().reset_index()
            else:
                df_resumen = df.groupby(['Timestamp', 'Slot_ID'])['Temp'].max().reset_index()
            
            # Filtrar por selección
            df_plot = df_resumen[df_resumen['Slot_ID'].isin(seleccion)]
            
            # Gráfico de líneas (estilo el que enviaste en la imagen)
            fig = px.line(
                df_plot, 
                x='Timestamp', 
                y='Temp', 
                color='Slot_ID',
                title=f"Tendencia Térmica de Red ({metrica})",
                markers=True,
                template="plotly_white"
            )
            
            # Línea de Umbral Crítico
            fig.add_hline(y=UMBRAL_CRITICO, line_dash="dash", line_color="red", 
                          annotation_text="UMBRAL CRÍTICO", annotation_position="top left")
            
            fig.update_layout(
                hovermode="x unified",
                yaxis_title="Temperatura (°C)",
                xaxis_title="Fecha y Hora (Sincronizada)"
            )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Seleccione al menos un slot para visualizar la tendencia.")

    # --- 6. TABLA DE RESUMEN ---
    st.divider()
    st.subheader("📋 Resumen de Desempeño por Slot (Toda la Red)")
    if not df.empty:
        # Mostramos los datos consolidados de los slots seleccionados
        resumen = df[df['Slot_ID'].isin(seleccion)].groupby('Slot_ID')['Temp'].agg(['max', 'mean', 'min']).reset_index()
        resumen.columns = ['Slot', 'Máximo Global (°C)', 'Promedio Global (°C)', 'Mínimo Global (°C)']
        st.dataframe(resumen.style.format(precision=1).highlight_max(subset=['Máximo Global (°C)'], color='#ffcccc'), use_container_width=True)

else:
    st.warning("👈 La base de datos no existe. Por favor, haga clic en 'Actualizar Base de Red' en el panel lateral.")
