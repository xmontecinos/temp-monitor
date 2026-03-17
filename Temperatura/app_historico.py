import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Monitor Térmico Huawei", layout="wide")

# --- RUTAS ---
# Verifica que esta ruta sea la correcta en tu PC
FOLDER_PATH = r'D:\Temperaturas\temperaturas\temp-monitor\Temperatura\temp-monitor\Temperatura'
PARQUET_FILE = 'historico_temperaturas_v2.parquet'

def extraer_datos(path):
    """Extrae Subrack, Slot y Temperatura de archivos log de Huawei."""
    rows = []
    try:
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            content = f.read()
            
            # 1. Intentar extraer Fecha y Hora del contenido del archivo
            ts_match = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', content)
            if ts_match:
                ts = pd.to_datetime(f"{ts_match.group(1)} {ts_match.group(2)}")
            else:
                # Si no hay fecha en el texto, usamos la fecha de modificación del archivo
                ts = pd.to_datetime(os.path.getmtime(path), unit='s')

            # 2. Capturar tabla: Subrack No. | Slot No. | Temperature
            # Buscamos líneas que empiecen con espacios y tengan al menos 3 grupos de números
            filas = re.findall(r'^\s*(\d+)\s+(\d+)\s+(\d+)', content, re.MULTILINE)
            
            for r in filas:
                subrack = r[0]
                slot = r[1]
                temp_val = int(r[2])
                
                # Filtro: Evitamos IDs bajos (como 0 o 1) y ruidos extremos
                if 15 < temp_val < 120:
                    rows.append({
                        "Timestamp": ts,
                        "Subrack": subrack,
                        "Slot": slot,
                        "HW_ID": f"Subrack {subrack} - Slot {slot}",
                        "Temperatura": temp_val
                    })
    except Exception as e:
        print(f"Error procesando {os.path.basename(path)}: {e}")
    return rows

# --- INTERFAZ DE USUARIO ---
st.title("📊 Histórico de Temperaturas por Hardware")
st.markdown(f"**Directorio de logs:** `{FOLDER_PATH}`")

# Barra lateral para control de datos
with st.sidebar:
    st.header("⚙️ Administración")
    if st.button("🚀 Sincronizar Histórico"):
        if not os.path.exists(FOLDER_PATH):
            st.error(f"La ruta no existe: {FOLDER_PATH}")
        else:
            archivos = [os.path.join(FOLDER_PATH, f) for f in os.listdir(FOLDER_PATH) if f.endswith('.txt')]
            if archivos:
                all_data = []
                progreso = st.progress(0)
                status_text = st.empty()
                
                for i, arc in enumerate(archivos):
                    datos_archivo = extraer_datos(arc)
                    all_data.extend(datos_archivo)
                    progreso.progress((i + 1) / len(archivos))
                    status_text.text(f"Procesando {i+1}/{len(archivos)}")
                
                if all_data:
                    df_new = pd.DataFrame(all_data)
                    # Limpieza: Asegurar que Timestamp sea datetime y quitar duplicados
                    df_new['Timestamp'] = pd.to_datetime(df_new['Timestamp']).dt.floor('min')
                    df_new = df_new.drop_duplicates(subset=['Timestamp', 'HW_ID'])
                    
                    # Guardar
                    df_new.to_parquet(PARQUET_FILE, index=False)
                    st.success(f"✅ ¡Éxito! {len(df_new)} registros procesados.")
                    st.rerun()
                else:
                    st.warning("No se encontraron datos válidos dentro de los archivos.")
            else:
                st.error("No se encontraron archivos .txt en la carpeta.")

# --- VISUALIZACIÓN ---
if os.path.exists(PARQUET_FILE):
    df = pd.read_parquet(PARQUET_FILE)
    
    # Filtros superiores
    col1, col2 = st.columns([1, 3])
    
    with col1:
        subracks = sorted(df['Subrack'].unique(), key=int)
        sub_sel = st.multiselect("Seleccionar Subracks:", subracks, default=subracks)
    
    with col2:
        df_filtrado = df[df['Subrack'].isin(sub_sel)]
        ids_disponibles = sorted(df_filtrado['HW_ID'].unique())
        seleccion = st.multiselect("Seleccionar Slots para graficar:", ids_disponibles, default=ids_disponibles[:5])

    if seleccion:
        # Ordenar por tiempo para que la línea sea continua
        df_plot = df_filtrado[df_filtrado['HW_ID'].isin(seleccion)].sort_values('Timestamp')
        
        fig = px.line(
            df_plot,
            x='Timestamp',
            y='Temperatura',
            color='HW_ID',
            markers=True,
            template="plotly_white",
            labels={'Temperatura': 'Temp (°C)', 'Timestamp': 'Fecha y Hora'},
            title="Evolución Térmica por Slot"
        )
        
        # Umbral crítico
        fig.add_hline(y=78, line_dash="dash", line_color="red", annotation_text="Límite Crítico (78°C)")
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Tabla detallada opcional
        with st.expander("📂 Ver datos detallados"):
            st.dataframe(df_plot.sort_values('Timestamp', ascending=False), use_container_width=True)
else:
    st.info("👈 Haz clic en el botón 'Sincronizar Histórico' para cargar los datos por primera vez.")
