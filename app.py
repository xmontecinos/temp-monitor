import streamlit as st
import pandas as pd
import re
import os
import plotly.express as px

st.set_page_config(page_title="Monitor de Temperatura", layout="wide")

# Barra lateral con controles de mantenimiento
st.sidebar.header("Configuración")
if st.sidebar.button("🗑️ Limpiar Caché y Recargar"):
    st.cache_data.clear()
    st.rerun()

st.title("🌡️ Monitor de Temperaturas de Red")

FOLDER_PATH = 'Temperatura'

@st.cache_data(ttl=600)
def procesar_datos_completos(folder):
    rows_list = []
    if not os.path.exists(folder): return None
    
    # Listar y ordenar archivos (del más viejo al más nuevo para el historial)
    archivos = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".txt")]
    archivos.sort(key=os.path.getmtime)
    
    # Limitamos a los últimos 20 archivos para mantener la velocidad
    for path in archivos[-20:]:
        try:
            with open(path, 'r', encoding='latin-1', errors='ignore') as f:
                content = f.read()
                # Captura: NE Name, Fecha, Hora y el bloque de tabla
                blocks = re.findall(r'NE Name:\s+([^\n\r]+).*?(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2}).*?Display Board Temperature(.*?)\n---    END', content, re.DOTALL)
                
                for ne_name, fecha, hora, table_text in blocks:
                    # Captura: Cab(0), Sub(1), Slot(2), Temp(3), HPA(4)
                    rows = re.findall(r'(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(?:\d+|NULL)', table_text)
                    for r in rows:
                        rows_list.append({
                            "Timestamp": pd.to_datetime(f"{fecha} {hora}"),
                            "Sitio": ne_name.strip(),
                            "Subrack": r[1],
                            "Slot": r[2],
                            "Ubicacion": f"Sub:{r[1]} - Slot:{r[2]}", # Etiqueta combinada
                            "Temp": int(r[3])
                        })
        except:
            continue
    return pd.DataFrame(rows_list)

# --- EJECUCIÓN ---
with st.spinner('Analizando datos y subracks...'):
    df = procesar_datos_completos(FOLDER_PATH)

if df is not None and not df.empty:
    sitios = sorted(df['Sitio'].unique())
    sitio_sel = st.selectbox("Seleccione el Sitio a monitorear:", sitios)
    
    df_sitio = df[df['Sitio'] == sitio_sel].copy()
    
    # --- CREACIÓN DE LAS 2 PESTAÑAS ---
    tab1, tab2 = st.tabs(["🚦 Estado Actual (Semáforo)", "📈 Histórico de Tendencias"])

    with tab1:
        # Obtenemos solo la lectura más reciente para este sitio
        ultimo_ts = df_sitio['Timestamp'].max()
        df_ahora = df_sitio[df_sitio['Timestamp'] == ultimo_ts].sort_values(['Subrack', 'Slot'])
        
        st.subheader(f"Última lectura detectada: {ultimo_ts}")
        
        # Grid de tarjetas
        cols = st.columns(4)
        for i, (_, row) in enumerate(df_ahora.iterrows()):
            t = row['Temp']
            # Colores basados en criticidad
            if t < 45: bg, txt, icon = "#C6EFCE", "#006100", "🟢"
            elif t < 55: bg, txt, icon = "#FFEB9C", "#9C6500", "🟡"
            else: bg, txt, icon = "#FFC7CE", "#9C0006", "🔴"

            with cols[i % 4]:
                st.markdown(f"""
                    <div style="background-color:{bg}; color:{txt}; padding:15px; border-radius:10px; text-align:center; border:1px solid {txt}40; margin-bottom:10px;">
                        <p style="margin:0; font-size:12px; font-weight:bold;">SUBRACK {row['Subrack']}</p>
                        <p style="margin:0; font-size:14px;">SLOT {row['Slot']}</p>
                        <h2 style="margin:5px 0;">{t}°C</h2>
                        <span style="font-size:12px;">{icon} { "CRÍTICO" if t >= 55 else "NORMAL" }</span>
                    </div>
                """, unsafe_allow_html=True)

    with tab2:
        st.subheader("Evolución de Temperaturas")
        # Graficamos usando la ubicación combinada para que no se mezclen slots iguales de distintos subracks
        fig = px.line(df_sitio, 
                     x='Timestamp', 
                     y='Temp', 
                     color='Ubicacion',
                     markers=True,
                     title=f"Histórico Térmico - {sitio_sel}",
                     labels={'Ubicacion': 'Componente', 'Temp': 'Temp °C'})
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Tabla detallada al final
        with st.expander("Ver bitácora de datos completa"):
            st.dataframe(df_sitio.sort_values('Timestamp', ascending=False), use_container_width=True)

else:
    st.warning("No se encontraron archivos válidos en la carpeta 'Temperatura'. Verifique el formato.")
