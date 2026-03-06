import streamlit as st
import pandas as pd
import re
import os
import plotly.express as px

st.set_page_config(page_title="Monitor Térmico", layout="wide")

# Botón lateral para resetear si algo falla
if st.sidebar.button("🔄 Forzar Recarga Total"):
    st.cache_data.clear()
    st.rerun()

st.title("🌡️ Monitor de Temperaturas de Red")

FOLDER_PATH = 'Temperatura'

@st.cache_data(ttl=600) # El caché se limpia solo cada 10 min para evitar bloqueos
def procesar_datos_veloz(folder):
    rows_list = []
    if not os.path.exists(folder): return None
    
    # Listar archivos y ordenar por los más recientes
    archivos = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".txt")]
    archivos.sort(key=os.path.getmtime, reverse=True)
    
    # LEER SOLO LOS ÚLTIMOS 15 ARCHIVOS (Para que sea instantáneo)
    for path in archivos[:15]:
        try:
            with open(path, 'r', encoding='latin-1', errors='ignore') as f:
                content = f.read()
                # Buscar bloques de datos
                blocks = re.findall(r'NE Name:\s+([^\n\r]+).*?(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2}).*?Display Board Temperature(.*?)\n---    END', content, re.DOTALL)
                
                for ne_name, fecha, hora, table_text in blocks:
                    # Captura robusta de columnas
                    rows = re.findall(r'(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+|NULL)', table_text)
                    for r in rows:
                        rows_list.append({
                            "Timestamp": pd.to_datetime(f"{fecha} {hora}"),
                            "Sitio": ne_name.strip(),
                            "Board_Temp": int(r[3]), 
                            "Slot": int(r[2])
                        })
        except Exception:
            continue # Si un archivo falla, pasa al siguiente sin detener la app
            
    return pd.DataFrame(rows_list) if rows_list else None

# --- INICIO DE PROCESAMIENTO ---
with st.spinner('Cargando datos recientes...'):
    df = procesar_datos_veloz(FOLDER_PATH)

if df is not None and not df.empty:
    sitios = sorted(df['Sitio'].unique())
    tab1, tab2 = st.tabs(["🚦 Semáforo Actual", "📈 Histórico"])

    with tab1:
        seleccion = st.selectbox("Seleccione Sitio", sitios)
        
        # Filtro: Solo mostrar la lectura más reciente de ese sitio
        ultimo_ts = df[df['Sitio'] == seleccion]['Timestamp'].max()
        df_ahora = df[(df['Sitio'] == seleccion) & (df['Timestamp'] == ultimo_ts)].sort_values('Slot')

        st.subheader(f"Estado en tiempo real: {seleccion}")
        st.caption(f"Última lectura: {ultimo_ts}")

        cols = st.columns(4)
        for i, (_, row) in enumerate(df_ahora.iterrows()):
            t = row['Board_Temp']
            # Definir colores según temperatura
            if t < 45: bg, txt, icon = "#C6EFCE", "#006100", "🟢" # Verde
            elif t < 55: bg, txt, icon = "#FFEB9C", "#9C6500", "🟡" # Amarillo
            else: bg, txt, icon = "#FFC7CE", "#9C0006", "🔴" # Rojo

            with cols[i % 4]:
                st.markdown(f"""
                    <div style="background-color:{bg}; color:{txt}; padding:15px; border-radius:10px; text-align:center; border:1px solid {txt}40;">
                        <b style="font-size:14px;">SLOT {row['Slot']}</b>
                        <h2 style="margin:5px 0;">{t}°C</h2>
                        <span style="font-size:12px;">{icon} { "CRÍTICO" if t >= 55 else "NORMAL" }</span>
                    </div>
                """, unsafe_allow_html=True)

    with tab2:
        df_hist = df[df['Sitio'] == seleccion].copy()
        df_hist['Slot'] = df_hist['Slot'].astype(str)
        fig = px.line(df_hist, x='Timestamp', y='Board_Temp', color='Slot', markers=True)
        st.plotly_chart(fig, use_container_width=True)
else:
    st.error("No se pudieron cargar los datos. Verifica la carpeta 'Temperatura'.")
