import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px
import gc

st.set_page_config(page_title="Monitor Red - Full Histórico", layout="wide")

# --- CONFIGURACIÓN ---
UMBRAL_CRITICO = 79 
FOLDER_PATH = 'Temperatura'

def extraer_datos_masivo(path):
    """Escaneo profundo para capturar TODOS los sitios y filas de un archivo."""
    rows = []
    try:
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            content = f.read()
            
            # 1. Encontrar el Timestamp del reporte (común para todo el archivo)
            ts_match = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', content)
            if not ts_match: return []
            ts = pd.to_datetime(f"{ts_match.group(1)} {ts_match.group(2)}")

            # 2. Dividir el archivo por bloques de "NE Name" para no mezclar sitios
            bloques = re.split(r'NE Name\s*:\s*', content)
            
            for bloque in bloques[1:]: # Ignoramos el texto antes del primer sitio
                lineas = bloque.split('\n')
                nombre_sitio = lineas[0].strip().split()[0] # Primer palabra tras el ":"
                
                # 3. Buscar todas las filas de datos en este bloque
                # Buscamos: Subrack(r[0]) Slot(r[1]) Temperatura(r[2])
                filas = re.findall(r'^\s*\d+\s+(\d+)\s+(\d+)\s+(\d+)', bloque, re.MULTILINE)
                
                for r in filas:
                    rows.append({
                        "Timestamp": ts,
                        "Sitio": nombre_sitio,
                        "Slot": int(r[1]),
                        "Temp": int(r[2]),
                        "ID_Full": f"{nombre_sitio} (S:{r[0]}-L:{r[1]})"
                    })
    except: pass
    return rows

@st.cache_data(ttl=60)
def listar_archivos(folder):
    if not os.path.exists(folder): return []
    # Captura .txt, .gz.txt y cualquier variante
    fs = [os.path.join(folder, f) for f in os.listdir(folder) if ".txt" in f]
    fs.sort(key=lambda x: "".join(re.findall(r'\d+', x)), reverse=True)
    return fs

# --- INTERFAZ ---
archivos_lista = listar_archivos(FOLDER_PATH)

if archivos_lista:
    # Carga automática del último reporte
    if "df_now" not in st.session_state:
        st.session_state["df_now"] = pd.DataFrame(extraer_datos_masivo(archivos_lista[0]))
    
    df_actual = st.session_state["df_now"]
    tab1, tab2, tab3 = st.tabs(["🚨 ALERTAS ACTUALES", "🔍 BUSCADOR", "📈 HISTÓRICO "])

    with tab1:
        if not df_actual.empty:
            st.write(f"**Reporte:** {os.path.basename(archivos_lista[0])} | **Sitios detectados:** {df_actual['Sitio'].nunique()}")
            
            slots = sorted(df_actual['Slot'].unique())
            sel_slots = st.multiselect("Filtrar por Slots:", slots, default=slots)
            
            criticos = df_actual[(df_actual['Temp'] >= UMBRAL_CRITICO) & (df_actual['Slot'].isin(sel_slots))]
            if not criticos.empty:
                # Mostrar en cuadricula
                cols = st.columns(4)
                for i, (_, r) in enumerate(criticos.sort_values('Temp', ascending=False).iterrows()):
                    with cols[i % 4]:
                        st.markdown(f"""<div style="background-color:#fee2e2; border:1px solid #dc2626; padding:10px; border-radius:8px; margin-bottom:10px; text-align:center;">
                            <strong style="color:#991b1b;">{r['Sitio']}</strong><br>
                            <span style="font-size:24px; font-weight:bold; color:#dc2626;">{r['Temp']}°C</span><br>
                            <small>Slot: {r['Slot']}</small></div>""", unsafe_allow_html=True)
            else: st.success("No hay alertas en los slots seleccionados.")

    with tab3:
        st.subheader(f"Tendencia extendida (Disponibles: {len(archivos_lista)} reportes)")
        
        # El usuario elige cuántos reportes cargar para no saturar si no es necesario
        num_reportes = st.slider("Cantidad de reportes a procesar:", 100, min(100, len(archivos_lista)), 500)
        
        if st.button(f"📊 Cargar {num_reportes} Horas"):
            all_data = []
            progress = st.progress(0)
            status = st.empty()
            
            for i, p in enumerate(archivos_lista[:num_reportes]):
                status.text(f"Procesando {i+1}/{num_reportes}...")
                all_data.extend(extraer_datos_masivo(p))
                progress.progress((i + 1) / num_reportes)
                if i % 15 == 0: gc.collect() # Evitar error "Failed to fetch"

            if all_data:
                df_h = pd.DataFrame(all_data)
                # Agrupamos por hora para que el gráfico no sea lento
                df_h['Hora'] = df_h['Timestamp'].dt.floor('h')
                st.session_state["df_full"] = df_h.groupby(['Hora', 'Sitio', 'ID_Full'])['Temp'].max().reset_index()
                status.success(f"✅ ¡{num_reportes} horas cargadas! Sitios encontrados: {df_h['Sitio'].nunique()}")
            else:
                status.error("No se encontraron datos en el rango seleccionado.")

        if "df_full" in st.session_state:
            df_p = st.session_state["df_full"]
            sitio_sel = st.selectbox("Seleccione el sitio para ver su gráfico:", sorted(df_p['Sitio'].unique()))
            
            fig = px.line(df_p[df_p['Sitio'] == sitio_sel], 
                         x='Hora', y='Temp', color='ID_Full', markers=True,
                         title=f"Histórico 100h - {sitio_sel}")
            st.plotly_chart(fig, use_container_width=True)

else:
    st.error("No se detectaron archivos en la carpeta 'Temperatura'.")
