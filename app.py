import streamlit as st
import pandas as pd
import re
import os

st.set_page_config(page_title="Monitor Veloz", layout="wide")

# Botón para limpiar todo si se queda pegado
if st.sidebar.button("🗑️ Limpiar Todo y Reintentar"):
    st.cache_data.clear()
    st.cache_resource.clear()
    st.rerun()

st.title("🌡️ Monitor de Temperaturas")

FOLDER_PATH = 'Temperatura'

@st.cache_data(ttl=300) # El caché expira rápido para no saturar
def carga_minima(folder):
    rows_list = []
    if not os.path.exists(folder): return None
    
    # SOLO LEER LOS 3 ARCHIVOS MÁS NUEVOS
    archivos = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".txt")]
    archivos.sort(key=os.path.getmtime, reverse=True)
    
    for path in archivos[:3]: # Límite agresivo de 3 archivos
        try:
            with open(path, 'r', encoding='latin-1', errors='ignore') as f:
                content = f.read()
                # Buscamos solo el bloque final de cada archivo
                blocks = re.findall(r'NE Name:\s+([^\n\r]+).*?(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2}).*?Display Board Temperature(.*?)\n---    END', content, re.DOTALL)
                
                for ne_name, fecha, hora, table_text in blocks:
                    rows = re.findall(r'(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+|NULL)', table_text)
                    for r in rows:
                        rows_list.append({
                            "Timestamp": pd.to_datetime(f"{fecha} {hora}"),
                            "Sitio": ne_name.strip(),
                            "Temp": int(r[3]), 
                            "Slot": int(r[2])
                        })
        except: continue
    return pd.DataFrame(rows_list)

# Ejecución
df = carga_minima(FOLDER_PATH)

if df is not None and not df.empty:
    sitio = st.selectbox("Sitio", df['Sitio'].unique())
    # Mostrar solo la última lectura
    ultimo_ts = df[df['Sitio'] == sitio]['Timestamp'].max()
    datos = df[(df['Sitio'] == sitio) & (df['Timestamp'] == ultimo_ts)]
    
    st.write(f"### Última lectura: {ultimo_ts}")
    cols = st.columns(len(datos))
    for i, (_, r) in enumerate(datos.iterrows()):
        with cols[i]:
            st.metric(f"Slot {r['Slot']}", f"{r['Temp']}°C")
else:
    st.info("Buscando archivos... Si no carga en 10 segundos, usa el botón lateral.")
