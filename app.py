import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px
import gc

st.set_page_config(page_title="Monitor Red Pro", layout="wide")

# --- CONFIGURACIÓN ---
UMBRAL_CRITICO = 65 
FOLDER_PATH = 'Temperatura'

if st.sidebar.button("♻️ Reiniciar App"):
    st.cache_data.clear()
    st.session_state.clear()
    st.rerun()

def procesar_archivo_veloz(path):
    """Extrae datos sin cargar el archivo completo en RAM."""
    rows = []
    try:
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            sitio = "Unknown"
            ts = None
            # Leemos solo lo necesario
            for line in f:
                if "NE Name:" in line:
                    sitio = line.split(":")[-1].strip()
                if "REPORT" in line and not ts:
                    m = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', line)
                    if m: ts = pd.to_datetime(f"{m.group(1)} {m.group(2)}")
                
                # Detectar líneas de datos: Subrack, Slot, Temperatura
                # Formato esperado: espacios + numero + numero + numero + temperatura
                match = re.match(r'^\s*(\d+)\s+(\d+)\s+(\d+)\s+(\d+)', line)
                if match and ts:
                    rows.append({
                        "Timestamp": ts, "Sitio": sitio,
                        "Subrack": match.group(2), "Slot": int(match.group(3)),
                        "Temp": int(match.group(4)),
                        "ID_Full": f"{sitio} (S:{match.group(2)}-L:{match.group(3)})"
                    })
    except: pass
    return rows

@st.cache_data(ttl=60)
def obtener_lista_archivos(folder):
    if not os.path.exists(folder): return []
    fs = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".txt")]
    # Ordenar por fecha en el nombre del archivo (más reciente primero)
    fs.sort(key=lambda x: "".join(re.findall(r'\d+', x)), reverse=True)
    return fs

# --- INICIO ---
lista_fs = obtener_lista_archivos(FOLDER_PATH)

if lista_fs:
    # Carga rápida del reporte actual para Alertas y Buscador
    if "df_now" not in st.session_state:
        st.session_state["df_now"] = pd.DataFrame(procesar_archivo_veloz(lista_fs[0]))
    
    df_actual = st.session_state["df_now"]
    tab1, tab2, tab3 = st.tabs(["🚨 ALERTAS", "🔍 BUSCADOR", "📈 HISTÓRICO 100H"])

    with tab1:
        if not df_actual.empty:
            st.write(f"**Archivo:** {os.path.basename(lista_fs[0])}")
            slots = sorted(df_actual['Slot'].unique())
            sel_slots = st.multiselect("Filtrar Slots:", slots, default=slots)
            
            criticos = df_actual[(df_actual['Temp'] >= UMBRAL_CRITICO) & (df_actual['Slot'].isin(sel_slots))]
            if not criticos.empty:
                cols = st.columns(4)
                for i, (_, r) in enumerate(criticos.sort_values('Temp', ascending=False).iterrows()):
                    with cols[i % 4]:
                        st.error(f"**{r['Sitio']}**\n\n# {r['Temp']}°C\nSlot: {r['Slot']}")
            else: st.success("Todo bajo control.")

    with tab2:
        if not df_actual.empty:
            s_sel = st.selectbox("Sitio:", sorted(df_actual['Sitio'].unique()))
            st.table(df_actual[df_actual['Sitio'] == s_sel][['Timestamp', 'Subrack', 'Slot', 'Temp']])

    with tab3:
        st.subheader("Tendencia de 100 Reportes")
        if st.button("📊 Cargar Datos (100h)"):
            all_data = []
            p_bar = st.progress(0)
            msg = st.empty()
            
            # Procesamos hasta 100 archivos
            max_archivos = lista_fs[:100]
            for i, f_path in enumerate(max_archivos):
                msg.text(f"Leyendo {i+1}/100...")
                all_data.extend(procesar_archivo_veloz(f_path))
                p_bar.progress((i + 1) / len(max_archivos))
                if i % 20 == 0: gc.collect()
            
            df_h = pd.DataFrame(all_data)
            if not df_h.empty:
                # Importante: Reducir tamaño agrupando por hora
                df_h['Hora'] = df_h['Timestamp'].dt.floor('h')
                st.session_state["df_100h"] = df_h.groupby(['Hora', 'Sitio', 'ID_Full'])['Temp'].max().reset_index()
                msg.success("✅ Histórico cargado.")
            else: msg.error("No hay datos.")

        if "df_100h" in st.session_state:
            df_p = st.session_state["df_100h"]
            s_h = st.selectbox("Sitio:", sorted(df_p['Sitio'].unique()), key="h_100")
            fig = px.line(df_p[df_p['Sitio'] == s_h], x='Hora', y='Temp', color='ID_Full', markers=True)
            st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("No se encontraron archivos en la carpeta 'Temperatura'.")
