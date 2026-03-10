import streamlit as st
import pandas as pd
import os
import re
import gc

st.set_page_config(page_title="Monitor Red - Restauración Forzada", layout="wide")

FOLDER_PATH = 'Temperatura'

def extraer_flexible(path):
    """Extrae datos buscando patrones numéricos, ignorando espacios variables."""
    rows = []
    try:
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            ts, sitio = None, "Desconocido"
            for line in f:
                # Captura Fecha
                if "REPORT" in line:
                    m = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', line)
                    if m: ts = pd.to_datetime(f"{m.group(1)} {m.group(2)}")
                # Captura Sitio
                if "NE Name" in line or "NE :" in line:
                    sitio = line.split(":")[-1].strip().split()[0]
                
                # BUSCADOR FLEXIBLE: Busca líneas que terminen en números (temperatura)
                # Formato esperado: [Cualquier cosa] Numero Numero Numero
                partes = line.split()
                if len(partes) >= 4 and partes[-1].isdigit() and partes[-2].isdigit():
                    try:
                        temp = int(partes[-1])
                        slot = int(partes[-3])
                        if 10 < temp < 120:  # Filtro de seguridad para temperaturas lógicas
                            rows.append({
                                "Timestamp": ts, "Sitio": sitio,
                                "ID": f"{sitio} (Slot:{slot})",
                                "Slot": slot, "Temp": temp
                            })
                    except: continue
    except: pass
    return rows

@st.cache_data(ttl=300)
def listar_fs(folder):
    if not os.path.exists(folder): return []
    return sorted([os.path.join(folder, f) for f in os.listdir(folder) if ".txt" in f], reverse=True)

# --- INICIO ---
archivos = listar_fs(FOLDER_PATH)

if not archivos:
    st.error(f"No se ven archivos en '{FOLDER_PATH}'. Revisa GitHub.")
else:
    st.sidebar.write(f"📂 Archivos: {len(archivos)}")
    if st.sidebar.button("♻️ Limpiar Todo"):
        st.cache_data.clear()
        st.rerun()

    t1, t2, t3 = st.tabs(["🚨 ALERTAS", "🔍 BUSCADOR", "📈 HISTÓRICO"])

    # Carga inicial del reporte más nuevo
    data_now = pd.DataFrame(extraer_flexible(archivos[0]))

    with t1:
        if not data_now.empty:
            st.info(f"Reporte: {os.path.basename(archivos[0])}")
            criticos = data_now[data_now['Temp'] >= 65].sort_values('Temp', ascending=False)
            if not criticos.empty:
                cols = st.columns(4)
                for i, (_, r) in enumerate(criticos.iterrows()):
                    with cols[i % 4]:
                        st.markdown(f"""<div style="background-color:#fee2e2; border:2px solid #dc2626; padding:10px; border-radius:10px; text-align:center; margin-bottom:10px;">
                            <b style="color:#991b1b;">{r['Sitio']}</b><br>
                            <span style="font-size:24px; font-weight:bold; color:#dc2626;">{r['Temp']}°C</span><br>
                            <small>Slot: {r['Slot']}</small></div>""", unsafe_allow_html=True)
            else: st.success("Temperaturas normales.")
        else: st.warning("No se detectaron datos en el formato esperado.")

    with t2:
        if not data_now.empty:
            s = st.selectbox("Sitio:", sorted(data_now['Sitio'].unique()))
            st.table(data_now[data_now['Sitio'] == s][['ID', 'Temp']])

    with t3:
        horas = st.slider("Horas:", 10, len(archivos), 72)
        if st.button("📊 Generar Gráfico"):
            all_data = []
            p = st.progress(0)
            for i, f in enumerate(archivos[:horas]):
                all_data.extend(extraer_flexible(f))
                if i % 30 == 0:
                    p.progress((i+1)/horas)
                    gc.collect()
            
            if all_data:
                df_h = pd.DataFrame(all_data)
                df_h['Hora'] = df_h['Timestamp'].dt.floor('h')
                st.session_state['h_data'] = df_h.groupby(['Hora', 'Sitio', 'ID'])['Temp'].max().reset_index()
                st.success("Cargado.")
            
        if 'h_data' in st.session_state:
            df_p = st.session_state['h_data']
            sel = st.selectbox("Sitio:", sorted(df_p['Sitio'].unique()), key="h_sel")
            st.plotly_chart(px.line(df_p[df_p['Sitio'] == sel], x='Hora', y='Temp', color='ID', markers=True))
