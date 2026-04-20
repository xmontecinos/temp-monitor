import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px
import gc
from io import BytesIO
import pyarrow as pa
import pyarrow.parquet as pq

# 1. CONFIGURACIÓN DE PÁGINA
st.set_page_config(page_title="Monitor Red - Huawei", layout="wide")

# UMBRALES
UMBRAL_CRITICO = 78 
UMBRAL_PREVENTIVO = 65
FOLDER_PATH = 'Temperatura'
PARQUET_FILE = 'base_historica.parquet'

# --- FUNCIONES DE EXTRACCIÓN ---
def extraer_datos_masivo(path):
    rows = []
    try:
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            content = f.read()
            ts_match = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', content)
            if not ts_match: return []
            ts = pd.to_datetime(f"{ts_match.group(1)} {ts_match.group(2)}")
            
            bloques = re.split(r'NE Name\s*:\s*', content)
            for bloque in bloques[1:]:
                lineas = bloque.split('\n')
                if not lineas: continue
                nombre_sitio = lineas[0].strip().split()[0] # NEName junto
                
                filas = re.findall(r'^\s*\d+\s+(\d+)\s+(\d+)\s+(\d+)', bloque, re.MULTILINE)
                for r in filas:
                    rows.append({
                        "Timestamp": ts, 
                        "Sitio": nombre_sitio, 
                        "Slot": int(r[1]),
                        "Temp": int(r[2]), 
                        "ID_Full": f"{nombre_sitio} (S:{r[0]}-L:{r[1]})"
                    })
    except Exception: pass
    return rows

@st.cache_data(ttl=60)
def listar_archivos(folder):
    if not os.path.exists(folder): 
        os.makedirs(folder, exist_ok=True)
        return []
    fs = [os.path.join(folder, f) for f in os.listdir(folder) if ".txt" in f]
    fs.sort(key=lambda x: "".join(re.findall(r'\d+', x)), reverse=True)
    return fs

# --- PROCESAMIENTO INICIAL ---
archivos_lista = listar_archivos(FOLDER_PATH)

if archivos_lista:
    if "df_now" not in st.session_state:
        st.session_state["df_now"] = pd.DataFrame(extraer_datos_masivo(archivos_lista[0]))
    
    df_actual = st.session_state["df_now"]
    tabs = st.tabs(["📊 DASHBOARD", "🚨 ALERTAS ACTUALES", "🔍 BUSCADOR", "📈 HISTÓRICO", "🚀 ANÁLISIS UPGRADE"])

    # --- DASHBOARD ---
    with tabs[0]:
        if not df_actual.empty:
            ultima_hora = df_actual['Timestamp'].max().strftime('%d/%m/%Y %H:%M:%S')
            st.title("📊 Monitor de Salud de Red")
            c1, c2 = st.columns(2)
            c1.info(f"🕒 Reporte: {ultima_hora}")
            c2.success(f"📍 Sitios: {df_actual['Sitio'].nunique()}")
            
            t_crit = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
            t_prev = df_actual[(df_actual['Temp'] >= UMBRAL_PREVENTIVO) & (df_actual['Temp'] < UMBRAL_CRITICO)]
            t_ok = df_actual[df_actual['Temp'] < UMBRAL_PREVENTIVO]

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Total Tarjetas", len(df_actual))
            with m2: st.markdown(f'<div style="background-color:#fee2e2; border:2px solid #dc2626; padding:15px; border-radius:10px; text-align:center;"><h4 style="color:#991b1b; margin:0;">CRÍTICO</h4><h1 style="color:#dc2626; margin:5px 0; font-size:45px;">{len(t_crit)}</h1></div>', unsafe_allow_html=True)
            with m3: st.markdown(f'<div style="background-color:#fef9c3; border:2px solid #ca8a04; padding:15px; border-radius:10px; text-align:center;"><h4 style="color:#854d0e; margin:0;">PREVENTIVO</h4><h1 style="color:#ca8a04; margin:5px 0; font-size:45px;">{len(t_prev)}</h1></div>', unsafe_allow_html=True)
            with m4: st.markdown(f'<div style="background-color:#dcfce7; border:2px solid #16a34a; padding:15px; border-radius:10px; text-align:center;"><h4 style="color:#166534; margin:0;">ÓPTIMO</h4><h1 style="color:#166534; margin:5px 0; font-size:45px;">{len(t_ok)}</h1></div>', unsafe_allow_html=True)

            if not t_crit.empty:
                st.divider()
                st.subheader("🚨 Detalle por Slot")
                slot_s = st.selectbox("Elegir Slot:", sorted(t_crit['Slot'].unique()))
                df_s = t_crit[t_crit['Slot'] == slot_s].sort_values('Temp', ascending=False)
                st.dataframe(df_s[['Sitio', 'Temp', 'ID_Full']], use_container_width=True)

    # --- HISTÓRICO ---
    with tabs[3]:
        st.subheader("📈 Gestión Histórica")
        c1, c2 = st.columns([1, 2])
        with c1:
            num = st.slider("Archivos:", 1, len(archivos_lista), min(150, len(archivos_lista)))
            if st.button("🔥 Reconstruir Base"):
                if os.path.exists(PARQUET_FILE): os.remove(PARQUET_FILE)
                writer = None
                prog = st.progress(0)
                for i, p in enumerate(archivos_lista[:num]):
                    prog.progress((i+1)/num)
                    data = extraer_datos_masivo(p)
                    if data:
                        df_t = pd.DataFrame(data)
                        table = pa.Table.from_pandas(df_t)
                        if writer is None: writer = pq.ParquetWriter(PARQUET_FILE, table.schema)
                        writer.write_table(table)
                if writer: writer.close()
                st.success("✅ Base OK")
                st.rerun()
        with c2:
            if os.path.exists(PARQUET_FILE):
                df_h_menu = pq.read_table(PARQUET_FILE, columns=['Sitio']).to_pandas()
                sh = st.selectbox("Sitio:", sorted(df_h_menu['Sitio'].unique()))
                if sh:
                    df_v = pd.read_parquet(PARQUET_FILE, filters=[('Sitio', '==', sh)])
                    st.plotly_chart(px.line(df_v, x='Timestamp', y='Temp', color='ID_Full'), use_container_width=True)

    # --- ANÁLISIS UPGRADE (CORREGIDO) ---
    with tabs[4]:
        st.header("🚀 Análisis de Upgrade")
        if os.path.exists(PARQUET_FILE):
            # Leemos base para filtros
            df_full = pd.read_parquet(PARQUET_FILE)
            tiempos = sorted(df_full['Timestamp'].unique(), reverse=True)
            
            c_u1, c_u2 = st.columns(2)
            with c_u1:
                f_up = st.file_uploader("Lista de sitios (93):", type=['xlsx', 'csv'])
            with c_u2:
                # Conversión explícita a Timestamp para evitar el TypeError de Plotly
                referencia = st.selectbox("🎯 Punto de Comparación (Antes):", tiempos, format_func=lambda x: x.strftime('%Y-%m-%d %H:%M'))
                ref_ts = pd.Timestamp(referencia)

            sitios_lista = []
            if f_up:
                try:
                    df_lista = pd.read_csv(f_up) if f_up.name.endswith('.csv') else pd.read_excel(f_up)
                    sitios_lista = df_lista['Sitio'].astype(str).str.strip().unique().tolist()
                except: st.error("Error al leer archivo.")

            nodos = sorted(df_full['Sitio'].unique())
            sel = st.multiselect("Nodos a analizar:", nodos, default=[s for s in sitios_lista if s in nodos])
            
            if sel:
                df_up = df_full[df_full['Sitio'].isin(sel)]
                # Máxima temperatura por hora/sitio
                res_up = df_up.groupby(['Timestamp', 'Sitio'])['Temp'].max().reset_index()
                
                fig = px.line(res_up, x='Timestamp', y='Temp', color='Sitio', markers=True)
                # IMPORTANTE: ref_ts asegura que la línea se dibuje sin errores
                fig.add_vline(x=ref_ts.timestamp() * 1000, line_dash="dash", line_color="orange")
                st.plotly_chart(fig, use_container_width=True)

                st.divider()
                st.subheader(f"📉 Mejora desde {ref_ts.strftime('%d/%m %H:%M')}")
                
                df_ref = res_up[res_up['Timestamp'] == ref_ts][['Sitio', 'Temp']].rename(columns={'Temp': 'T_Antes'})
                df_now = res_up[res_up['Timestamp'] == res_up['Timestamp'].max()][['Sitio', 'Temp']].rename(columns={'Temp': 'T_Ahora'})
                
                df_final = pd.merge(df_ref, df_now, on='Sitio')
                df_final['Mejora'] = df_final['T_Antes'] - df_final['T_Ahora']
                
                # Sitios con baja de 10 o más
                bajan_10 = df_final[df_final['Mejora'] >= 10].sort_values('Mejora', ascending=False)
                
                if not bajan_10.empty:
                    st.success(f"Se encontraron {len(bajan_10)} sitios con mejora significativa.")
                    st.dataframe(bajan_10, use_container_width=True, hide_index=True)
                else:
                    st.info("No hay sitios con baja > 10°C respecto a esa hora.")

    # --- ALERTAS Y BUSCADOR ---
    with tabs[1]:
        st.dataframe(df_actual[df_actual['Temp'] >= UMBRAL_CRITICO], use_container_width=True)
    with tabs[2]:
        sb = st.selectbox("Nodo:", sorted(df_actual['Sitio'].unique()))
        st.dataframe(df_actual[df_actual['Sitio'] == sb], use_container_width=True)

else:
    st.warning("No hay datos.")
