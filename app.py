import streamlit as st
import pandas as pd
import os
import re
import plotly.express as px
import gc
import pyarrow as pa
import pyarrow.parquet as pq

# 1. CONFIGURACIÓN DE LA APP
st.set_page_config(page_title="Monitor Red Huawei - Full Pro", layout="wide")

UMBRAL_CRITICO = 78 
UMBRAL_PREVENTIVO = 65
FOLDER_PATH = 'Temperatura'
PARQUET_FILE = 'base_historica.parquet'

# --- MOTOR DE EXTRACCIÓN (OPTIMIZADO PARA MEMORIA) ---
def extraer_datos_unidad(path):
    rows = []
    try:
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            content = f.read()
            ts_match = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', content)
            if not ts_match: return None
            ts = pd.to_datetime(f"{ts_match.group(1)} {ts_match.group(2)}")
            
            bloques = re.split(r'NE Name\s*:\s*', content)
            for bloque in bloques[1:]:
                lineas = bloque.split('\n')
                if not lineas: continue
                # Requerimiento: NEName debe ir junto (sin espacio)
                sitio = lineas[0].strip().split()[0]
                # Buscar Board Type, Slot y Temp
                filas = re.findall(r'^\s*\d+\s+(\d+)\s+(\d+)\s+(\d+)', bloque, re.MULTILINE)
                for r in filas:
                    rows.append({
                        "Timestamp": ts, "Sitio": sitio, "Slot": int(r[1]),
                        "Temp": int(r[2]), "ID_Full": f"{sitio} (S:{r[0]}-L:{r[1]})"
                    })
    except: return None
    return pd.DataFrame(rows)

@st.cache_data(ttl=60)
def listar_archivos(folder):
    if not os.path.exists(folder): 
        os.makedirs(folder, exist_ok=True)
        return []
    fs = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".txt")]
    fs.sort(key=lambda x: "".join(re.findall(r'\d+', x)), reverse=True)
    return fs

# --- PROCESAMIENTO INICIAL ---
archivos_lista = listar_archivos(FOLDER_PATH)

if archivos_lista:
    # Carga rápida para Dashboard
    if "df_now" not in st.session_state:
        st.session_state["df_now"] = extraer_datos_unidad(archivos_lista[0])
    
    df_actual = st.session_state["df_now"]
    tab_dash, tab_busq, tab_hist, tab_upgrade = st.tabs([
        "📊 DASHBOARD", "🔍 BUSCADOR", "📈 HISTÓRICO MASIVO", "🚀 UPGRADE"
    ])

    # --- PESTAÑA 1: DASHBOARD ---
    with tab_dash:
        if df_actual is not None:
            st.title("📊 Estado de Red Actual")
            st.info(f"🕒 Reporte: {df_actual['Timestamp'].max().strftime('%d/%m/%Y %H:%M')}")
            
            t_crit = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
            t_prev = df_actual[(df_actual['Temp'] >= UMBRAL_PREVENTIVO) & (df_actual['Temp'] < UMBRAL_CRITICO)]
            
            m1, m2, m3 = st.columns(3)
            with m1: st.markdown(f'<div style="background-color:#fee2e2; border:2px solid #dc2626; padding:15px; border-radius:10px; text-align:center;"><h3 style="color:#991b1b; margin:0;">CRÍTICO</h3><h1 style="color:#dc2626;">{len(t_crit)}</h1></div>', unsafe_allow_html=True)
            with m2: st.markdown(f'<div style="background-color:#fef9c3; border:2px solid #ca8a04; padding:15px; border-radius:10px; text-align:center;"><h3 style="color:#854d0e; margin:0;">PREVENTIVO</h3><h1 style="color:#ca8a04;">{len(t_prev)}</h1></div>', unsafe_allow_html=True)
            m3.metric("Total Tarjetas", f"{len(df_actual):,}")

            if not t_crit.empty:
                st.divider()
                st.subheader("🚨 Detalle por Slot")
                slot_sel = st.selectbox("Filtrar Slot:", sorted(t_crit['Slot'].unique()))
                st.dataframe(t_crit[t_crit['Slot'] == slot_sel].sort_values('Temp', ascending=False), use_container_width=True)
                
                st.subheader("🔝 Slots con más Alarmas")
                res_slots = t_crit.groupby('Slot').size().reset_index(name='Alertas')
                res_slots['Slot_Label'] = "Slot " + res_slots['Slot'].astype(str)
                st.plotly_chart(px.bar(res_slots, x='Slot_Label', y='Alertas', color='Alertas', color_continuous_scale='Reds'), use_container_width=True)

    # --- PESTAÑA 2: BUSCADOR ---
    with tab_busq:
        if df_actual is not None:
            sb = st.selectbox("Seleccione Nodo:", sorted(df_actual['Sitio'].unique()))
            st.dataframe(df_actual[df_actual['Sitio'] == sb], use_container_width=True)

    # --- PESTAÑA 3: HISTÓRICO (MOTOR MASIVO +300) ---
    with tab_hist:
        st.subheader("📈 Procesamiento de Datos Históricos")
        c1, c2 = st.columns([1, 2])
        with c1:
            num = st.number_input("Archivos a procesar:", 1, len(archivos_lista), len(archivos_lista))
            if st.button("🔥 RECONSTRUIR BASE (MODO SEGURO)"):
                if os.path.exists(PARQUET_FILE): os.remove(PARQUET_FILE)
                writer = None
                prog = st.progress(0)
                status = st.empty()
                for i, p in enumerate(archivos_lista[:num]):
                    df_tmp = extraer_datos_unidad(p)
                    if df_tmp is not None and not df_tmp.empty:
                        table = pa.Table.from_pandas(df_tmp)
                        if writer is None:
                            writer = pq.ParquetWriter(PARQUET_FILE, table.schema, compression='snappy')
                        writer.write_table(table)
                    if i % 25 == 0:
                        prog.progress((i+1)/num)
                        status.text(f"Procesando: {i+1} de {num}")
                        gc.collect() # LIBERA RAM CADA 25 ARCHIVOS
                if writer: writer.close()
                st.success("✅ Base generada."); st.rerun()
        
        with c2:
            if os.path.exists(PARQUET_FILE) and os.path.getsize(PARQUET_FILE) > 0:
                try:
                    df_h = pd.read_parquet(PARQUET_FILE)
                    sh = st.selectbox("Nodo Histórico:", sorted(df_h['Sitio'].unique()))
                    ids = sorted(df_h[df_h['Sitio'] == sh]['ID_Full'].unique())
                    sel = st.multiselect("Slots:", ids, default=ids[:2] if ids else [])
                    if sel:
                        fig = px.line(df_h[(df_h['Sitio'] == sh) & (df_h['ID_Full'].isin(sel))], x='Timestamp', y='Temp', color='ID_Full')
                        st.plotly_chart(fig, use_container_width=True)
                except: st.error("Archivo corrupto. Reconstruye la base.")

    # --- PESTAÑA 4: UPGRADE ---
    with tab_upgrade:
        st.header("🚀 Análisis de Upgrade")
        if os.path.exists(PARQUET_FILE):
            try:
                df_full = pd.read_parquet(PARQUET_FILE)
                tiempos = sorted(df_full['Timestamp'].unique(), reverse=True)
                cu1, cu2 = st.columns(2)
                with cu1: f_up = st.file_uploader("Subir lista sitios:", type=['csv', 'xlsx'])
                with cu2:
                    ref_sel = st.selectbox("🎯 Punto Referencia:", tiempos, format_func=lambda x: x.strftime('%Y-%m-%d %H:%M'))
                    ref_ts = pd.Timestamp(ref_sel)
                
                if f_up:
                    df_l = pd.read_csv(f_up) if f_up.name.endswith('.csv') else pd.read_excel(f_up)
                    sitios_up = df_l['Sitio'].astype(str).str.strip().tolist()
                    res_up = df_full[df_full['Sitio'].isin(sitios_up)].groupby(['Timestamp', 'Sitio'])['Temp'].max().reset_index()
                    fig_u = px.line(res_up, x='Timestamp', y='Temp', color='Sitio')
                    # PARCHE FECHA: Evita el TypeError de la imagen 5a0afb
                    fig_u.add_vline(x=ref_ts.timestamp() * 1000, line_dash="dash", line_color="orange")
                    st.plotly_chart(fig_u, use_container_width=True)
            except: st.info("Carga la base de datos primero.")
else:
    st.warning("Sin archivos .txt en la carpeta 'Temperatura'")
