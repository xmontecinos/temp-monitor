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
st.set_page_config(page_title="Monitor Huawei Network - Pro", layout="wide")

# CONFIGURACIÓN DE RUTAS Y UMBRALES
FOLDER_PATH = 'Temperatura'
# Cambiamos a V4 para asegurar que no lea ningún archivo corrupto anterior
PARQUET_FILE = 'base_v4_pro.parquet' 
UMBRAL_CRITICO = 78 
UMBRAL_PREVENTIVO = 65

# --- MOTOR DE EXTRACCIÓN (NEName junto, Board Type con espacio) ---
def extraer_datos_unidad(path):
    rows = []
    try:
        with open(path, 'r', encoding='latin-1', errors='ignore') as f:
            content = f.read()
            # Captura de Timestamp
            ts_match = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', content)
            if not ts_match: return None
            ts = pd.to_datetime(f"{ts_match.group(1)} {ts_match.group(2)}")
            
            # Separación por NE Name
            bloques = re.split(r'NE Name\s*:\s*', content)
            for bloque in bloques[1:]:
                lineas = bloque.split('\n')
                if not lineas: continue
                
                # NEName debe ir junto (Ej: Site_01)
                sitio = lineas[0].strip().split()[0]
                
                # Regex para capturar Board Type, Slot y Temp
                filas = re.findall(r'^\s*\d+\s+(\d+)\s+(\d+)\s+(\d+)', bloque, re.MULTILINE)
                for r in filas:
                    rows.append({
                        "Timestamp": ts, 
                        "Sitio": sitio, 
                        "Slot": int(r[1]),
                        "Temp": int(r[2]), 
                        "ID_Full": f"{sitio} (S:{r[0]}-L:{r[1]})"
                    })
    except: return None
    return pd.DataFrame(rows)

def preparar_excel(df):
    output = BytesIO()
    # Usamos openpyxl para evitar errores de librerías faltantes
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

@st.cache_data(ttl=60)
def listar_archivos(folder):
    if not os.path.exists(folder): 
        os.makedirs(folder, exist_ok=True)
        return []
    fs = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".txt")]
    fs.sort(key=lambda x: "".join(re.findall(r'\d+', x)), reverse=True)
    return fs

# --- INICIO DE LÓGICA ---
archivos_lista = listar_archivos(FOLDER_PATH)

if archivos_lista:
    # Estado actual (Archivo más reciente)
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
            st.info(f"🕒 Reporte: {df_actual['Timestamp'].max()}")
            
            t_crit = df_actual[df_actual['Temp'] >= UMBRAL_CRITICO]
            t_prev = df_actual[(df_actual['Temp'] >= UMBRAL_PREVENTIVO) & (df_actual['Temp'] < UMBRAL_CRITICO)]
            
            m1, m2, m3 = st.columns(3)
            m1.metric("Total Tarjetas", f"{len(df_actual):,}")
            with m2: 
                st.markdown(f'<div style="background-color:#fee2e2; border:2px solid #dc2626; padding:15px; border-radius:10px; text-align:center;"><h4 style="color:#991b1b; margin:0;">CRÍTICO</h4><h1 style="color:#dc2626;">{len(t_crit)}</h1></div>', unsafe_allow_html=True)
            with m3:
                st.markdown(f'<div style="background-color:#fef9c3; border:2px solid #ca8a04; padding:15px; border-radius:10px; text-align:center;"><h4 style="color:#854d0e; margin:0;">PREVENTIVO</h4><h1 style="color:#ca8a04;">{len(t_prev)}</h1></div>', unsafe_allow_html=True)

            if not t_crit.empty:
                st.divider()
                st.subheader("🚨 Detalle de Nodos en Alerta")
                st.dataframe(t_crit.sort_values('Temp', ascending=False), use_container_width=True)
                st.download_button("📥 Descargar Alertas (Excel)", preparar_excel(t_crit), "reporte_critico.xlsx")

    # --- PESTAÑA 2: BUSCADOR ---
    with tab_busq:
        if df_actual is not None:
            st.subheader("🔍 Localizador de Sitio")
            nodo_sel = st.selectbox("Seleccione Nodo:", sorted(df_actual['Sitio'].unique()))
            st.dataframe(df_actual[df_actual['Sitio'] == nodo_sel], use_container_width=True)

    # --- PESTAÑA 3: HISTÓRICO (PROCESAMIENTO SEGURO) ---
    with tab_hist:
        st.subheader("📈 Gestión de Datos Históricos (+300 archivos)")
        c1, c2 = st.columns([1, 2])
        
        with c1:
            n = st.number_input("Cantidad de archivos a procesar:", 1, len(archivos_lista), len(archivos_lista))
            if st.button("🔥 GENERAR BASE DE DATOS"):
                if os.path.exists(PARQUET_FILE):
                    os.remove(PARQUET_FILE)
                
                writer = None
                p_bar = st.progress(0)
                status = st.empty()
                
                try:
                    for i, p in enumerate(archivos_lista[:n]):
                        status.text(f"Procesando: {os.path.basename(p)} ({i+1}/{n})")
                        df_tmp = extraer_datos_unidad(p)
                        
                        if df_tmp is not None and not df_tmp.empty:
                            # Tipos de datos pequeños para no agotar la RAM
                            df_tmp['Slot'] = df_tmp['Slot'].astype('int16')
                            df_tmp['Temp'] = df_tmp['Temp'].astype('int16')
                            
                            table = pa.Table.from_pandas(df_tmp)
                            if writer is None:
                                writer = pq.ParquetWriter(PARQUET_FILE, table.schema, compression='snappy')
                            writer.write_table(table)
                        
                        # Limpieza de memoria agresiva cada 10 archivos
                        if i % 10 == 0:
                            p_bar.progress((i+1)/n)
                            gc.collect()
                    
                    if writer:
                        writer.close()
                        st.success("✅ Base de datos construida con éxito.")
                        st.rerun()
                except Exception as e:
                    st.error(f"Error en la reconstrucción: {e}")

        with c2:
            if os.path.exists(PARQUET_FILE) and os.path.getsize(PARQUET_FILE) > 500:
                df_h = pd.read_parquet(PARQUET_FILE)
                s_h = st.selectbox("Seleccione Sitio para ver tendencia:", sorted(df_h['Sitio'].unique()))
                df_v = df_h[df_h['Sitio'] == s_h]
                fig = px.line(df_v, x='Timestamp', y='Temp', color='ID_Full', markers=True)
                fig.add_hline(y=UMBRAL_CRITICO, line_dash="dash", line_color="red")
                st.plotly_chart(fig, use_container_width=True)

    # --- PESTAÑA 4: UPGRADE ---
    with tab_upgrade:
        st.header("🚀 Análisis Post-Upgrade")
        if os.path.exists(PARQUET_FILE):
            df_full = pd.read_parquet(PARQUET_FILE)
            tiempos = sorted(df_full['Timestamp'].unique(), reverse=True)
            cu1, cu2 = st.columns(2)
            with cu1: 
                file_up = st.file_uploader("Cargar lista de sitios upgrade (xlsx/csv):", type=['xlsx', 'csv'])
            with cu2:
                ref_ts = st.selectbox("🎯 Fecha de Referencia:", tiempos, format_func=lambda x: x.strftime('%Y-%m-%d %H:%M'))
            
            if file_up:
                df_up = pd.read_csv(file_up) if file_up.name.endswith('.csv') else pd.read_excel(file_up)
                nodos_up = df_up['Sitio'].astype(str).str.strip().tolist()
                df_filt = df_full[df_full['Sitio'].isin(nodos_up)]
                
                if not df_filt.empty:
                    res_up = df_filt.groupby(['Timestamp', 'Sitio'])['Temp'].max().reset_index()
                    fig_up = px.line(res_up, x='Timestamp', y='Temp', color='Sitio')
                    # Solución al error de Plotly: timestamp en milisegundos
                    fig_up.add_vline(x=pd.Timestamp(ref_ts).timestamp() * 1000, line_dash="dash", line_color="orange")
                    st.plotly_chart(fig_up, use_container_width=True)
else:
    st.warning("⚠️ No se encontraron archivos .txt en la carpeta 'Temperatura'.")
