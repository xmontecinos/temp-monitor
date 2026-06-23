import streamlit as st
import pandas as pd
import os
import re
from streamlit_autorefresh import st_autorefresh

# =========================
# CONFIG
# =========================
st.set_page_config(page_title="FAN Monitoring Real-Time", layout="wide")

FOLDER = "FANF"
UMBRAL = 90

st_autorefresh(interval=10_000, key="refresh")  # 10 segundos

# =========================
# ESTADO EN MEMORIA
# =========================
if "df" not in st.session_state:
    st.session_state.df = pd.DataFrame()

if "procesados" not in st.session_state:
    st.session_state.procesados = set()

if "alertas_previas" not in st.session_state:
    st.session_state.alertas_previas = set()

# =========================
# EXTRACCIÓN
# =========================
def extraer_datos_fan(path):
    rows = []

    try:
        with open(path, "r", encoding="latin-1", errors="ignore") as f:
            content = f.read()

        bloques = content.split("MML Command Result")

        for bloque in bloques[1:]:
            ne = re.search(r"NE Name\s*:\s*([\w_-]+)", bloque)
            if not ne:
                continue

            sitio = ne.group(1)

            slots = re.findall(r"Slot No\.\s*=\s*(\d+)", bloque)
            speeds = re.findall(r"Fan Speed Rate\(%\)\s*=\s*(\d+)", bloque)

            for s, v in zip(slots, speeds):
                val = int(v)

                rows.append({
                    "Sitio": sitio,
                    "Slot": s,
                    "Fan": val,
                    "Estado": "CRÍTICO" if val >= UMBRAL else "OK"
                })

    except Exception:
        pass

    return rows

# =========================
# INGESTA INCREMENTAL
# =========================
def cargar_nuevos():
    archivos = set(os.listdir(FOLDER))
    nuevos = archivos - st.session_state.procesados

    rows = []

    for f in nuevos:
        path = os.path.join(FOLDER, f)
        rows.extend(extraer_datos_fan(path))
        st.session_state.procesados.add(f)

    if rows:
        df_new = pd.DataFrame(rows)
        st.session_state.df = pd.concat(
            [st.session_state.df, df_new],
            ignore_index=True
        )

# =========================
# EJECUCIÓN INGESTA
# =========================
cargar_nuevos()
df = st.session_state.df

# =========================
# ALERTAS
# =========================
alertas = df[df["Fan"] >= UMBRAL] if not df.empty else pd.DataFrame()

# detectar nuevas alarmas
nuevas_alertas = set()

if not alertas.empty:
    nuevas_alertas = set(
        alertas["Sitio"].astype(str) + "-" + alertas["Slot"].astype(str)
    )

if nuevas_alertas - st.session_state.alertas_previas:
    st.toast("🚨 ALERTA FAN > 90% detectada", icon="🔥")

st.session_state.alertas_previas = nuevas_alertas

# =========================
# UI PRINCIPAL
# =========================
st.title("🔥 FAN Monitoring Real-Time (Huawei)")

col1, col2, col3 = st.columns(3)

col1.metric("Registros totales", len(df))
col2.metric("Alertas activas", len(alertas))
col3.metric("Sitios en alerta", alertas["Sitio"].nunique() if not alertas.empty else 0)

# =========================
# ALERTAS CRÍTICAS
# =========================
st.subheader("🚨 Alarmas FAN > 90%")

if not alertas.empty:
    st.error("⚠️ Equipos en condición CRÍTICA")
    st.dataframe(alertas, use_container_width=True)
else:
    st.success("✔ Sin alarmas activas")

# =========================
# TOP 20
# =========================
st.subheader("📊 Top 20 FAN más altos")

if not df.empty:
    top = df.sort_values("Fan", ascending=False).head(20)
    st.bar_chart(top.set_index("Sitio")["Fan"])
