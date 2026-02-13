
# app_mapa_1zip.py
# 🗺️ ICC — Mapa con 1 ZIP (Secciones INE + Manzanas INEGI)
#
# ✅ NUEVO: Filtro por VARIAS SECCIONES (multiselect) + botones “Seleccionar todas / Limpiar”
#
# - Sube SOLO 1 ZIP que contenga ambos SHP:
#     * Secciones (INE): ...SECCION*.shp o ...SECCIONES*.shp
#     * Manzanas (INEGI): ...MANZANAS*.shp o ...25m.shp
# - Filtros: Distrito local/federal, Municipio y (multi) Sección
# - Mapa base: relieve/topo/calles/satélite
# - Tablas + export CSV/Excel
#
# Nota: para que funcione sin broncas, el ZIP debe traer .shp + .dbf + .shx + .prj (y opcional .cpg)

from __future__ import annotations

import io
import os
import re
import zipfile
import tempfile
import hashlib
from typing import List, Tuple, Optional

import pandas as pd
import streamlit as st

import geopandas as gpd
import folium
from streamlit_folium import st_folium


# -------------------------
# UI
# -------------------------
st.set_page_config(page_title="ICC — 1 ZIP Mapas", page_icon="🗺️", layout="wide")
st.title("🗺️ ICC — Mapas con 1 ZIP (Secciones + Manzanas)")
st.caption("Sube un ZIP que ya contenga Secciones (INE) y Manzanas (INEGI). Filtra por distrito y sección (multi).")


# -------------------------
# ZIP / workspace helpers
# -------------------------
def md5_bytes(b: bytes) -> str:
    return hashlib.md5(b).hexdigest()


def safe_extract_zip_bytes(zip_bytes: bytes, out_dir: str) -> None:
    zpath = os.path.join(out_dir, "root.zip")
    with open(zpath, "wb") as f:
        f.write(zip_bytes)
    with zipfile.ZipFile(zpath, "r") as z:
        try:
            z.extractall(out_dir)
        except Exception:
            for n in z.namelist():
                try:
                    z.extract(n, out_dir)
                except Exception:
                    pass


def extract_nested_zips(base_dir: str, max_depth: int = 2) -> None:
    for _ in range(max_depth):
        nested = []
        for root, _, files in os.walk(base_dir):
            for fn in files:
                if fn.lower().endswith(".zip") and fn.lower() != "root.zip":
                    nested.append(os.path.join(root, fn))
        if not nested:
            return
        for z in nested:
            out = z + "_unzipped"
            os.makedirs(out, exist_ok=True)
            try:
                with zipfile.ZipFile(z, "r") as zz:
                    zz.extractall(out)
            except Exception:
                pass


def list_shps(ws_dir: str) -> List[str]:
    shps = []
    for root, _, files in os.walk(ws_dir):
        for fn in files:
            if fn.lower().endswith(".shp"):
                shps.append(os.path.relpath(os.path.join(root, fn), ws_dir))
    return sorted(shps)


def prepare_workspace(zip_bytes: bytes, key: str = "ONEZIP") -> Tuple[str, List[str]]:
    h = md5_bytes(zip_bytes)
    ss_key = f"WS_{key}"
    if ss_key in st.session_state:
        item = st.session_state[ss_key]
        if item.get("hash") == h and os.path.exists(item.get("dir", "")):
            return item["dir"], item["shps"]

    ws = tempfile.mkdtemp(prefix=f"{key}_")
    safe_extract_zip_bytes(zip_bytes, ws)
    extract_nested_zips(ws, max_depth=2)
    shps = list_shps(ws)

    st.session_state[ss_key] = {"hash": h, "dir": ws, "shps": shps}
    return ws, shps


# -------------------------
# Geo helpers
# -------------------------
def ensure_active_geometry(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Normaliza geometry activa a 'geometry'."""
    try:
        _ = gdf.geometry
    except Exception:
        for cand in ["geometry", "GEOMETRY", "Shape", "SHAPE"]:
            if cand in gdf.columns:
                gdf = gdf.set_geometry(cand)
                break

    geom_name = gdf.geometry.name
    if geom_name != "geometry":
        if "geometry" in gdf.columns and str(gdf["geometry"].dtype) != "geometry":
            gdf = gdf.rename(columns={"geometry": "GEOMETRY_OLD"})
        gdf = gdf.rename(columns={geom_name: "geometry"}).set_geometry("geometry")
    return gdf


def uppercase_non_geometry(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    rename = {}
    for c in gdf.columns:
        if c == "geometry":
            continue
        rename[c] = str(c).strip().upper()
    return gdf.rename(columns=rename)


def read_layer(ws_dir: str, shp_rel: str, label: str) -> gpd.GeoDataFrame:
    shp_path = os.path.join(ws_dir, shp_rel)
    if not os.path.exists(shp_path):
        raise FileNotFoundError(f"No existe el SHP: {shp_rel}")

    gdf = gpd.read_file(shp_path)
    gdf = ensure_active_geometry(gdf)
    gdf = uppercase_non_geometry(gdf)
    gdf = ensure_active_geometry(gdf)

    if gdf.crs is None:
        st.warning(f"⚠️ {label} sin CRS. Asumiendo EPSG:4326.")
        gdf = gdf.set_crs(epsg=4326, allow_override=True)
    try:
        gdf = gdf.to_crs(epsg=4326)
    except Exception:
        gdf = gdf.set_crs(epsg=4326, allow_override=True)

    gdf = gdf[gdf.geometry.notna()].copy()
    return gdf


def safe_center(gdf: gpd.GeoDataFrame) -> Tuple[float, float]:
    minx, miny, maxx, maxy = gdf.total_bounds
    return ((miny + maxy) / 2.0, (minx + maxx) / 2.0)


def pick_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    cols_u = {c.upper() for c in cols}
    for c in candidates:
        if c.upper() in cols_u:
            return c.upper()
    return None


def to_excel_bytes(sheets: dict) -> bytes:
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        for name, df in sheets.items():
            df.to_excel(w, index=False, sheet_name=name[:31])
    return out.getvalue()


# -------------------------
# Auto-detect SHPs
# -------------------------
def auto_pick_secciones(shps: List[str]) -> Optional[str]:
    low = [s.lower() for s in shps]
    for i, s in enumerate(low):
        if "secciones" in s and s.endswith(".shp"):
            return shps[i]
    for i, s in enumerate(low):
        if "seccion" in s and s.endswith(".shp"):
            return shps[i]
    return None


def auto_pick_manzanas(shps: List[str]) -> Optional[str]:
    low = [s.lower() for s in shps]
    for i, s in enumerate(low):
        if "manzanas" in s and s.endswith(".shp"):
            return shps[i]
    for i, s in enumerate(low):
        if re.search(r"(^|/)\d{2}m\.shp$", s):
            return shps[i]
    for i, s in enumerate(low):
        if "mza" in s and s.endswith(".shp"):
            return shps[i]
    return None


# -------------------------
# Input: 1 ZIP
# -------------------------
zip_file = st.file_uploader("📦 Sube tu ZIP (Secciones + Manzanas)", type=["zip"])
basemap = st.selectbox("Mapa base", ["Relieve (Esri)", "Topográfico (OpenTopoMap)", "Calles (OSM)", "Satélite (Esri)"], index=0)

if not zip_file:
    st.info("Sube el ZIP para empezar.")
    st.stop()

zbytes = zip_file.getvalue()

with st.spinner("Preparando ZIP..."):
    ws, shps = prepare_workspace(zbytes, "ONEZIP")

if not shps:
    st.error("No encontré ningún .shp dentro del ZIP.")
    st.stop()

secc_guess = auto_pick_secciones(shps)
mza_guess = auto_pick_manzanas(shps)

st.subheader("🧩 Capas detectadas")
c1, c2 = st.columns(2)
with c1:
    secc_shp = st.selectbox("Capa de SECCIONES (INE)", shps, index=shps.index(secc_guess) if secc_guess in shps else 0)
with c2:
    mza_shp = st.selectbox("Capa de MANZANAS (INEGI)", shps, index=shps.index(mza_guess) if mza_guess in shps else min(1, len(shps)-1))

with st.expander("🧪 Debug: lista de SHP encontrados", expanded=False):
    st.write(shps)

with st.spinner("Leyendo capas..."):
    secc = read_layer(ws, secc_shp, "Secciones")
    mza = read_layer(ws, mza_shp, "Manzanas")

# -------------------------
# Columnas esperadas
# -------------------------
col_ent = pick_col(list(secc.columns), ["ENTIDAD", "CVE_ENT", "ENT"])
col_mun = pick_col(list(secc.columns), ["MUNICIPIO", "CVE_MUN", "MUN"])
col_dl  = pick_col(list(secc.columns), ["DISTRITO_L", "DISTRITO", "DTO_L", "DIST_L"])
col_df  = pick_col(list(secc.columns), ["DISTRITO_F", "DTO_F", "DIST_F"])
col_sec = pick_col(list(secc.columns), ["SECCION", "SECC", "CVE_SECC", "ID_SECC"])
col_manz = pick_col(list(secc.columns), ["MANZANAS"])
col_vot = pick_col(list(secc.columns), ["VOTANTES", "VOT_EST"])
col_p18 = pick_col(list(secc.columns), ["POB18MAS", "POB_18_MAS", "P18MAS"])

mza_sec = pick_col(list(mza.columns), ["SECCION"])
mza_p18 = pick_col(list(mza.columns), ["POB18MAS", "POB_18_MAS", "P18MAS"])

# -------------------------
# Filtros
# -------------------------
st.divider()
st.subheader("🎛️ Filtros (Distrito / Sección)")

secc_f = secc.copy()

f1, f2, f3, f4 = st.columns([1, 1, 1, 1])

with f1:
    if col_dl:
        vals = sorted(secc_f[col_dl].dropna().astype(int).unique().tolist())
        dl_sel = st.selectbox("Distrito Local", ["(todos)"] + vals, index=0)
        if dl_sel != "(todos)":
            secc_f = secc_f[secc_f[col_dl].astype(int) == int(dl_sel)].copy()
    else:
        st.write("Distrito Local: (no detectado)")

with f2:
    if col_df:
        vals = sorted(secc_f[col_df].dropna().astype(int).unique().tolist())
        df_sel = st.selectbox("Distrito Federal", ["(todos)"] + vals, index=0)
        if df_sel != "(todos)":
            secc_f = secc_f[secc_f[col_df].astype(int) == int(df_sel)].copy()
    else:
        st.write("Distrito Federal: (no detectado)")

with f3:
    if col_mun:
        vals = sorted(secc_f[col_mun].dropna().astype(int).unique().tolist())
        mun_sel = st.selectbox("Municipio", ["(todos)"] + vals, index=0)
        if mun_sel != "(todos)":
            secc_f = secc_f[secc_f[col_mun].astype(int) == int(mun_sel)].copy()
    else:
        st.write("Municipio: (no detectado)")

with f4:
    if col_sec:
        secs = sorted(secc_f[col_sec].dropna().astype(int).unique().tolist())
        # estado para multiselect
        if "sec_multi" not in st.session_state:
            st.session_state["sec_multi"] = []

        b1, b2 = st.columns(2)
        if b1.button("Seleccionar todas", use_container_width=True):
            st.session_state["sec_multi"] = secs
        if b2.button("Limpiar", use_container_width=True):
            st.session_state["sec_multi"] = []

        sec_selected = st.multiselect(
            "Secciones (multi)",
            options=secs,
            default=st.session_state.get("sec_multi", []),
            key="sec_multi",
        )

        # aplicar filtro si seleccionó algo
        if sec_selected:
            sec_set = set(int(x) for x in sec_selected)
            secc_f = secc_f[secc_f[col_sec].astype(int).isin(sec_set)].copy()
    else:
        st.write("Sección: (no detectada)")

if secc_f.empty:
    st.error("Con esos filtros no quedó ninguna sección.")
    st.stop()

# recorte manzanas por bbox de secciones filtradas
minx, miny, maxx, maxy = secc_f.total_bounds
mza_bbox = mza.cx[minx:maxx, miny:maxy].copy()

# si manzanas tiene SECCION, filtrar por las seleccionadas (más exacto que bbox)
if col_sec and mza_sec and col_sec in secc_f.columns:
    try:
        selected_secs = sorted(secc_f[col_sec].dropna().astype(int).unique().tolist())
        if selected_secs:
            mza_bbox = mza_bbox[mza_bbox[mza_sec].astype(int).isin(set(selected_secs))].copy()
    except Exception:
        pass

# -------------------------
# KPIs / info general
# -------------------------
st.divider()
st.subheader("📌 Información general del recorte")

k1, k2, k3, k4 = st.columns(4)
k1.metric("Secciones", f"{len(secc_f):,}")
k2.metric("Manzanas (recorte)", f"{len(mza_bbox):,}")

if col_manz:
    k3.metric("Manzanas (sum en secciones)", f"{int(secc_f[col_manz].fillna(0).sum()):,}")
else:
    k3.metric("Manzanas (sum secciones)", "N/D")

if col_vot:
    try:
        k4.metric("Votantes (col)", f"{int(pd.to_numeric(secc_f[col_vot], errors='coerce').fillna(0).sum()):,}")
    except Exception:
        k4.metric("Votantes (col)", "N/D")
elif col_p18:
    k4.metric("POB 18+ (col)", f"{int(pd.to_numeric(secc_f[col_p18], errors='coerce').fillna(0).sum()):,}")
else:
    k4.metric("Votantes/POB", "N/D")

# -------------------------
# Tabs
# -------------------------
tab_map, tab_tables, tab_export = st.tabs(["🗺️ Mapa", "📋 Tablas", "⬇️ Exportar"])

# -------------------------
# MAP
# -------------------------
with tab_map:
    st.subheader("Mapa")
    show_manz = st.checkbox("Mostrar manzanas (puede ser pesado)", value=False)

    lat, lon = safe_center(secc_f)
    m = folium.Map(location=[lat, lon], zoom_start=12, tiles=None, control_scale=True)

    if basemap == "Relieve (Esri)":
        folium.TileLayer(
            tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Shaded_Relief/MapServer/tile/{z}/{y}/{x}",
            name="Relieve (Esri)", attr="Tiles © Esri", overlay=False, control=True
        ).add_to(m)
    elif basemap == "Topográfico (OpenTopoMap)":
        folium.TileLayer(
            tiles="https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png",
            name="Topográfico (OpenTopoMap)",
            attr="© OpenTopoMap / © OpenStreetMap contributors",
            overlay=False, control=True
        ).add_to(m)
    elif basemap == "Satélite (Esri)":
        folium.TileLayer(
            tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
            name="Satélite (Esri)", attr="Tiles © Esri", overlay=False, control=True
        ).add_to(m)
    else:
        folium.TileLayer("OpenStreetMap", name="Calles (OSM)", overlay=False, control=True).add_to(m)

    # Tooltip fields
    fields = []
    aliases = []
    for c, a in [
        (col_sec, "Sección:"),
        (col_dl, "DL:"),
        (col_df, "DF:"),
        (col_mun, "Mpio:"),
        (col_manz, "Manzanas:"),
        (col_vot, "Votantes:"),
        (col_p18, "POB18+:"),
    ]:
        if c and c in secc_f.columns:
            fields.append(c)
            aliases.append(a)

    folium.GeoJson(
        secc_f.to_json(),
        name="Secciones",
        style_function=lambda feat: {"weight": 2, "fillOpacity": 0.06},
        tooltip=folium.GeoJsonTooltip(fields=fields, aliases=aliases, sticky=False) if fields else None,
    ).add_to(m)

    if show_manz:
        # muestreo para no matar el mapa
        max_feat = 6000
        mz_show = mza_bbox.sample(max_feat, random_state=7).copy() if len(mza_bbox) > max_feat else mza_bbox
        folium.GeoJson(
            mz_show.to_json(),
            name="Manzanas",
            style_function=lambda feat: {"weight": 1, "fillOpacity": 0.02},
        ).add_to(m)

    folium.LayerControl(collapsed=False).add_to(m)
    bds = secc_f.total_bounds
    m.fit_bounds([[bds[1], bds[0]], [bds[3], bds[2]]])

    st_folium(m, use_container_width=True, height=650)

# -------------------------
# TABLES
# -------------------------
with tab_tables:
    st.subheader("Tabla de secciones (filtradas)")
    show_cols = [c for c in [col_ent, col_mun, col_dl, col_df, col_sec, col_manz, col_p18, col_vot] if c and c in secc_f.columns]
    df_secc = secc_f[show_cols].copy() if show_cols else secc_f.drop(columns=["geometry"], errors="ignore").copy()

    st.dataframe(df_secc.drop(columns=["geometry"], errors="ignore"), use_container_width=True, height=420)

    st.subheader("Tabla de manzanas (recorte)")
    df_mza = mza_bbox.drop(columns=["geometry"], errors="ignore").copy()

    # ordenar columnas típicas al frente
    front = [c for c in ["CVE_ENT", "CVE_MUN", "CVE_LOC", "CVE_AGEB", "CVE_MZA", "TIPOMZA"] if c in df_mza.columns]
    extra = [c for c in [mza_sec, mza_p18] if c and c in df_mza.columns and c not in front]
    rest = [c for c in df_mza.columns if c not in front + extra]
    df_mza = df_mza[front + extra + rest]

    st.dataframe(df_mza, use_container_width=True, height=420)

# -------------------------
# EXPORT
# -------------------------
with tab_export:
    st.subheader("Exportación")

    resumen = {
        "SECCIONES": len(secc_f),
        "MANZANAS_RECORTE": len(mza_bbox),
    }
    if col_manz:
        resumen["MANZANAS_SUM_SECCIONES"] = int(secc_f[col_manz].fillna(0).sum())
    if col_p18:
        resumen["POB18MAS_TOTAL"] = int(pd.to_numeric(secc_f[col_p18], errors="coerce").fillna(0).sum())
    if col_vot:
        resumen["VOTANTES_TOTAL"] = int(pd.to_numeric(secc_f[col_vot], errors="coerce").fillna(0).sum())

    df_res = pd.DataFrame([resumen])

    st.download_button(
        "⬇️ Descargar Excel (resumen + secciones + manzanas)",
        data=to_excel_bytes({
            "RESUMEN": df_res,
            "SECCIONES": df_secc.drop(columns=["geometry"], errors="ignore"),
            "MANZANAS": df_mza
        }),
        file_name="export_distrito_secciones_manzanas.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True
    )

    st.download_button(
        "⬇️ Descargar CSV (secciones)",
        data=df_secc.drop(columns=["geometry"], errors="ignore").to_csv(index=False).encode("utf-8"),
        file_name="secciones_filtradas.csv",
        mime="text/csv",
        use_container_width=True
    )

    st.download_button(
        "⬇️ Descargar CSV (manzanas recorte)",
        data=df_mza.to_csv(index=False).encode("utf-8"),
        file_name="manzanas_recorte.csv",
        mime="text/csv",
        use_container_width=True
    )

st.success("✅ Listo. Ahora puedes seleccionar VARIAS secciones (multi) y exportar.")
