# app_viewer_kmz.py
# ICC — Manzanas (cuadras) por Sección Electoral (INE x INEGI)
#
# FIXES:
# - Evita "ciclo" guardando resultados en st.session_state
# - Evita FileNotFoundError por cache+tmp (NO usa st.cache con rutas temporales)
# - Normaliza geometría: siempre columna activa = "geometry" (evita GEOMETRY vs geometry)
# - sjoin robusto: fuerza GeoDataFrame con CRS
# - Basemap relieve/topo/calles

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


# =========================
# UI
# =========================
st.set_page_config(page_title="ICC — Manzanas por Sección", page_icon="🗺️", layout="wide")
st.title("🗺️ ICC — Manzanas (cuadras) por Sección Electoral")
st.caption("Sube ZIP INE (Secciones) e INEGI (Manzanas). Contamos manzanas por sección y lo mostramos en mapa.")


# =========================
# Helpers
# =========================
def md5_bytes(b: bytes) -> str:
    return hashlib.md5(b).hexdigest()


def safe_extract_zipfile(zpath: str, out_dir: str) -> None:
    """Extrae zip (best effort)."""
    with zipfile.ZipFile(zpath, "r") as z:
        # extractall suele funcionar; si falla, intentamos archivo por archivo
        try:
            z.extractall(out_dir)
        except Exception:
            for name in z.namelist():
                try:
                    z.extract(name, out_dir)
                except Exception:
                    pass


def extract_zip_bytes(zip_bytes: bytes, out_dir: str) -> None:
    zpath = os.path.join(out_dir, "root.zip")
    with open(zpath, "wb") as f:
        f.write(zip_bytes)
    safe_extract_zipfile(zpath, out_dir)


def extract_nested_zips(base_dir: str, max_depth: int = 2) -> None:
    """Extrae ZIPs anidados hasta max_depth."""
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
            if not os.path.exists(out):
                os.makedirs(out, exist_ok=True)
                try:
                    safe_extract_zipfile(z, out)
                except Exception:
                    pass


def list_shps(ws_dir: str) -> List[str]:
    shps = []
    for root, _, files in os.walk(ws_dir):
        for fn in files:
            if fn.lower().endswith(".shp"):
                shps.append(os.path.relpath(os.path.join(root, fn), ws_dir))
    return sorted(shps)


def prepare_workspace(zip_bytes: bytes, key: str) -> Tuple[str, List[str]]:
    """
    Crea/recupera workspace por session_state (NO cache_data) para evitar
    que se rompa con rutas temporales.
    """
    h = md5_bytes(zip_bytes)
    ss_key = f"WS_{key}"

    if ss_key in st.session_state:
        item = st.session_state[ss_key]
        if item.get("hash") == h and os.path.exists(item.get("dir", "")):
            return item["dir"], item["shps"]

    ws = tempfile.mkdtemp(prefix=f"{key}_")
    extract_zip_bytes(zip_bytes, ws)
    extract_nested_zips(ws, max_depth=2)

    shps = list_shps(ws)
    st.session_state[ss_key] = {"hash": h, "dir": ws, "shps": shps}
    return ws, shps


def filter_list(items: List[str], q: str) -> List[str]:
    q = (q or "").strip().lower()
    if not q:
        return items
    return [x for x in items if q in x.lower()]


def auto_pick(shps: List[str], kind: str) -> int:
    low = [s.lower() for s in shps]
    if kind == "ine_seccion":
        for i, s in enumerate(low):
            if "seccion" in s or "secc" in s:
                return i
        return 0

    if kind == "inegi_manzana":
        # frecuente: 25m.shp = manzana, 25a.shp = ageb
        for i, s in enumerate(low):
            if re.search(r"(^|/)\d{2}m\.shp$", s):
                return i
        for i, s in enumerate(low):
            if "manz" in s or "mza" in s:
                return i
        return 0

    return 0


def ensure_active_geometry(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Normaliza para que SIEMPRE:
    - exista columna llamada 'geometry'
    - sea la geometría activa
    Esto evita el error de logs GEOMETRY vs geometry.
    """
    # 1) localizar columna geom candidata
    geom_cols = [c for c in gdf.columns if str(gdf[c].dtype) == "geometry"]
    active = None
    try:
        active = gdf.geometry.name
    except Exception:
        active = None

    cand = None
    if active and active in gdf.columns:
        cand = active
    elif "geometry" in gdf.columns and str(gdf["geometry"].dtype) == "geometry":
        cand = "geometry"
    elif "GEOMETRY" in gdf.columns and str(gdf["GEOMETRY"].dtype) == "geometry":
        cand = "GEOMETRY"
    elif geom_cols:
        cand = geom_cols[0]

    if cand is None:
        raise AttributeError("No se detectó ninguna columna geométrica válida en la capa.")

    # 2) si ya se llama geometry, ok
    if cand != "geometry":
        # si existe 'geometry' pero no es geometry dtype, la movemos para no chocar
        if "geometry" in gdf.columns and str(gdf["geometry"].dtype) != "geometry":
            gdf = gdf.rename(columns={"geometry": "GEOMETRY_OLD"})
        gdf = gdf.rename(columns={cand: "geometry"})

    # 3) set geometry activa
    gdf = gdf.set_geometry("geometry", inplace=False)

    return gdf


def uppercase_non_geometry(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Uppercase columnas excepto la geometry activa ('geometry')."""
    rename = {}
    for c in gdf.columns:
        if c == "geometry":
            continue
        rename[c] = str(c).strip().upper()
    return gdf.rename(columns=rename)


def read_gdf(ws_dir: str, shp_rel: str, label: str) -> gpd.GeoDataFrame:
    shp_path = os.path.join(ws_dir, shp_rel)
    if not os.path.exists(shp_path):
        raise FileNotFoundError(f"No existe el SHP seleccionado: {shp_rel}")

    gdf = gpd.read_file(shp_path)

    # normalizar geometry activa
    gdf = ensure_active_geometry(gdf)

    # renombrar columnas sin romper geometry
    gdf = uppercase_non_geometry(gdf)
    gdf = ensure_active_geometry(gdf)  # por seguridad tras renombre

    # CRS
    if gdf.crs is None:
        st.warning(f"⚠️ {label} no trae CRS. Asumiendo EPSG:4326.")
        gdf = gdf.set_crs(epsg=4326, allow_override=True)

    # a 4326 para mapa
    try:
        gdf = gdf.to_crs(epsg=4326)
    except Exception:
        st.warning(f"⚠️ No pude convertir CRS de {label}. Forzando EPSG:4326.")
        gdf = gdf.set_crs(epsg=4326, allow_override=True)

    gdf = gdf[gdf.geometry.notna()].copy()

    # arreglar geometrías inválidas (opcional)
    try:
        gdf["geometry"] = gdf.geometry.buffer(0)
        gdf = ensure_active_geometry(gdf)
    except Exception:
        pass

    return gdf


def pick_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    cols_u = {c.upper() for c in cols}
    for cand in candidates:
        if cand.upper() in cols_u:
            return cand.upper()
    return None


def safe_center(gdf: gpd.GeoDataFrame) -> Tuple[float, float]:
    minx, miny, maxx, maxy = gdf.total_bounds
    return ((miny + maxy) / 2.0, (minx + maxx) / 2.0)


def to_excel_bytes(df: pd.DataFrame, sheet: str) -> bytes:
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name=sheet)
    return out.getvalue()


# =========================
# Uploads
# =========================
c1, c2, c3 = st.columns([1, 1, 1])
with c1:
    ine_zip = st.file_uploader("ZIP INE — Secciones electorales", type=["zip"])
with c2:
    inegi_zip = st.file_uploader("ZIP INEGI — Manzanas (Marco Geoestadístico)", type=["zip"])
with c3:
    basemap = st.selectbox("Basemap", ["Relieve (Esri)", "Topográfico (OpenTopoMap)", "Calles (OSM)"], index=0)

if not ine_zip or not inegi_zip:
    st.info("Sube ambos ZIP para continuar.")
    st.stop()

ine_bytes = ine_zip.getvalue()
inegi_bytes = inegi_zip.getvalue()

with st.spinner("📦 Preparando ZIP INE..."):
    ws_ine, shps_ine = prepare_workspace(ine_bytes, "INE")
with st.spinner("📦 Preparando ZIP INEGI..."):
    ws_inegi, shps_inegi = prepare_workspace(inegi_bytes, "INEGI")

if not shps_ine:
    st.error("No encontré ningún .shp dentro del ZIP del INE.")
    st.stop()
if not shps_inegi:
    st.error("No encontré ningún .shp dentro del ZIP del INEGI.")
    st.stop()

st.divider()
st.subheader("🧩 Elige las capas correctas (.shp)")

f1, f2 = st.columns(2)
with f1:
    fil_ine = st.text_input("Filtrar INE (ej: secc, seccion)", value="secc")
with f2:
    fil_inegi = st.text_input("Filtrar INEGI (tip: 25m para manzana)", value="25m")

ine_view = filter_list(shps_ine, fil_ine) or shps_ine
inegi_view = filter_list(shps_inegi, fil_inegi) or shps_inegi

ine_idx = auto_pick(ine_view, "ine_seccion")
inegi_idx = auto_pick(inegi_view, "inegi_manzana")

cc1, cc2 = st.columns(2)
with cc1:
    shp_ine_choice = st.selectbox("INE: SHP de SECCIONES", ine_view, index=min(ine_idx, len(ine_view) - 1))
with cc2:
    shp_inegi_choice = st.selectbox("INEGI: SHP de MANZANAS", inegi_view, index=min(inegi_idx, len(inegi_view) - 1))

if re.search(r"(^|/)\d{2}a\.shp$", shp_inegi_choice.lower()):
    st.warning("⚠️ Ese parece **AGEB (..a.shp)**. Para manzana normalmente es **..m.shp** (ej: 25m.shp).")

with st.expander("🧪 Debug (listas detectadas)", expanded=False):
    st.write("INE .shp detectados:", len(shps_ine))
    st.write("INEGI .shp detectados:", len(shps_inegi))
    st.write("INE ejemplo:", shps_ine[:10])
    st.write("INEGI ejemplo:", shps_inegi[:10])

with st.spinner("🧠 Leyendo capas..."):
    secc = read_gdf(ws_ine, shp_ine_choice, "INE (Secciones)")
    mza = read_gdf(ws_inegi, shp_inegi_choice, "INEGI (Capa)")

st.success(f"INE Secciones: {len(secc):,} | INEGI elementos: {len(mza):,}")

st.divider()
st.subheader("🎛️ Columnas / filtros")

guess_seccion = pick_col(list(secc.columns), ["SECCION", "SECC", "CVE_SECC", "ID_SECC"])
k1, k2, k3 = st.columns(3)

with k1:
    secc_id_col = st.selectbox(
        "Columna ID de Sección",
        options=sorted(secc.columns),
        index=(sorted(secc.columns).index(guess_seccion) if guess_seccion in secc.columns else 0),
    )
with k2:
    distrito_col = st.selectbox("Columna Distrito (opcional)", options=["(no usar)"] + sorted(secc.columns), index=0)
with k3:
    mun_col = st.selectbox("Columna Municipio (opcional)", options=["(no usar)"] + sorted(secc.columns), index=0)

secc_f = secc.copy()

if distrito_col != "(no usar)":
    vals = sorted(secc_f[distrito_col].dropna().astype(str).unique().tolist())
    distrito_sel = st.selectbox("Filtrar distrito", ["(todos)"] + vals, index=0)
    if distrito_sel != "(todos)":
        secc_f = secc_f[secc_f[distrito_col].astype(str) == str(distrito_sel)].copy()

if mun_col != "(no usar)":
    vals = sorted(secc_f[mun_col].dropna().astype(str).unique().tolist())
    # intentar poner AHOM(E) por defecto si aparece
    def_idx = 0
    for i, v in enumerate(vals):
        if "ahome" in str(v).lower():
            def_idx = i + 1
            break
    mun_sel = st.selectbox("Filtrar municipio", ["(todos)"] + vals, index=def_idx)
    if mun_sel != "(todos)":
        secc_f = secc_f[secc_f[mun_col].astype(str) == str(mun_sel)].copy()

if secc_f.empty:
    st.error("Con esos filtros no quedó ninguna sección.")
    st.stop()

# recorte bbox para performance
minx, miny, maxx, maxy = secc_f.total_bounds
mza_f = mza.cx[minx:maxx, miny:maxy].copy()

if mza_f.empty:
    st.warning("No quedaron elementos INEGI dentro del bbox de las secciones (posible capa incorrecta).")
    st.stop()

st.write(f"Secciones filtradas: **{len(secc_f):,}** | INEGI (bbox): **{len(mza_f):,}**")

st.divider()
st.subheader("📊 Conteo de manzanas por sección")

pred = st.selectbox("Regla espacial", ["intersects", "within"], index=0)
cbtn1, cbtn2 = st.columns([1, 1])

# Persistencia para que NO “se cicle” y pierda el resultado
if "ICC_RESULT" not in st.session_state:
    st.session_state["ICC_RESULT"] = None

if cbtn1.button("🚀 Calcular conteo", width="stretch"):
    with st.spinner("Cruzando (sjoin) y contando..."):
        secc_p = ensure_active_geometry(secc_f).to_crs(epsg=3857)
        mza_p = ensure_active_geometry(mza_f).to_crs(epsg=3857)

        # forzar GeoDataFrame con CRS
        left = gpd.GeoDataFrame(mza_p[["geometry"]].copy(), geometry="geometry", crs=mza_p.crs)
        right = gpd.GeoDataFrame(secc_p[[secc_id_col, "geometry"]].copy(), geometry="geometry", crs=secc_p.crs)

        if left.crs != right.crs:
            right = right.to_crs(left.crs)

        joined = gpd.sjoin(left, right, how="inner", predicate=pred)

        counts = (
            joined.groupby(secc_id_col)
            .size()
            .reset_index(name="MANZANAS")
            .sort_values("MANZANAS", ascending=False)
        )

        secc_out = ensure_active_geometry(secc_f).copy()
        secc_out[secc_id_col] = secc_out[secc_id_col].astype(str)
        counts[secc_id_col] = counts[secc_id_col].astype(str)

        secc_out = secc_out.merge(counts, on=secc_id_col, how="left")
        secc_out["MANZANAS"] = secc_out["MANZANAS"].fillna(0).astype(int)

        st.session_state["ICC_RESULT"] = {"counts": counts, "secc_out": secc_out}

if cbtn2.button("🧹 Limpiar resultado", width="stretch"):
    st.session_state["ICC_RESULT"] = None

res = st.session_state["ICC_RESULT"]
if res is None:
    st.info("Cuando le des **Calcular conteo**, aquí se quedará guardado el resultado y ya no se “cicla”.")
    st.stop()

counts = res["counts"]
secc_out = res["secc_out"]

st.success("Conteo listo ✅")

m1, m2, m3 = st.columns(3)
m1.metric("Secciones", f"{len(secc_out):,}")
m2.metric("Manzanas contadas", f"{int(secc_out['MANZANAS'].sum()):,}")
m3.metric("Promedio por sección", f"{secc_out['MANZANAS'].mean():.2f}")

cT, cM = st.columns([1, 1.35])

with cT:
    st.dataframe(counts, width="stretch", height=560)

    st.download_button(
        "⬇️ CSV",
        data=counts.to_csv(index=False).encode("utf-8"),
        file_name="conteo_manzanas_por_seccion.csv",
        mime="text/csv",
        width="stretch",
    )

    st.download_button(
        "⬇️ Excel",
        data=to_excel_bytes(counts, sheet="MANZANAS_X_SECCION"),
        file_name="conteo_manzanas_por_seccion.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        width="stretch",
    )

with cM:
    st.subheader("🗺️ Mapa (coloreado por conteo)")

    lat, lon = safe_center(secc_out)
    m = folium.Map(location=[lat, lon], zoom_start=12, tiles=None, control_scale=True)

    if basemap == "Relieve (Esri)":
        folium.TileLayer(
            tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Shaded_Relief/MapServer/tile/{z}/{y}/{x}",
            name="Relieve (Esri)",
            attr="Tiles © Esri",
            overlay=False,
            control=True,
        ).add_to(m)
    elif basemap == "Topográfico (OpenTopoMap)":
        folium.TileLayer(
            tiles="https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png",
            name="Topográfico (OpenTopoMap)",
            attr="© OpenTopoMap / © OpenStreetMap contributors",
            overlay=False,
            control=True,
        ).add_to(m)
    else:
        folium.TileLayer("OpenStreetMap", name="Calles (OSM)", overlay=False, control=True).add_to(m)

    def style_fn(feat):
        v = int(feat["properties"].get("MANZANAS", 0))
        if v == 0:
            return {"weight": 1, "fillOpacity": 0.06, "color": "#444"}
        if v <= 20:
            return {"weight": 2, "fillOpacity": 0.12, "color": "#1f77b4"}
        if v <= 50:
            return {"weight": 2, "fillOpacity": 0.14, "color": "#ff7f0e"}
        return {"weight": 2, "fillOpacity": 0.16, "color": "#d62728"}

    tooltip_fields = [secc_id_col, "MANZANAS"]
    aliases = ["Sección:", "Manzanas:"]

    if distrito_col != "(no usar)":
        tooltip_fields.append(distrito_col)
        aliases.append("Distrito:")
    if mun_col != "(no usar)":
        tooltip_fields.append(mun_col)
        aliases.append("Municipio:")

    folium.GeoJson(
        secc_out.to_json(),
        name="Secciones + Conteo",
        style_function=style_fn,
        tooltip=folium.GeoJsonTooltip(fields=tooltip_fields, aliases=aliases, sticky=False),
    ).add_to(m)

    folium.LayerControl(collapsed=False).add_to(m)

    b = secc_out.total_bounds
    m.fit_bounds([[b[1], b[0]], [b[3], b[2]]])

    st_folium(m, use_container_width=True, height=610)
