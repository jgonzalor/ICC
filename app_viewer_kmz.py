# icc/app_viewer_kmz.py
# ICC — Manzanas (cuadras) por Sección Electoral (INE x INEGI)
# - ZIP INE (BGD) + ZIP INEGI (Marco)
# - Detecta SHP, permite elegir capas
# - Robustez: sidecars, geometría activa, CRS
# - Conteo manzanas por sección + mapa relieve/topo/calles

from __future__ import annotations

import io
import os
import re
import zipfile
import tempfile
import hashlib
from typing import List, Tuple, Optional, Dict

import pandas as pd
import streamlit as st

try:
    import geopandas as gpd
    from geopandas.array import GeometryDtype
except Exception:
    st.error("Falta geopandas/shapely en tu entorno. Revisa requirements.txt.")
    st.stop()

import folium
from streamlit_folium import st_folium


# =========================
# UI
# =========================
st.set_page_config(page_title="ICC — Manzanas por Sección", page_icon="🗺️", layout="wide")
st.title("🗺️ ICC — Manzanas (cuadras) por Sección Electoral")
st.caption("Sube ZIP INE (Secciones) e INEGI (Manzanas). Luego contamos cuántas manzanas caen dentro de cada sección.")


# =========================
# ZIP helpers
# =========================
def _md5(b: bytes) -> str:
    return hashlib.md5(b).hexdigest()


def _safe_extract_zip_bytes(zip_bytes: bytes, out_dir: str) -> None:
    """Extrae zip_bytes en out_dir."""
    zpath = os.path.join(out_dir, "root.zip")
    with open(zpath, "wb") as f:
        f.write(zip_bytes)
    with zipfile.ZipFile(zpath, "r") as z:
        z.extractall(out_dir)


def _extract_nested_zips(base_dir: str, max_depth: int = 2) -> None:
    """Extrae ZIPs anidados (hasta max_depth)."""
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
                    with zipfile.ZipFile(z, "r") as zz:
                        zz.extractall(out)
                except Exception:
                    pass


def prepare_zip_workspace(zip_bytes: bytes, key: str) -> Tuple[str, List[str]]:
    """Extrae ZIP (y zip anidados) a workspace cacheado; regresa (dir, lista shp rel)."""
    h = _md5(zip_bytes)
    ss_key = f"_ws_{key}"

    if ss_key in st.session_state:
        item = st.session_state[ss_key]
        if item.get("hash") == h and os.path.exists(item.get("dir", "")):
            return item["dir"], item["shps"]

    ws = tempfile.mkdtemp(prefix=f"{key}_")
    _safe_extract_zip_bytes(zip_bytes, ws)
    _extract_nested_zips(ws, max_depth=2)

    shps = []
    for root, _, files in os.walk(ws):
        for fn in files:
            if fn.lower().endswith(".shp"):
                shps.append(os.path.relpath(os.path.join(root, fn), ws))
    shps = sorted(shps)

    st.session_state[ss_key] = {"hash": h, "dir": ws, "shps": shps}
    return ws, shps


def _auto_pick_shp(shps: List[str], kind: str) -> int:
    """
    Autoselección:
    - INE secciones: contiene 'SECCION'/'SECC'
    - INEGI manzanas: patrón /25m.shp (o /..m.shp) o contiene 'MANZ'
    """
    low = [s.lower() for s in shps]
    if kind == "ine_seccion":
        for i, s in enumerate(low):
            if "seccion" in s or "secc" in s:
                return i
        return 0

    if kind == "inegi_manzana":
        # típico: 25m.shp para Sinaloa
        for i, s in enumerate(low):
            if re.search(r"(^|/)\d{2}m\.shp$", s):
                return i
        # alternativa: contiene manz/manzana/mza
        for i, s in enumerate(low):
            if "manz" in s or "manzana" in s or "mza" in s:
                return i
        return 0

    return 0


def _sidecar_missing(shp_path: str) -> List[str]:
    """Verifica sidecars básicos del shapefile."""
    base = os.path.splitext(shp_path)[0]
    needed = [base + ".dbf", base + ".shx", base + ".prj"]
    miss = [os.path.basename(p) for p in needed if not os.path.exists(p)]
    return miss


def _ensure_geometry_active(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Asegura que el GeoDataFrame tenga geometría activa.
    Si no, intenta encontrar una columna geométrica y setearla.
    """
    # Caso normal
    try:
        _ = gdf.geometry  # noqa
        return gdf
    except Exception:
        pass

    # Intento 1: columna llamada GEOMETRY
    for cand in ["GEOMETRY", "geometry", "Shape", "SHAPE", "geom", "GEOM"]:
        if cand in gdf.columns:
            try:
                return gdf.set_geometry(cand)
            except Exception:
                pass

    # Intento 2: primera columna de tipo geometry (si la detecta)
    for c in gdf.columns:
        try:
            if isinstance(gdf[c].dtype, GeometryDtype):
                return gdf.set_geometry(c)
        except Exception:
            pass

    # Intento 3: inspección por valores shapely (geom_type)
    for c in gdf.columns:
        s = gdf[c].dropna()
        if len(s) == 0:
            continue
        v = s.iloc[0]
        if hasattr(v, "geom_type"):
            try:
                return gdf.set_geometry(c)
            except Exception:
                pass

    return gdf  # no se pudo


def read_gdf(ws_dir: str, shp_rel: str, label: str) -> gpd.GeoDataFrame:
    shp_path = os.path.join(ws_dir, shp_rel)
    if not os.path.exists(shp_path):
        raise FileNotFoundError(f"No existe el SHP seleccionado: {shp_rel}")

    miss = _sidecar_missing(shp_path)
    if miss:
        st.warning(
            f"⚠️ La capa **{os.path.basename(shp_rel)}** parece incompleta (faltan: {', '.join(miss)}). "
            "Esto puede causar que se lea sin geometría."
        )

    gdf = gpd.read_file(shp_path)
    # normalizar columnas
    gdf.columns = [str(c).strip().upper() for c in gdf.columns]

    # asegurar geometría activa antes de tocar CRS
    gdf = _ensure_geometry_active(gdf)

    # debug si sigue sin geometría
    try:
        _ = gdf.geometry
        has_geom = True
    except Exception:
        has_geom = False

    if not has_geom:
        with st.expander(f"🧪 Debug: {label} se leyó SIN geometría (revisa capa/ZIP)", expanded=True):
            st.write("Archivo:", shp_rel)
            st.write("Columnas:", list(gdf.columns))
            st.write("Primeras filas:", gdf.head(5))
            st.write("Sidecars faltantes:", miss)
        raise AttributeError(f"La capa {label} se leyó sin geometría activa.")

    # CRS -> EPSG:4326 (para mapa)
    try:
        if gdf.crs is None:
            st.warning(f"⚠️ {label} no trae CRS. Asumiendo EPSG:4326 para visualizar.")
            gdf = gdf.set_crs(epsg=4326, allow_override=True)
        gdf = gdf.to_crs(epsg=4326)
    except Exception:
        st.warning(f"⚠️ No pude convertir CRS de {label}. Intento forzar EPSG:4326.")
        gdf = gdf.set_crs(epsg=4326, allow_override=True)

    # limpiar nulos de geometría
    gdf = gdf[gdf.geometry.notna()].copy()

    # arreglar geometría (opcional)
    try:
        gdf["geometry"] = gdf.geometry.buffer(0)
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
    return ( (miny + maxy) / 2.0, (minx + maxx) / 2.0 )


def to_excel_bytes(df: pd.DataFrame, sheet: str = "RESUMEN") -> bytes:
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name=sheet)
    return out.getvalue()


# =========================
# Uploads
# =========================
c1, c2, c3 = st.columns([1, 1, 1])
with c1:
    ine_zip_file = st.file_uploader("ZIP INE — Secciones electorales", type=["zip"])
with c2:
    inegi_zip_file = st.file_uploader("ZIP INEGI — Manzanas (Marco Geoestadístico)", type=["zip"])
with c3:
    basemap = st.selectbox("Basemap", ["Relieve (Esri)", "Topográfico (OpenTopoMap)", "Calles (OSM)"], index=0)

if not ine_zip_file or not inegi_zip_file:
    st.info("Sube ambos ZIP para continuar.")
    st.stop()

ine_bytes = ine_zip_file.getvalue()
inegi_bytes = inegi_zip_file.getvalue()

with st.spinner("📦 Preparando ZIP INE..."):
    ws_ine, shps_ine = prepare_zip_workspace(ine_bytes, "INE")
with st.spinner("📦 Preparando ZIP INEGI..."):
    ws_inegi, shps_inegi = prepare_zip_workspace(inegi_bytes, "INEGI")

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
    fil_ine = st.text_input("Filtrar lista INE (ej: secc, seccion)", value="secc")
with f2:
    fil_inegi = st.text_input("Filtrar lista INEGI (tip: escribe 'm.shp' o '25m')", value="25m")

def _filter(items: List[str], q: str) -> List[str]:
    q = (q or "").strip().lower()
    if not q:
        return items
    return [x for x in items if q in x.lower()]

ine_view = _filter(shps_ine, fil_ine) or shps_ine
inegi_view = _filter(shps_inegi, fil_inegi) or shps_inegi

# autoselección
ine_idx = _auto_pick_shp(ine_view, "ine_seccion")
inegi_idx = _auto_pick_shp(inegi_view, "inegi_manzana")

cA, cB = st.columns(2)
with cA:
    shp_ine_choice = st.selectbox("INE: SHP de SECCIONES", ine_view, index=min(ine_idx, len(ine_view)-1))
with cB:
    shp_inegi_choice = st.selectbox("INEGI: SHP de MANZANAS", inegi_view, index=min(inegi_idx, len(inegi_view)-1))

# aviso si eligieron AGEB por error
if re.search(r"(^|/)\d{2}a\.shp$", shp_inegi_choice.lower()):
    st.warning("⚠️ Ese archivo parece **AGEB (..a.shp)**, no manzana. Busca **..m.shp** (ej: 25m.shp).")

with st.spinner("🧠 Leyendo capas..."):
    try:
        secc = read_gdf(ws_ine, shp_ine_choice, label="INE (Secciones)")
    except Exception as e:
        st.error(f"No pude leer INE (Secciones): {e}")
        st.stop()

    try:
        mza = read_gdf(ws_inegi, shp_inegi_choice, label="INEGI (Manzanas)")
    except Exception as e:
        st.error(f"No pude leer INEGI (Manzanas): {e}")
        st.stop()

st.success(f"INE Secciones: {len(secc):,} | INEGI Capa: {len(mza):,}")

# =========================
# Columnas / filtros
# =========================
st.divider()
st.subheader("🎛️ Columnas / filtros")

guess_seccion = pick_col(list(secc.columns), ["SECCION", "SECC", "CVE_SECC", "ID_SECC"])
guess_distrito = pick_col(list(secc.columns), ["DISTRITO", "DTO", "DIST", "CVE_DIST"])
guess_mun = pick_col(list(secc.columns), ["MUNICIPIO", "NOM_MUN", "NOM_MPIO", "CVE_MUN", "MUN"])

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

# performance: bbox
minx, miny, maxx, maxy = secc_f.total_bounds
mza_f = mza.cx[minx:maxx, miny:maxy].copy()
if mza_f.empty:
    st.warning("No quedaron elementos INEGI dentro del bbox. OJO: tal vez no elegiste manzanas.")
    st.stop()

st.write(f"Secciones filtradas: **{len(secc_f):,}** | Elementos INEGI (bbox): **{len(mza_f):,}**")

# =========================
# Conteo
# =========================
st.divider()
st.subheader("📊 Conteo de manzanas por sección")

pred = st.selectbox("Regla espacial", ["intersects", "within"], index=0)
run = st.button("🚀 Calcular conteo", use_container_width=True)
if not run:
    st.stop()

with st.spinner("Cruzando (sjoin) y contando..."):
    secc_p = secc_f.to_crs(epsg=3857)
    mza_p = mza_f.to_crs(epsg=3857)

    joined = gpd.sjoin(mza_p[["geometry"]], secc_p[[secc_id_col, "geometry"]], how="inner", predicate=pred)

    counts = (
        joined.groupby(secc_id_col)
        .size()
        .reset_index(name="MANZANAS")
        .sort_values("MANZANAS", ascending=False)
    )

    secc_out = secc_f.copy()
    secc_out[secc_id_col] = secc_out[secc_id_col].astype(str)
    counts[secc_id_col] = counts[secc_id_col].astype(str)

    secc_out = secc_out.merge(counts, on=secc_id_col, how="left")
    secc_out["MANZANAS"] = secc_out["MANZANAS"].fillna(0).astype(int)

st.success("Conteo listo ✅")

m1, m2, m3 = st.columns(3)
m1.metric("Secciones", f"{len(secc_out):,}")
m2.metric("Manzanas contadas", f"{int(secc_out['MANZANAS'].sum()):,}")
m3.metric("Promedio por sección", f"{secc_out['MANZANAS'].mean():.2f}")

cT, cM = st.columns([1, 1.35])

with cT:
    st.dataframe(counts, use_container_width=True, height=560)
    st.download_button("⬇️ CSV", counts.to_csv(index=False).encode("utf-8"), "conteo_manzanas_por_seccion.csv", "text/csv", use_container_width=True)
    st.download_button(
        "⬇️ Excel",
        to_excel_bytes(counts, sheet="MANZANAS_X_SECCION"),
        "conteo_manzanas_por_seccion.xlsx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
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
        tooltip_fields.append(distrito_col); aliases.append("Distrito:")
    if mun_col != "(no usar)":
        tooltip_fields.append(mun_col); aliases.append("Municipio:")

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
