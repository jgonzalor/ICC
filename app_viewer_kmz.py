# icc/app_viewer_kmz.py
# ICC — Manzanas (cuadras) por Sección Electoral (INE x INEGI)
# - Carga ZIP INE (BGD) y ZIP INEGI (Marco Geoestadístico)
# - Detecta TODOS los .shp (incluye ZIPs anidados) y te deja elegir capas
# - Conteo de manzanas por sección (spatial join)
# - Mapa con relieve/topo/calles + tabla + export

from __future__ import annotations

import io
import os
import zipfile
import tempfile
import hashlib
from typing import Dict, List, Tuple, Optional

import pandas as pd
import streamlit as st

try:
    import geopandas as gpd
except ModuleNotFoundError:
    st.error("Falta instalar 'geopandas'. Revisa tu requirements.txt.")
    st.stop()

import folium
from streamlit_folium import st_folium


# =========================
# UI
# =========================
st.set_page_config(page_title="ICC — Manzanas por Sección", page_icon="🗺️", layout="wide")

st.title("🗺️ ICC — Manzanas (cuadras) por Sección Electoral")
st.caption(
    "Sube los ZIP de INE (Secciones) e INEGI (Manzanas). "
    "Esta versión detecta múltiples SHP (y ZIPs anidados) y te deja elegir el correcto."
)


# =========================
# ZIP helpers (robustos)
# =========================
def _md5(b: bytes) -> str:
    return hashlib.md5(b).hexdigest()


def _safe_extract_zip(zip_path: str, out_dir: str) -> None:
    """Extrae zip_path a out_dir (no revienta si hay archivos raros)."""
    try:
        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(out_dir)
    except Exception:
        # Si el zip trae un archivo corrupto/extraño, igual seguimos con lo demás
        try:
            with zipfile.ZipFile(zip_path, "r") as z:
                for name in z.namelist():
                    try:
                        z.extract(name, out_dir)
                    except Exception:
                        pass
        except Exception:
            pass


def _extract_zip_bytes_to_dir(zip_bytes: bytes, out_dir: str) -> str:
    """Guarda bytes como root.zip y lo extrae."""
    root_zip = os.path.join(out_dir, "root.zip")
    with open(root_zip, "wb") as f:
        f.write(zip_bytes)
    _safe_extract_zip(root_zip, out_dir)
    return out_dir


def _extract_nested_zips(base_dir: str, max_depth: int = 2) -> None:
    """
    Busca ZIPs dentro del directorio y los extrae a subcarpetas.
    max_depth=2 suele ser suficiente para INE/INEGI.
    """
    for depth in range(max_depth):
        nested = []
        for root, _, files in os.walk(base_dir):
            for fn in files:
                if fn.lower().endswith(".zip") and fn.lower() != "root.zip":
                    nested.append(os.path.join(root, fn))

        if not nested:
            return

        for zpath in nested:
            sub = zpath + "_unzipped"
            if not os.path.exists(sub):
                os.makedirs(sub, exist_ok=True)
                _safe_extract_zip(zpath, sub)


def _list_files(base_dir: str, limit: int = 300) -> List[str]:
    out = []
    for root, _, files in os.walk(base_dir):
        for fn in files:
            rel = os.path.relpath(os.path.join(root, fn), base_dir)
            out.append(rel)
            if len(out) >= limit:
                return out
    return out


def prepare_zip_workspace(zip_bytes: bytes, key: str) -> Tuple[str, List[str]]:
    """
    Extrae ZIP (y ZIPs anidados) a un workspace persistente en session_state.
    Regresa: (workspace_dir, lista_relativa_de_shp)
    """
    h = _md5(zip_bytes)
    ss_key = f"_zip_ws_{key}"

    # Reusar si ya está el mismo archivo
    if ss_key in st.session_state:
        if st.session_state[ss_key].get("hash") == h and os.path.exists(st.session_state[ss_key].get("dir", "")):
            ws = st.session_state[ss_key]["dir"]
            shps = st.session_state[ss_key]["shps"]
            return ws, shps

    # Si cambia archivo: crear workspace nuevo
    ws = tempfile.mkdtemp(prefix=f"{key}_")
    _extract_zip_bytes_to_dir(zip_bytes, ws)
    _extract_nested_zips(ws, max_depth=2)

    # listar shp
    shps = []
    for root, _, files in os.walk(ws):
        for fn in files:
            if fn.lower().endswith(".shp"):
                shps.append(os.path.relpath(os.path.join(root, fn), ws))
    shps = sorted(shps)

    st.session_state[ss_key] = {"hash": h, "dir": ws, "shps": shps}
    return ws, shps


def read_gdf(ws_dir: str, shp_rel: str) -> gpd.GeoDataFrame:
    shp_path = os.path.join(ws_dir, shp_rel)
    if not os.path.exists(shp_path):
        raise FileNotFoundError(f"No existe el SHP seleccionado: {shp_rel}")

    gdf = gpd.read_file(shp_path)
    gdf = gdf[gdf.geometry.notna()].copy()
    gdf.columns = [str(c).strip().upper() for c in gdf.columns]

    # CRS -> EPSG:4326 para mapa
    if gdf.crs is None:
        st.warning("⚠️ La capa no trae CRS. Asumiendo EPSG:4326 para visualización.")
        gdf = gdf.set_crs(epsg=4326, allow_override=True)
    gdf = gdf.to_crs(epsg=4326)

    # intentar arreglar geometrías inválidas
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
    ine_zip_file = st.file_uploader("ZIP INE — Secciones electorales (BGD)", type=["zip"])
with c2:
    inegi_zip_file = st.file_uploader("ZIP INEGI — Manzanas (Marco Geoestadístico)", type=["zip"])
with c3:
    basemap = st.selectbox("Basemap", ["Relieve (Esri)", "Topográfico (OpenTopoMap)", "Calles (OSM)"], index=0)

if not ine_zip_file or not inegi_zip_file:
    st.info("Sube ambos ZIP para continuar.")
    st.stop()

ine_bytes = ine_zip_file.getvalue()
inegi_bytes = inegi_zip_file.getvalue()

# preparar workspaces
with st.spinner("📦 Preparando ZIP INE (incluye ZIPs anidados)..."):
    ws_ine, shps_ine = prepare_zip_workspace(ine_bytes, key="INE")

with st.spinner("📦 Preparando ZIP INEGI (incluye ZIPs anidados)..."):
    ws_inegi, shps_inegi = prepare_zip_workspace(inegi_bytes, key="INEGI")

# Validación fuerte (aquí ya no tronamos con FileNotFoundError)
if not shps_ine:
    st.error("No encontré ningún .shp dentro del ZIP del INE (ni dentro de ZIPs anidados).")
    st.write("Archivos detectados (muestra):", _list_files(ws_ine, limit=200))
    st.stop()

if not shps_inegi:
    st.error("No encontré ningún .shp dentro del ZIP del INEGI (ni dentro de ZIPs anidados).")
    st.write("Archivos detectados (muestra):", _list_files(ws_inegi, limit=200))
    st.stop()

# selector con filtro (para listas largas)
st.divider()
st.subheader("🧩 Elige las capas correctas (.shp)")

f1, f2 = st.columns(2)
with f1:
    fil_ine = st.text_input("Filtrar lista INE (ej: secc, seccion, sección)", value="secc")
with f2:
    fil_inegi = st.text_input("Filtrar lista INEGI (ej: mza, manzana)", value="mza")

def _filter_list(items: List[str], q: str) -> List[str]:
    q = (q or "").strip().lower()
    if not q:
        return items
    return [x for x in items if q in x.lower()]

shps_ine_view = _filter_list(shps_ine, fil_ine)
shps_inegi_view = _filter_list(shps_inegi, fil_inegi)

if not shps_ine_view:
    shps_ine_view = shps_ine
if not shps_inegi_view:
    shps_inegi_view = shps_inegi

cA, cB = st.columns(2)
with cA:
    shp_ine_choice = st.selectbox("INE: elige SHP de SECCIONES", shps_ine_view, index=0)
with cB:
    shp_inegi_choice = st.selectbox("INEGI: elige SHP de MANZANAS", shps_inegi_view, index=0)

# cargar capas
with st.spinner("🧠 Leyendo capas seleccionadas..."):
    secc = read_gdf(ws_ine, shp_ine_choice)
    mza = read_gdf(ws_inegi, shp_inegi_choice)

st.success(f"INE Secciones: {len(secc):,} | INEGI Manzanas: {len(mza):,}")

# =========================
# Config columnas y filtros
# =========================
st.divider()
st.subheader("🎛️ Columnas / filtros (para quedarte solo con Ahome / Distrito 05 si existe)")

guess_seccion = pick_col(list(secc.columns), ["SECCION", "SECC", "CVE_SECC", "ID_SECC", "SECCION_E", "SECCION_I"])
guess_distrito = pick_col(list(secc.columns), ["DISTRITO", "DTO", "DIST", "DISTRITO_F", "DISTRITO_L", "CVE_DIST"])
guess_mun = pick_col(list(secc.columns), ["MUNICIPIO", "NOM_MUN", "NOM_MPIO", "CVE_MUN", "MUN"])

k1, k2, k3 = st.columns(3)
with k1:
    secc_id_col = st.selectbox(
        "Columna ID de sección",
        options=sorted(secc.columns),
        index=(sorted(secc.columns).index(guess_seccion) if guess_seccion in secc.columns else 0),
    )
with k2:
    distrito_col = st.selectbox(
        "Columna distrito (opcional)",
        options=["(no usar)"] + sorted(secc.columns),
        index=(1 + sorted(secc.columns).index(guess_distrito) if guess_distrito in secc.columns else 0),
    )
with k3:
    mun_col = st.selectbox(
        "Columna municipio (opcional)",
        options=["(no usar)"] + sorted(secc.columns),
        index=(1 + sorted(secc.columns).index(guess_mun) if guess_mun in secc.columns else 0),
    )

secc_f = secc.copy()

if distrito_col != "(no usar)":
    vals = sorted(secc_f[distrito_col].dropna().astype(str).unique().tolist())
    distrito_sel = st.selectbox("Filtrar por distrito", ["(todos)"] + vals, index=0)
    if distrito_sel != "(todos)":
        secc_f = secc_f[secc_f[distrito_col].astype(str) == str(distrito_sel)].copy()

if mun_col != "(no usar)":
    vals = sorted(secc_f[mun_col].dropna().astype(str).unique().tolist())
    # intento default Ahome
    def_idx = 0
    for i, v in enumerate(vals):
        if "ahome" in str(v).lower():
            def_idx = i + 1
            break
    mun_sel = st.selectbox("Filtrar por municipio", ["(todos)"] + vals, index=def_idx)
    if mun_sel != "(todos)":
        secc_f = secc_f[secc_f[mun_col].astype(str) == str(mun_sel)].copy()

if secc_f.empty:
    st.error("Con esos filtros no quedó ninguna sección.")
    st.stop()

# recorte bbox (performance)
minx, miny, maxx, maxy = secc_f.total_bounds
mza_f = mza.cx[minx:maxx, miny:maxy].copy()
if mza_f.empty:
    st.warning("No quedaron manzanas dentro del bbox de las secciones filtradas (revisa que sí sea capa de MANZANA).")
    st.stop()

st.write(f"Secciones filtradas: **{len(secc_f):,}** | Manzanas (bbox): **{len(mza_f):,}**")

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
    # Proyectar a 3857 para join más estable
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

    st.download_button(
        "⬇️ Descargar CSV",
        data=counts.to_csv(index=False).encode("utf-8"),
        file_name="conteo_manzanas_por_seccion.csv",
        mime="text/csv",
        use_container_width=True,
    )
    st.download_button(
        "⬇️ Descargar Excel",
        data=to_excel_bytes(counts, sheet="MANZANAS_X_SECCION"),
        file_name="conteo_manzanas_por_seccion.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )

with cM:
    st.subheader("🗺️ Mapa (coloreado por conteo)")

    lat, lon = safe_center(secc_out)
    m = folium.Map(location=[lat, lon], zoom_start=12, tiles=None, control_scale=True)

    # basemap
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

    # estilo por rangos
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
