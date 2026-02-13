
# app_mapa_1zip.py
# 🗺️ ICC — Mapa con 1 ZIP (Secciones INE + Manzanas INEGI)
#
# ✅ Filtro por VARIAS SECCIONES (multiselect)
# ✅ Secciones con color distinto (por sección) + etiquetas visibles (número de sección)
# ✅ Exportar a KMZ con POLÍGONOS COLOREADOS (por sección)  ← (lo que pediste)
# ✅ “Imprimir pantalla”: descarga del HTML del mapa (ábrelo en navegador y Ctrl+P / Imprimir)
#
# - Sube SOLO 1 ZIP que contenga ambos SHP:
#     * Secciones (INE): ...SECCION*.shp o ...SECCIONES*.shp
#     * Manzanas (INEGI): ...MANZANAS*.shp o ...25m.shp
# - Filtros: Distrito local/federal, Municipio y (multi) Sección
# - Mapa base: relieve/topo/calles/satélite
# - Tablas + export CSV/Excel/KMZ/HTML del mapa

from __future__ import annotations

import io
import os
import re
import zipfile
import tempfile
import hashlib
from typing import List, Tuple, Optional, Iterable, Dict

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
# Color helpers for sections
# -------------------------
def _hsl_to_rgb(h: float, s: float, l: float) -> Tuple[int, int, int]:
    """h in [0,360), s,l in [0,1]"""
    c = (1 - abs(2*l - 1)) * s
    hp = h / 60.0
    x = c * (1 - abs((hp % 2) - 1))
    if 0 <= hp < 1:
        r1, g1, b1 = c, x, 0
    elif 1 <= hp < 2:
        r1, g1, b1 = x, c, 0
    elif 2 <= hp < 3:
        r1, g1, b1 = 0, c, x
    elif 3 <= hp < 4:
        r1, g1, b1 = 0, x, c
    elif 4 <= hp < 5:
        r1, g1, b1 = x, 0, c
    else:
        r1, g1, b1 = c, 0, x
    m = l - c/2
    r, g, b = r1 + m, g1 + m, b1 + m
    return int(round(r*255)), int(round(g*255)), int(round(b*255))


def color_for_section(section_value) -> str:
    """Color determinístico por sección (hex #RRGGBB)."""
    try:
        v = int(section_value)
    except Exception:
        v = abs(hash(str(section_value))) % 100000
    h = (v * 47) % 360
    r, g, b = _hsl_to_rgb(h, 0.65, 0.55)
    return f"#{r:02x}{g:02x}{b:02x}"


# -------------------------
# KML / KMZ export helpers (con estilos por sección)
# -------------------------
def xml_escape(s: str) -> str:
    s = "" if s is None else str(s)
    return (s.replace("&", "&amp;")
             .replace("<", "&lt;")
             .replace(">", "&gt;")
             .replace('"', "&quot;")
             .replace("'", "&apos;"))


def kml_color_from_hex(hex_rgb: str, alpha: int = 140) -> str:
    """
    KML usa AABBGGRR (no RRGGBBAA).
    alpha: 0-255 (0 transparente, 255 opaco)
    """
    h = (hex_rgb or "#999999").lstrip("#")
    if len(h) != 6:
        h = "999999"
    rr = int(h[0:2], 16)
    gg = int(h[2:4], 16)
    bb = int(h[4:6], 16)
    return f"{alpha:02x}{bb:02x}{gg:02x}{rr:02x}"


def style_id_for(val) -> str:
    s = "S_" + re.sub(r"[^A-Za-z0-9_]+", "_", str(val))
    return s[:60]  # corto


def iter_polygons(geom) -> Iterable:
    if geom is None:
        return []
    gt = geom.geom_type
    if gt == "Polygon":
        return [geom]
    if gt == "MultiPolygon":
        return list(geom.geoms)
    if gt == "GeometryCollection":
        out = []
        for g in geom.geoms:
            out.extend(iter_polygons(g))
        return out
    return []


def ring_to_kml_coords(ring) -> str:
    coords = list(ring.coords)
    return " ".join([f"{x:.8f},{y:.8f},0" for x, y in coords])


def polygon_to_kml(poly) -> str:
    outer = ring_to_kml_coords(poly.exterior)
    inners = ""
    for interior in poly.interiors:
        inners += f"""
        <innerBoundaryIs>
          <LinearRing>
            <coordinates>{ring_to_kml_coords(interior)}</coordinates>
          </LinearRing>
        </innerBoundaryIs>"""
    return f"""
    <Polygon>
      <outerBoundaryIs>
        <LinearRing>
          <coordinates>{outer}</coordinates>
        </LinearRing>
      </outerBoundaryIs>
      {inners}
    </Polygon>"""


def geom_to_kml(geom) -> str:
    polys = iter_polygons(geom)
    if not polys:
        return ""
    if len(polys) == 1:
        return polygon_to_kml(polys[0])
    parts = "\n".join([polygon_to_kml(p) for p in polys])
    return f"<MultiGeometry>\n{parts}\n</MultiGeometry>"


def build_section_styles(secc_gdf: gpd.GeoDataFrame, section_col: Optional[str], alpha_fill: int = 140) -> Tuple[str, Dict[str, str]]:
    """
    Retorna:
      - styles_xml: <Style ...>...</Style> concatenado
      - value_to_styleid: dict(str(section)->styleId)
    """
    if not section_col or section_col not in secc_gdf.columns:
        return "", {}

    values = sorted(secc_gdf[section_col].dropna().astype(str).unique().tolist())
    mapping: Dict[str, str] = {}
    style_chunks = []

    for v in values:
        sid = style_id_for(v)
        mapping[v] = sid
        hex_rgb = color_for_section(v)
        poly = kml_color_from_hex(hex_rgb, alpha=alpha_fill)
        line = "ff000000"  # negro opaco
        style_chunks.append(f"""
  <Style id="{xml_escape(sid)}">
    <LineStyle><color>{line}</color><width>2</width></LineStyle>
    <PolyStyle><color>{poly}</color><fill>1</fill><outline>1</outline></PolyStyle>
  </Style>
        """)
    return "\n".join(style_chunks), mapping


def gdf_to_kml_folder(
    gdf: gpd.GeoDataFrame,
    folder_name: str,
    name_col: Optional[str],
    max_features: int,
    style_by_col: Optional[str] = None,
    style_map: Optional[Dict[str, str]] = None,
) -> str:
    gdf2 = gdf.copy()
    if len(gdf2) > max_features:
        gdf2 = gdf2.sample(max_features, random_state=7).copy()

    placemarks = []
    cols = [c for c in gdf2.columns if c != "geometry"]

    for _, row in gdf2.iterrows():
        kml_geom = geom_to_kml(row.geometry)
        if not kml_geom:
            continue

        nm = ""
        if name_col and name_col in gdf2.columns:
            nm = str(row.get(name_col))
        else:
            nm = str(row.get("SECCION") or row.get("SECC") or "")

        # style url (por sección)
        style_url = ""
        if style_by_col and style_map and style_by_col in gdf2.columns:
            key = row.get(style_by_col)
            if key is not None and not pd.isna(key):
                sid = style_map.get(str(key))
                if sid:
                    style_url = f"<styleUrl>#{xml_escape(sid)}</styleUrl>"

        data_items = []
        for c in cols:
            v = row.get(c)
            if pd.isna(v):
                continue
            data_items.append(f'<Data name="{xml_escape(c)}"><value>{xml_escape(v)}</value></Data>')
        ext = f"<ExtendedData>{''.join(data_items)}</ExtendedData>" if data_items else ""

        placemarks.append(f"""
        <Placemark>
          <name>{xml_escape(nm)}</name>
          {style_url}
          {ext}
          {kml_geom}
        </Placemark>
        """)
    return f"<Folder><name>{xml_escape(folder_name)}</name>{''.join(placemarks)}</Folder>"


def build_kml_document(
    secc_gdf: gpd.GeoDataFrame,
    mza_gdf: Optional[gpd.GeoDataFrame],
    section_col_for_style: Optional[str],
    name_col_secc: Optional[str],
    name_col_mza: Optional[str],
    max_manzanas: int,
    alpha_fill: int = 140,
) -> str:
    styles_xml, style_map = build_section_styles(secc_gdf, section_col_for_style, alpha_fill=alpha_fill)

    folders = []
    folders.append(
        gdf_to_kml_folder(
            secc_gdf, "SECCIONES", name_col_secc, max_features=len(secc_gdf),
            style_by_col=section_col_for_style, style_map=style_map
        )
    )
    if mza_gdf is not None and len(mza_gdf) > 0:
        folders.append(gdf_to_kml_folder(mza_gdf, "MANZANAS", name_col_mza, max_features=max_manzanas))

    kml = f"""<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
<Document>
  <name>ICC Export</name>
  {styles_xml}
  {''.join(folders)}
</Document>
</kml>"""
    return kml


def kml_to_kmz_bytes(kml_text: str, doc_name: str = "doc.kml") -> bytes:
    bio = io.BytesIO()
    with zipfile.ZipFile(bio, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr(doc_name, kml_text.encode("utf-8"))
    return bio.getvalue()


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

        if sec_selected:
            secc_f = secc_f[secc_f[col_sec].astype(int).isin(set(int(x) for x in sec_selected))].copy()
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
tab_map, tab_tables, tab_export = st.tabs(["🗺️ Mapa", "📋 Tablas", "⬇️ Exportar (Excel/CSV/KMZ/HTML)"])

# -------------------------
# MAP
# -------------------------
with tab_map:
    st.subheader("Mapa")

    cA, cB, cC = st.columns([1, 1, 2])
    with cA:
        show_manz = st.checkbox("Mostrar manzanas", value=False)
    with cB:
        show_labels = st.checkbox("Mostrar número de sección (etiqueta)", value=True)
    with cC:
        label_size = st.slider("Tamaño etiqueta", min_value=10, max_value=26, value=16, step=1)

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

    sec_col = col_sec if col_sec and col_sec in secc_f.columns else None

    def style_fn(feat):
        props = feat.get("properties", {})
        sec_val = props.get(sec_col) if sec_col else None
        fc = color_for_section(sec_val if sec_val is not None else props.get("id", 0))
        return {"color": "#000000", "weight": 2, "fillColor": fc, "fillOpacity": 0.35}

    folium.GeoJson(
        secc_f.to_json(),
        name="Secciones",
        style_function=style_fn,
        tooltip=folium.GeoJsonTooltip(fields=fields, aliases=aliases, sticky=False) if fields else None,
    ).add_to(m)

    if show_labels and sec_col:
        for _, row in secc_f.iterrows():
            sec_val = row.get(sec_col)
            pt = row.geometry.representative_point()
            folium.Marker(
                location=[pt.y, pt.x],
                icon=folium.DivIcon(
                    html=f"""
                    <div style="
                        font-size:{label_size}px;
                        font-weight:700;
                        color:#111;
                        background:rgba(255,255,255,0.70);
                        border:1px solid rgba(0,0,0,0.35);
                        border-radius:6px;
                        padding:1px 6px;
                        line-height:1.1;
                        white-space:nowrap;">
                        {sec_val}
                    </div>
                    """
                )
            ).add_to(m)

    if show_manz:
        max_feat = 6000
        mz_show = mza_bbox.sample(max_feat, random_state=7).copy() if len(mza_bbox) > max_feat else mza_bbox
        folium.GeoJson(
            mz_show.to_json(),
            name="Manzanas",
            style_function=lambda feat: {"weight": 1, "fillOpacity": 0.04},
        ).add_to(m)

    folium.LayerControl(collapsed=False).add_to(m)
    bds = secc_f.total_bounds
    m.fit_bounds([[bds[1], bds[0]], [bds[3], bds[2]]])

    st.session_state["LAST_MAP_HTML"] = m.get_root().render()
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

    resumen = {"SECCIONES": len(secc_f), "MANZANAS_RECORTE": len(mza_bbox)}
    if col_manz:
        resumen["MANZANAS_SUM_SECCIONES"] = int(secc_f[col_manz].fillna(0).sum())
    if col_p18:
        resumen["POB18MAS_TOTAL"] = int(pd.to_numeric(secc_f[col_p18], errors="coerce").fillna(0).sum())
    if col_vot:
        resumen["VOTANTES_TOTAL"] = int(pd.to_numeric(secc_f[col_vot], errors="coerce").fillna(0).sum())
    df_res = pd.DataFrame([resumen])

    cA, cB = st.columns(2)
    with cA:
        st.download_button(
            "⬇️ Excel (resumen + secciones + manzanas)",
            data=to_excel_bytes({"RESUMEN": df_res, "SECCIONES": df_secc.drop(columns=["geometry"], errors="ignore"), "MANZANAS": df_mza}),
            file_name="export_distrito_secciones_manzanas.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
    with cB:
        st.download_button(
            "⬇️ CSV (secciones)",
            data=df_secc.drop(columns=["geometry"], errors="ignore").to_csv(index=False).encode("utf-8"),
            file_name="secciones_filtradas.csv",
            mime="text/csv",
            use_container_width=True
        )

    st.download_button(
        "⬇️ CSV (manzanas recorte)",
        data=df_mza.to_csv(index=False).encode("utf-8"),
        file_name="manzanas_recorte.csv",
        mime="text/csv",
        use_container_width=True
    )

    st.divider()
    st.subheader("🗺️ Exportar KMZ (Google Earth) — con colores por sección")

    include_manz = st.checkbox("Incluir manzanas en el KMZ (puede pesar)", value=False)
    max_mz = st.slider("Límite máximo de manzanas en KMZ", min_value=500, max_value=20000, value=6000, step=500)
    alpha = st.slider("Transparencia relleno (KMZ)", min_value=40, max_value=220, value=140, step=10,
                      help="Más alto = más opaco. Se aplica al relleno de las secciones en el KMZ.")

    if st.button("Preparar KMZ", use_container_width=True):
        with st.spinner("Generando KMZ..."):
            mza_for_kmz = mza_bbox if include_manz else None
            kml = build_kml_document(
                secc_f,
                mza_for_kmz,
                section_col_for_style=col_sec if col_sec else None,
                name_col_secc=col_sec if col_sec else None,
                name_col_mza=mza_sec if mza_sec else None,
                max_manzanas=max_mz,
                alpha_fill=int(alpha),
            )
            kmz_bytes = kml_to_kmz_bytes(kml, "doc.kml")
            st.session_state["LAST_KMZ"] = kmz_bytes
        st.success("KMZ listo ✅ (ya va coloreado por sección)")

    kmz_bytes = st.session_state.get("LAST_KMZ")
    if kmz_bytes:
        st.download_button(
            "⬇️ Descargar KMZ",
            data=kmz_bytes,
            file_name="export_secciones_manzanas_coloreado.kmz",
            mime="application/vnd.google-earth.kmz",
            use_container_width=True
        )

    st.divider()
    st.subheader("🖨️ Imprimir (pantalla del mapa)")

    map_html = st.session_state.get("LAST_MAP_HTML")
    if not map_html:
        st.info("Primero entra a la pestaña **Mapa** para que se genere el HTML.")
    else:
        st.download_button(
            "⬇️ Descargar HTML del mapa (para imprimir)",
            data=map_html.encode("utf-8"),
            file_name="mapa_filtrado.html",
            mime="text/html",
            use_container_width=True
        )
        st.caption("Abre el HTML en tu navegador y usa **Ctrl+P / Imprimir** (o “Guardar como PDF”).")

st.success("✅ Listo. El KMZ ahora exporta las secciones COLOREADAS por sección.")
