#!/usr/bin/env python
# -*- coding: utf-8 -*-

import io
import zipfile
import random
import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd
import pydeck as pdk
import streamlit as st


st.set_page_config(page_title="Visor KMZ de Secciones", layout="wide")

st.title("Visor web de KMZ — Secciones por Distrito")
st.write(
    """
Sube un archivo **KMZ o KML** o selecciona uno que ya tengas en el repositorio.

- La app leerá el KML interno.
- Detectará **carpetas por Distrito** (Folder) y **Placemarks** con polígonos.
- Intentará respetar el **color de relleno** que venga en el KMZ (PolyStyle o styleUrl).
- Puede pintar el **número de sección dentro de cada polígono** (si activas la opción).
"""
)


def parse_kml_color(color_text: str):
    """
    Convierte un color KML (aabbggrr en hex) a lista [r, g, b, a] (0-255).
    Ejemplo KML: '96ff0000' -> a=0x96, b=0xff, g=0x00, r=0x00 -> [0, 0, 255, 150]
    """
    if not color_text:
        return None
    txt = color_text.strip().lstrip("#")
    if len(txt) != 8:
        return None
    try:
        aa = int(txt[0:2], 16)
        bb = int(txt[2:4], 16)
        gg = int(txt[4:6], 16)
        rr = int(txt[6:8], 16)
        return [rr, gg, bb, aa]
    except Exception:
        return None


def cargar_kmz_o_kml(file_bytes: bytes, filename: str) -> pd.DataFrame:
    """
    Lee un KMZ/KML (bytes + nombre de archivo) y devuelve un DataFrame con:
    - district
    - section
    - polygon  (lista de [lon, lat])
    - centroid_lon
    - centroid_lat
    - color  (lista [r,g,b,a] si venía en el KML, sino None)
    """
    # 1) Obtener texto KML desde KMZ o KML plano
    fname = filename.lower()
    if fname.endswith(".kmz"):
        zf = zipfile.ZipFile(io.BytesIO(file_bytes))
        kml_name = None
        for name in zf.namelist():
            if name.lower().endswith(".kml"):
                kml_name = name
                break
        if kml_name is None:
            raise ValueError("El KMZ no contiene ningún archivo .kml.")
        kml_bytes = zf.read(kml_name)
        kml_text = kml_bytes.decode("utf-8", errors="ignore")
    else:
        # Asumimos KML plano
        kml_text = file_bytes.decode("utf-8", errors="ignore")

    # 2) Parsear XML
    ns = {"kml": "http://www.opengis.net/kml/2.2"}
    root = ET.fromstring(kml_text)

    # --- Mapa de estilos globales (Style id -> color) ---
    style_colors = {}
    for style in root.findall(".//kml:Style", ns):
        style_id = style.attrib.get("id")
        if not style_id:
            continue
        color_elem = style.find(".//kml:PolyStyle/kml:color", ns)
        if color_elem is None or not color_elem.text:
            continue
        rgba = parse_kml_color(color_elem.text)
        if rgba is None:
            continue
        style_colors[style_id] = rgba
        style_colors["#" + style_id] = rgba  # por si styleUrl viene con '#'

    registros = []

    # Buscamos Folders (cada uno suele ser un Distrito)
    for folder in root.findall(".//kml:Folder", ns):
        name_elem = folder.find("kml:name", ns)
        district_name = name_elem.text.strip() if name_elem is not None else "Sin distrito"

        # Dentro del folder, buscamos Placemarks con Polygon
        for placemark in folder.findall("kml:Placemark", ns):
            sec_elem = placemark.find("kml:name", ns)
            section_name = sec_elem.text.strip() if sec_elem is not None else ""

            # 1) PolyStyle directo en el Placemark
            color_rgba = None
            color_elem = placemark.find(".//kml:PolyStyle/kml:color", ns)
            if color_elem is not None and color_elem.text:
                color_rgba = parse_kml_color(color_elem.text)

            # 2) styleUrl que apunta a un Style global
            if color_rgba is None:
                style_url_elem = placemark.find("kml:styleUrl", ns)
                if style_url_elem is not None and style_url_elem.text:
                    style_ref = style_url_elem.text.strip()
                    if style_ref in style_colors:
                        color_rgba = style_colors[style_ref]

            poly = placemark.find(".//kml:Polygon", ns)
            if poly is None:
                continue
            coords_elem = poly.find(".//kml:coordinates", ns)
            if coords_elem is None or not coords_elem.text:
                continue

            coord_pairs = []
            for token in coords_elem.text.replace("\n", " ").split():
                parts = token.split(",")
                if len(parts) < 2:
                    continue
                try:
                    lon = float(parts[0])
                    lat = float(parts[1])
                    coord_pairs.append([lon, lat])
                except ValueError:
                    continue

            if not coord_pairs:
                continue

            lons = [p[0] for p in coord_pairs]
            lats = [p[1] for p in coord_pairs]
            centroid_lon = sum(lons) / len(lons)
            centroid_lat = sum(lats) / len(lats)

            registros.append(
                {
                    "district": district_name,
                    "section_raw": section_name,
                    "polygon": coord_pairs,
                    "centroid_lon": centroid_lon,
                    "centroid_lat": centroid_lat,
                    "color": color_rgba,
                }
            )

    df = pd.DataFrame(registros)
    if df.empty:
        return df

    # Limpieza de texto de sección
    def limpia_sec(s):
        if s is None:
            return ""
        txt = str(s).strip()
        txt = txt.replace("SEC", "").replace("Sec", "").strip()
        return txt

    df["section"] = df["section_raw"].apply(limpia_sec)

    # Campo numérico cuando se pueda
    def to_int_or_none(s):
        try:
            return int(s)
        except Exception:
            return None

    df["section_int"] = df["section"].apply(to_int_or_none)

    return df


# ------------------ SELECCIÓN DE ARCHIVO ------------------ #

st.sidebar.header("Fuente del archivo")

# Revisamos si ya hay archivos en 'dir' para decidir el modo por defecto
repo_dir = Path("dir")
repo_files = []
if repo_dir.exists():
    repo_files = sorted(
        [p.name for p in repo_dir.glob("*.kmz")] +
        [p.name for p in repo_dir.glob("*.kml")]
    )

# Si hay archivos en 'dir', arrancamos en "Archivo del repositorio"
default_radio_index = 1 if repo_files else 0

modo_archivo = st.sidebar.radio(
    "¿De dónde tomamos el KMZ/KML?",
    ["Subir archivo", "Archivo del repositorio"],
    index=default_radio_index,
)

file_bytes = None
filename = None

if modo_archivo == "Subir archivo":
    uploaded_kmz = st.file_uploader(
        "Sube tu archivo KMZ/KML con las secciones",
        type=["kmz", "kml"]
    )
    if uploaded_kmz is not None:
        file_bytes = uploaded_kmz.read()
        filename = uploaded_kmz.name
else:
    # Usar carpeta 'dir' dentro del repo
    if not repo_dir.exists():
        st.sidebar.warning(
            "Crea una carpeta llamada **'dir'** en la raíz del repositorio "
            "y coloca ahí tus archivos .kmz/.kml."
        )
    else:
        if not repo_files:
            st.sidebar.warning(
                "No se encontraron archivos .kmz/.kml en la carpeta 'dir'."
            )
        else:
            archivo_sel = st.sidebar.selectbox(
                "Archivo del repositorio",
                repo_files,
                index=0,
            )
            if archivo_sel:
                ruta = repo_dir / archivo_sel
                with ruta.open("rb") as f:
                    file_bytes = f.read()
                filename = archivo_sel

# --------------------------------------------------------- #

if file_bytes is not None and filename is not None:
    try:
        df = cargar_kmz_o_kml(file_bytes, filename)
    except Exception as e:
        st.error(f"No se pudo leer el KMZ/KML: {e}")
        st.stop()

    if df.empty:
        st.warning("No se encontraron polígonos en el archivo.")
        st.stop()

    st.success(f"Se cargaron {len(df)} secciones de {df['district'].nunique()} distritos.")

    # --- FILTROS: distritos tipo "check" (multiselect) + secciones del subconjunto ---

    st.sidebar.header("Filtros")

    distritos = sorted(df["district"].unique())

    dist_sel = st.sidebar.multiselect(
        "Distritos",
        options=distritos,
        default=distritos  # todos marcados al inicio
    )

    # Si no seleccionó nada, usamos todos para no dejar vacío
    if not dist_sel:
        df_base = df
    else:
        df_base = df[df["district"].isin(dist_sel)]

    def sort_key(x):
        try:
            return (0, int(x))
        except Exception:
            return (1, x)

    secciones_vals = sorted(df_base["section"].unique().tolist(), key=sort_key)

    sec_sel = st.sidebar.multiselect(
        "Secciones (opcional)",
        options=secciones_vals,
        default=[]
    )

    show_labels = st.sidebar.checkbox(
        "Mostrar número de sección en el mapa",
        value=True
    )

    if sec_sel:
        df_filtrado = df_base[df_base["section"].isin(sec_sel)]
    else:
        df_filtrado = df_base

    if df_filtrado.empty:
        st.warning("No hay secciones con esos filtros.")
        st.stop()

    df_filtrado = df_filtrado.copy()

    # Colores: usar los del KMZ si existen, si no, generarlos por distrito
    if df_filtrado["color"].isna().all():
        colores_por_distrito = {}
        for d in df_filtrado["district"].unique():
            random.seed(hash(d) & 0xFFFF)
            r = random.randint(80, 255)
            g = random.randint(80, 255)
            b = random.randint(80, 255)
            colores_por_distrito[d] = [r, g, b, 140]
        df_filtrado["color_vis"] = df_filtrado["district"].map(colores_por_distrito)
    else:
        colores_por_distrito = {}
        for d in df_filtrado["district"].unique():
            random.seed(hash(d) & 0xFFFF)
            r = random.randint(80, 255)
            g = random.randint(80, 255)
            b = random.randint(80, 255)
            colores_por_distrito[d] = [r, g, b, 140]

        def pick_color(row):
            if isinstance(row["color"], list):
                return row["color"]
            return colores_por_distrito[row["district"]]

        df_filtrado["color_vis"] = df_filtrado.apply(pick_color, axis=1)

    # Vista inicial
    view_state = pdk.ViewState(
        longitude=float(df_filtrado["centroid_lon"].mean()),
        latitude=float(df_filtrado["centroid_lat"].mean()),
        zoom=11,
        pitch=0,
    )

    # Capa de polígonos
    polygon_layer = pdk.Layer(
        "PolygonLayer",
        df_filtrado,
        get_polygon="polygon",
        get_fill_color="color_vis",
        get_line_color=[80, 80, 80],
        get_line_width=30,
        pickable=True,
        auto_highlight=True,
    )

    layers = [polygon_layer]

    # Capa de texto opcional
    if show_labels:
        text_layer = pdk.Layer(
            "TextLayer",
            df_filtrado,
            get_position="[centroid_lon, centroid_lat]",
            get_text="section",
            get_size=12,
            get_color=[0, 0, 0, 255],
            get_alignment_baseline="'center'",
        )
        layers.append(text_layer)

    tooltip = {
        "html": "<b>Distrito:</b> {district}<br/>"
                "<b>Sección:</b> {section}",
        "style": {"backgroundColor": "white", "color": "black"}
    }

    deck = pdk.Deck(
        layers=layers,
        initial_view_state=view_state,
        tooltip=tooltip,
        map_style="light"
    )

    st.subheader("Mapa interactivo")
    st.pydeck_chart(deck, use_container_width=True)

else:
    st.info("Selecciona un archivo del repo o súbelo para comenzar.")
