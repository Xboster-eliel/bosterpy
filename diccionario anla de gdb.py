# Leyendo una GDB

## Septud

# Instalando gdal
# !apt-get -qq update && apt-get -qq install -y gdal-bin python3-gdal >/dev/null
# !pip -q install pandas openpyxl

# =========================
# 0) Preparación en Colab
# =========================
# Instalamos binarios de GDAL y los bindings de Python.
# En Colab suele venir una versión reciente; forzamos compatibilidad.
!apt-get -qq update
!apt-get -qq install -y gdal-bin python3-gdal > /dev/null

!pip install  xlsxwriter



# Monta tu Google Drive si la GDB está allí.
from google.colab import drive
drive.mount('/content/drive')

# =========================
# 1) Importaciones
# =========================
import os
import glob
import pandas as pd
from osgeo import ogr, osr, gdal

import xlsxwriter
from pathlib import Path

## Utilidades

# =========================
# 3) Auxiliares pequeños
# =========================

def srs_to_auth(srs):
    """
    Devuelve un texto corto del sistema de referencia:
    'EPSG:XXXX' si es posible; si no, WKT corto.
    """
    if srs is None:
        return ""
    srs = srs.Clone()
    srs.AutoIdentifyEPSG()
    auth = srs.GetAuthorityName(None)
    code = srs.GetAuthorityCode(None)
    if auth and code:
        return f"{auth}:{code}"
    return srs.ExportToWkt()[:60]  # recorte para no alargar

def try_get_fieldnames(layer):
    """
    Devuelve la lista de campos de una capa OGR.
    Útil para inspeccionar tablas internas como GDB_Items.
    """
    defn = layer.GetLayerDefn()
    return [defn.GetFieldDefn(i).GetName() for i in range(defn.GetFieldCount())]

## 1) Dataframe con la estructura de la gdb ->> Vectores y raster

# -*- coding: utf-8 -*-
# Listado jerárquico de una FileGDB y mapeo correcto de RÁSTER -> Feature Dataset.
# Requiere: GDAL/OGR (OpenFileGDB), pandas.
# Docs clave:
# - OpenFileGDB (vector): LIST_ALL_TABLES para ver GDB_*  [GDAL]  :contentReference[oaicite:1]{index=1}
# - OpenFileGDB (raster) y GetSubDatasets()               [GDAL]  :contentReference[oaicite:2]{index=2}
# - Tablas de sistema GDB_* y relación DatasetInFeatureDataset [Esri] :contentReference[oaicite:3]{index=3}

import os
import pandas as pd
from osgeo import gdal, ogr, osr

# -------- Auxiliares mínimos --------
def srs_to_auth(srs):
    """Devuelve 'EPSG:xxxx' si es posible, o cadena vacía."""
    if srs is None:
        return ""
    srs = srs.Clone()
    try:
        srs.AutoIdentifyEPSG()
    except Exception:
        pass
    auth = srs.GetAuthorityName(None)
    code = srs.GetAuthorityCode(None)
    return f"{auth}:{code}" if auth and code else ""

def try_get_fieldnames(layer):
    """Lista segura de campos de una capa OGR."""
    if layer is None:
        return []
    defn = layer.GetLayerDefn()
    return [defn.GetFieldDefn(i).GetName() for i in range(defn.GetFieldCount())]

# =========================
# 4) Apertura de la GDB
# =========================
GDB_PATH = "/content/drive/Shareddrives/HF_2025_SUSCEPTIBILIDAD_SIG/03_SIG/GDB/Susceptibilidad_Antioquia.gdb"
tema = os.path.basename(GDB_PATH.rstrip("/"))
rows = []

# =========================
# 5-1) Listado jerárquico robusto de vectores
# =========================
ds = gdal.OpenEx(
    GDB_PATH,
    gdal.OF_VECTOR | gdal.OF_READONLY,
    allowed_drivers=["OpenFileGDB","FileGDB"],
    open_options=["LIST_ALL_TABLES=YES"]  # expone GDB_* si se requiere
)
if ds is None:
    raise RuntimeError(f"No pude abrir la GDB: {GDB_PATH}")

root = getattr(ds, "GetRootGroup", lambda: None)()
if root:
    stack = [("", root)]
    while stack:
        path, grp = stack.pop()
        for gname in (grp.GetGroupNames() or []):
            subgrp = grp.OpenGroup(gname)
            new_path = f"{path}/{gname}" if path else gname
            stack.append((new_path, subgrp))
        for lname in (grp.GetVectorLayerNames() or []):
            lyr = grp.OpenVectorLayer(lname)
            rows.append({
                "TipoDato": "V",
                "Tema": tema,
                "Componente": path if path else "[root]",
                "Nombre": lname,
                "GeomType": ogr.GeometryTypeToName(lyr.GetGeomType()),
                "CRS": srs_to_auth(lyr.GetSpatialRef()),
                "Conteo": lyr.GetFeatureCount()
            })
else:
    # Fallback plano
    for i in range(ds.GetLayerCount()):
        lyr = ds.GetLayerByIndex(i)
        name = lyr.GetName()
        comp, capa = name.split("/", 1) if "/" in name else ("[root]", name)
        rows.append({
            "TipoDato": "V",
            "Tema": tema,
            "Componente": comp,
            "Nombre": capa,
            "GeomType": ogr.GeometryTypeToName(lyr.GetGeomType()),
            "CRS": srs_to_auth(lyr.GetSpatialRef()),
            "Conteo": lyr.GetFeatureCount()
        })

# =========================
# 5.b) RÁSTER con componente correcto usando RELACIONES GDB_*
#     (DatasetInFeatureDataset) en vez de inferir por Path
# =========================

# ---- 5.b.1 Leer GDB_Items a dict id->info (UUID -> {Name, Type, Path})
id2item = {}
items = ds.ExecuteSQL("SELECT * FROM GDB_Items")
if items:
    flds = try_get_fieldnames(items)
    ix_uuid = flds.index("UUID") if "UUID" in flds else None
    ix_name = flds.index("Name") if "Name" in flds else None
    ix_type = flds.index("Type") if "Type" in flds else None
    ix_path = flds.index("Path") if "Path" in flds else None
    f = items.GetNextFeature()
    while f:
        uid = f.GetField(ix_uuid) if ix_uuid is not None else None
        nm  = f.GetField(ix_name) if ix_name is not None else None
        tp  = f.GetField(ix_type) if ix_type is not None else None
        pth = f.GetField(ix_path) if ix_path is not None else None
        if uid:
            id2item[uid] = {"Name": nm, "Type": tp, "Path": pth}
        f = items.GetNextFeature()
    ds.ReleaseResultSet(items)

# ---- 5.b.2 Buscar UUID del tipo de relación 'DatasetInFeatureDataset'
uuid_difd = None
rtype = ds.ExecuteSQL("SELECT * FROM GDB_ItemRelationshipTypes")
if rtype:
    flds = try_get_fieldnames(rtype)
    ix_ruuid = flds.index("UUID") if "UUID" in flds else None
    ix_rname = flds.index("Name") if "Name" in flds else None
    f = rtype.GetNextFeature()
    while f:
        nm = f.GetField(ix_rname) if ix_rname is not None else None
        if nm == "DatasetInFeatureDataset":
            uuid_difd = f.GetField(ix_ruuid) if ix_ruuid is not None else None
            break
        f = rtype.GetNextFeature()
    ds.ReleaseResultSet(rtype)

# ---- 5.b.3 Construir mapa NombreRaster -> NombreFeatureDataset a partir de relaciones
name2comp_rel = {}
if uuid_difd:
    rels = ds.ExecuteSQL("SELECT * FROM GDB_ItemRelationships")
    if rels:
        flds = try_get_fieldnames(rels)
        ix_origin = flds.index("OriginID") if "OriginID" in flds else None
        ix_dest   = flds.index("DestID") if "DestID" in flds else None
        ix_rtype  = flds.index("RelationshipType") if "RelationshipType" in flds else None
        f = rels.GetNextFeature()
        while f:
            rt = f.GetField(ix_rtype) if ix_rtype is not None else None
            if rt == uuid_difd:
                parent_id = f.GetField(ix_origin) if ix_origin is not None else None
                child_id  = f.GetField(ix_dest) if ix_dest   is not None else None
                parent = id2item.get(parent_id, {})
                child  = id2item.get(child_id,  {})
                if child and child.get("Type") in ("RasterDataset","MosaicDataset"):
                    child_name  = child.get("Name")
                    parent_name = parent.get("Name") or "[root]"
                    if child_name:
                        name2comp_rel[child_name] = parent_name
            f = rels.GetNextFeature()
        ds.ReleaseResultSet(rels)

# ---- 5.b.4 (opcional) mapa por Path como respaldo
name2comp_path = {}
for uid, it in id2item.items():
    nm = it.get("Name") or ""
    p  = (it.get("Path") or "").strip("/")
    comp = p.split("/",1)[0] if "/" in p else "[root]"
    if nm:
        name2comp_path[nm] = comp

# ---- 5.b.5 Enumerar subdatasets ráster (GDAL ≥3.7) y registrar filas
seen_rasters = set()
rds = gdal.OpenEx(GDB_PATH, gdal.OF_RASTER | gdal.OF_READONLY,
                  allowed_drivers=["OpenFileGDB"])
if rds:
    for sds_name, _ in (rds.GetSubDatasets() or []):
        raw = sds_name.split('":')[-1].lstrip('/')  # 'FD/RAS' o 'RAS'
        if "/" in raw:
            comp, capa = raw.split("/", 1)
        else:
            comp = name2comp_rel.get(raw) or name2comp_path.get(raw, "[root]")
            capa = raw

        try:
            ds_r = gdal.Open(sds_name)
            x, y, bands = ds_r.RasterXSize, ds_r.RasterYSize, ds_r.RasterCount
            srs = osr.SpatialReference(wkt=ds_r.GetProjection())
            srs.AutoIdentifyEPSG()
            crs = f"{srs.GetAuthorityName(None)}:{srs.GetAuthorityCode(None)}" \
                  if srs.GetAuthorityName(None) and srs.GetAuthorityCode(None) else ""
        except Exception:
            x = y = bands = None
            crs = ""

        rows.append({
            "TipoDato": "R",
            "Tema": tema,
            "Componente": comp,
            "Nombre": capa,
            "GeomType": "Raster",
            "CRS": crs,
            "Conteo": None,
            "Ancho_px": x,
            "Alto_px": y,
            "Bandas": bands
        })
        seen_rasters.add(capa)

# ---- 5.b.6 Ráster que no salieron como subdatasets: agregarlos desde GDB_Items
for uid, it in id2item.items():
    if it.get("Type") in ("RasterDataset","MosaicDataset"):
        nm = it.get("Name")
        if not nm or nm in seen_rasters:
            continue
        comp = name2comp_rel.get(nm) or name2comp_path.get(nm, "[root]")
        rows.append({
            "TipoDato": "R",
            "Tema": tema,
            "Componente": comp,
            "Nombre": nm,
            "GeomType": "Raster",
            "CRS": "",
            "Conteo": None
        })

# =========================
# 7) Ordenar como jerarquía
# =========================
df_estructura = (pd.DataFrame(rows)
                 .sort_values(["Componente","Nombre"], kind="stable")
                 .reset_index(drop=True))

df_estructura
