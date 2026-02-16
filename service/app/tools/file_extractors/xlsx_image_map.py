# service/app/tools/file_extractors/xlsx_image_map.py
from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from typing import Dict, List, Optional, Tuple
import zipfile
import xml.etree.ElementTree as ET


_NS = {
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    "xdr": "http://schemas.openxmlformats.org/drawingml/2006/spreadsheetDrawing",
    "a": "http://schemas.openxmlformats.org/drawingml/2006/main",
    "ws": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "rel": "http://schemas.openxmlformats.org/package/2006/relationships",
}


@dataclass
class XlsxAnchoredImage:
    sheet_name: str
    sheet_file: str
    drawing_file: str
    media_path: str              # e.g. xl/media/image1.png
    media_name: str              # e.g. image1.png
    col0: int                    # 0-based
    row0: int                    # 0-based
    col1: Optional[int] = None   # optional bottom-right anchor
    row1: Optional[int] = None
    rel_id: Optional[str] = None # rId inside drawing rels


def _zip_read(z: zipfile.ZipFile, path: str) -> Optional[bytes]:
    try:
        return z.read(path)
    except Exception:
        return None


def _parse_xml_bytes(b: bytes) -> Optional[ET.Element]:
    try:
        return ET.fromstring(b)
    except Exception:
        return None


def _norm_target(base_dir: str, target: str) -> str:
    # targets are usually relative paths like "../drawings/drawing1.xml"
    t = (target or "").replace("\\", "/")
    if t.startswith("/"):
        t = t.lstrip("/")
    # resolve ../ segments in a simple way
    base_parts = [p for p in base_dir.split("/") if p]
    tgt_parts = [p for p in t.split("/") if p]
    out = base_parts[:]
    for p in tgt_parts:
        if p == "..":
            if out:
                out.pop()
        elif p == ".":
            continue
        else:
            out.append(p)
    return "/".join(out)


def _col_to_a1_col(col_1based: int) -> str:
    n = int(col_1based)
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def a1(col0: int, row0: int) -> str:
    return f"{_col_to_a1_col(col0 + 1)}{row0 + 1}"


def extract_xlsx_anchored_images(data: bytes, *, max_images: int = 25) -> List[XlsxAnchoredImage]:
    """
    Parses XLSX (zip) XML to map images to:
      sheet name + top-left anchor cell (col,row).

    Works even when openpyxl is read_only=True.
    """
    out: List[XlsxAnchoredImage] = []

    with zipfile.ZipFile(BytesIO(data), "r") as z:
        # 1) workbook.xml: list sheets with r:id
        wb_xml = _zip_read(z, "xl/workbook.xml")
        wb_root = _parse_xml_bytes(wb_xml or b"")
        if wb_root is None:
            return out

        # 2) workbook rels: map rId -> target (worksheet file)
        wb_rels_xml = _zip_read(z, "xl/_rels/workbook.xml.rels")
        rel_root = _parse_xml_bytes(wb_rels_xml or b"")
        if rel_root is None:
            return out

        rid_to_target: Dict[str, str] = {}
        for rel in rel_root.findall(".//rel:Relationship", _NS):
            rid = rel.attrib.get("Id", "")
            target = rel.attrib.get("Target", "")
            if not rid or not target:
                continue
            # workbook rel targets are relative to xl/
            rid_to_target[rid] = _norm_target("xl", target)

        # 3) sheet name -> worksheet file
        sheets: List[Tuple[str, str]] = []
        for sh in wb_root.findall(".//ws:sheets/ws:sheet", _NS):
            name = sh.attrib.get("name", "").strip()
            rid = sh.attrib.get(f"{{{_NS['r']}}}id", "").strip()
            if not name or not rid:
                continue
            ws_file = rid_to_target.get(rid, "")
            if ws_file and ws_file.startswith("xl/worksheets/"):
                sheets.append((name, ws_file))

        if not sheets:
            return out

        # helper: load rels of a worksheet and find drawing target
        def _worksheet_drawing_target(ws_file: str) -> Optional[str]:
            # worksheet rels file: xl/worksheets/_rels/sheetN.xml.rels
            base_dir = "/".join(ws_file.split("/")[:-1])
            rels_path = f"{base_dir}/_rels/{ws_file.split('/')[-1]}.rels"
            rels_xml = _zip_read(z, rels_path)
            rels_root = _parse_xml_bytes(rels_xml or b"")
            if rels_root is None:
                return None
            # find relationship of type drawing
            for rel in rels_root.findall(".//rel:Relationship", _NS):
                typ = rel.attrib.get("Type", "")
                target = rel.attrib.get("Target", "")
                if "drawing" in (typ or "").lower() and target:
                    return _norm_target(base_dir, target)
            return None

        # helper: drawing rels map rId -> media path
        def _drawing_media_map(drawing_file: str) -> Dict[str, str]:
            base_dir = "/".join(drawing_file.split("/")[:-1])
            rels_path = f"{base_dir}/_rels/{drawing_file.split('/')[-1]}.rels"
            rels_xml = _zip_read(z, rels_path)
            rels_root = _parse_xml_bytes(rels_xml or b"")
            mp: Dict[str, str] = {}
            if rels_root is None:
                return mp
            for rel in rels_root.findall(".//rel:Relationship", _NS):
                rid = rel.attrib.get("Id", "")
                target = rel.attrib.get("Target", "")
                if not rid or not target:
                    continue
                # target relative to drawing folder
                mp[rid] = _norm_target(base_dir, target)
            return mp

        # helper: parse drawing xml for anchors
        def _parse_drawing_anchors(drawing_file: str) -> List[Tuple[str, int, int, Optional[int], Optional[int]]]:
            """
            Returns list of (embedRid, col0, row0, col1, row1)
            """
            res: List[Tuple[str, int, int, Optional[int], Optional[int]]] = []
            dr_xml = _zip_read(z, drawing_file)
            dr_root = _parse_xml_bytes(dr_xml or b"")
            if dr_root is None:
                return res

            # Look for twoCellAnchor and oneCellAnchor
            anchors = []
            anchors.extend(dr_root.findall(".//xdr:twoCellAnchor", _NS))
            anchors.extend(dr_root.findall(".//xdr:oneCellAnchor", _NS))

            for an in anchors:
                # from
                fr = an.find("./xdr:from", _NS)
                if fr is None:
                    continue
                col_el = fr.find("./xdr:col", _NS)
                row_el = fr.find("./xdr:row", _NS)
                if col_el is None or row_el is None:
                    continue
                try:
                    col0 = int(col_el.text or "0")
                    row0 = int(row_el.text or "0")
                except Exception:
                    continue

                # to (optional for twoCellAnchor)
                col1 = None
                row1 = None
                to = an.find("./xdr:to", _NS)
                if to is not None:
                    c1 = to.find("./xdr:col", _NS)
                    r1 = to.find("./xdr:row", _NS)
                    try:
                        if c1 is not None:
                            col1 = int(c1.text or "0")
                        if r1 is not None:
                            row1 = int(r1.text or "0")
                    except Exception:
                        col1 = None
                        row1 = None

                # picture embed rid: a:blip r:embed
                blip = an.find(".//a:blip", _NS)
                if blip is None:
                    continue
                embed = blip.attrib.get(f"{{{_NS['r']}}}embed", "")
                if not embed:
                    continue

                res.append((embed, col0, row0, col1, row1))

            return res

        # main loop: map per sheet
        for sheet_name, ws_file in sheets:
            drawing_file = _worksheet_drawing_target(ws_file)
            if not drawing_file:
                continue
            media_map = _drawing_media_map(drawing_file)
            anchors = _parse_drawing_anchors(drawing_file)

            for (embed_rid, col0, row0, col1, row1) in anchors:
                media = media_map.get(embed_rid, "")
                if not media:
                    continue
                # normalize to full zip path
                media_path = media if media.startswith("xl/") else _norm_target("xl", media)
                media_name = media_path.split("/")[-1]
                out.append(
                    XlsxAnchoredImage(
                        sheet_name=sheet_name,
                        sheet_file=ws_file,
                        drawing_file=drawing_file,
                        media_path=media_path,
                        media_name=media_name,
                        col0=col0,
                        row0=row0,
                        col1=col1,
                        row1=row1,
                        rel_id=embed_rid,
                    )
                )
                if len(out) >= max_images:
                    return out

    return out


def load_anchored_image_bytes(data: bytes, anchored: List[XlsxAnchoredImage]) -> Dict[str, bytes]:
    """
    Returns media_path -> bytes for provided anchored images.
    """
    out: Dict[str, bytes] = {}
    with zipfile.ZipFile(BytesIO(data), "r") as z:
        for it in anchored:
            b = _zip_read(z, it.media_path)
            if b:
                out[it.media_path] = b
    return out