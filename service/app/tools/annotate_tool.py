from __future__ import annotations

from typing import Any, Dict, List, Tuple
from io import BytesIO

from PIL import Image, ImageDraw, ImageFont


class AnnotateTool:
    """
    Draws bounding boxes on an image.
    Boxes are normalized (0..1) coordinates.
    """

    def draw(
        self,
        image_bytes: bytes,
        boxes: List[Dict[str, Any]],
        *,
        out_format: str = "PNG",
    ) -> bytes:
        im = Image.open(BytesIO(image_bytes)).convert("RGB")
        w, h = im.size
        draw = ImageDraw.Draw(im)
        font = ImageFont.load_default()

        for b in boxes or []:
            label = str(b.get("label") or "defect")
            conf = b.get("confidence")
            box = b.get("box") or {}
            x1 = float(box.get("x1", 0.0))
            y1 = float(box.get("y1", 0.0))
            x2 = float(box.get("x2", 0.0))
            y2 = float(box.get("y2", 0.0))

            # clamp
            x1 = max(0.0, min(1.0, x1))
            y1 = max(0.0, min(1.0, y1))
            x2 = max(0.0, min(1.0, x2))
            y2 = max(0.0, min(1.0, y2))

            X1, Y1 = int(x1 * w), int(y1 * h)
            X2, Y2 = int(x2 * w), int(y2 * h)

            # rectangle (red)
            draw.rectangle([X1, Y1, X2, Y2], outline=(255, 0, 0), width=4)

            tag = label
            if conf is not None:
                try:
                    tag = f"{label} ({float(conf):.2f})"
                except Exception:
                    pass

            # label background
            tw, th = draw.textsize(tag, font=font)
            pad = 3
            bx1, by1 = X1, max(0, Y1 - th - 2 * pad)
            bx2, by2 = X1 + tw + 2 * pad, Y1
            draw.rectangle([bx1, by1, bx2, by2], fill=(255, 0, 0))
            draw.text((bx1 + pad, by1 + pad), tag, fill=(255, 255, 255), font=font)

        out = BytesIO()
        im.save(out, format=out_format)
        return out.getvalue()