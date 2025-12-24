import os
import requests
from ..config import Settings


class LLMTool:
    """
    Supports:
      - openai_compat (existing)
      - gemini (Google Generative Language API)
    """

    def __init__(self, settings: Settings):
        self.settings = settings

    def generate_text(self, prompt: str) -> str:
        provider = self.settings.llm_provider

        if provider == "openai_compat":
            base = os.getenv("LLM_BASE_URL", "https://api.openai.com").rstrip("/")
            url = f"{base}/v1/chat/completions"
            headers = {"Authorization": f"Bearer {self.settings.llm_api_key}"}
            payload = {
                "model": self.settings.llm_model,
                "messages": [
                    {"role": "system", "content": "You are a helpful manufacturing quality assistant."},
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0.2,
            }
            r = requests.post(url, json=payload, headers=headers, timeout=120)
            r.raise_for_status()
            data = r.json()
            return data["choices"][0]["message"]["content"].strip()

        if provider == "gemini":
            base = os.getenv("LLM_BASE_URL", "https://generativelanguage.googleapis.com").rstrip("/")
            model = self.settings.llm_model or "gemini-2.5-flash"
            key = self.settings.llm_api_key

            url = f"{base}/v1beta/models/{model}:generateContent?key={key}"
            payload = {
                "systemInstruction": {
                    "parts": [{"text": "You are a helpful manufacturing quality assistant."}]
                },
                "contents": [
                    {"role": "user", "parts": [{"text": prompt}]}
                ],
                "generationConfig": {"temperature": 0.2},
            }

            r = requests.post(url, json=payload, timeout=120)
            if not r.ok:
                raise RuntimeError(f"Gemini generateContent failed: {r.status_code} {r.text}")

            data = r.json()
            # pick first candidate text
            candidates = data.get("candidates", [])
            if not candidates:
                return ""
            parts = candidates[0].get("content", {}).get("parts", [])
            text = "".join([p.get("text", "") for p in parts]).strip()
            return text

        raise RuntimeError(f"Unsupported LLM_PROVIDER={provider}")

    def caption_image(self, *, image_bytes: bytes, mime_type: str, context: str = "") -> str:
        """
        Uses Gemini multimodal to generate a stable-ish caption for embedding.
        We keep temperature = 0 for repeatability.
        """
        provider = self.settings.llm_provider
        if provider != "gemini":
            # For now: captions require Gemini (multimodal).
            return ""

        import base64

        base = os.getenv("LLM_BASE_URL", "https://generativelanguage.googleapis.com").rstrip("/")
        model = self.settings.llm_model or "gemini-2.5-flash"
        key = self.settings.llm_api_key
        url = f"{base}/v1beta/models/{model}:generateContent?key={key}"

        b64 = base64.b64encode(image_bytes).decode("utf-8")

        prompt = (
            "ROLE: Manufacturing quality assistant.\n"
            "TASK: Create a RETRIEVAL CAPTION for this image.\n\n"
            "OUTPUT FORMAT (strict):\n"
            "Return EXACTLY 6 lines, each starting with the label:\n"
            "1) PART:\n"
            "2) PROCESS:\n"
            "3) DEFECT:\n"
            "4) LOCATION:\n"
            "5) MEASUREMENT:\n"
            "6) EVIDENCE:\n\n"
            "RULES:\n"
            "- Be factual. Do NOT guess or invent.\n"
            "- If unknown/unclear, write 'unclear'.\n"
            "- Keep each line <= 18 words.\n"
            "- Use manufacturing vocabulary when applicable.\n\n"
            "GUIDANCE:\n"
            "PART: component name if visible (shaft, plate, bracket, bore, hole, thread, weld bead, surface).\n"
            "PROCESS: machining/operation clues (turning, milling, drilling, tapping, welding, grinding, heat treat, coating, assembly).\n"
            "DEFECT: pick the best match if visible: burr, scratch, dent, crack, chip, porosity, lack of fusion, burn mark,\n"
            "       rust, corrosion, pitting, deformation, misalignment, wrong hole, thread damage, tool mark, chatter,\n"
            "       surface roughness, discoloration, bent, mismatch, gap, leakage, contamination.\n"
            "LOCATION: where on part (edge, corner, near hole, weld zone, bore ID/OD, face, slot, chamfer) + orientation if obvious.\n"
            "MEASUREMENT: ONLY if text/scale/reading is visible (e.g., 0.12 mm, 151.77, micrometer/vernier/CMM). Else 'unclear'.\n"
            "EVIDENCE: mention what's seen (close-up photo, annotation, arrow, scale, gauge, marking, handwriting).\n"
        )
        if context.strip():
            prompt += f"\nCONTEXT (use only if relevant; do not copy blindly):\n{context.strip()}\n"


        payload = {
            "systemInstruction": {"parts": [{"text": "You are a helpful manufacturing quality assistant."}]},
            "contents": [
                {
                    "role": "user",
                    "parts": [
                        {"text": prompt},
                        {"inlineData": {"mimeType": mime_type or "image/jpeg", "data": b64}},
                    ],
                }
            ],
            "generationConfig": {"temperature": 0.0},
        }

        r = requests.post(url, json=payload, timeout=120)
        if not r.ok:
            return ""

        data = r.json()
        candidates = data.get("candidates", [])
        if not candidates:
            return ""
        parts = candidates[0].get("content", {}).get("parts", [])
        text = "".join([p.get("text", "") for p in parts]).strip()
        return text
