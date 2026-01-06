from __future__ import annotations

import json
from pathlib import Path

from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/drive"]

def main() -> None:
    # Download OAuth client file from Google Cloud Console and save it as credentials.json
    creds_path = Path(__file__).resolve().parent / "credentials.json"
    if not creds_path.exists():
        raise SystemExit(
            f"Missing {creds_path}. Put your OAuth client secrets file there (credentials.json)."
        )

    flow = InstalledAppFlow.from_client_secrets_file(str(creds_path), SCOPES)
    creds = flow.run_local_server(port=0)

    # Print JSON you will copy into Render env var DRIVE_TOKEN_JSON
    # creds.to_json() already returns a JSON string.
    token_json_str = creds.to_json()
    # Validate it prints as JSON cleanly
    json.loads(token_json_str)

    print("\n==== COPY THIS INTO RENDER ENV: DRIVE_TOKEN_JSON ====\n")
    print(token_json_str)
    print("\n====================================================\n")

if __name__ == "__main__":
    main()
