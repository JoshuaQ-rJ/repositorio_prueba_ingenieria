"""Servidor REST ligero para la API interna de Catálogo de DataMart.

Utiliza la librería estándar de Python (http.server) para no requerir dependencias adicionales
y garantizar arranque instantáneo y resiliente en cualquier contenedor Docker.
Endpoints:
- GET /health
- GET /products?limit=50&offset=0
- GET /products/<code_sku>
"""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

from src.api.main import OFFICIAL_CATEGORIES, _SEED_DATA, infer_category_and_metadata


class CatalogApiHandler(BaseHTTPRequestHandler):

    def _send_json(self, status_code: int, data: dict):
        response_bytes = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(response_bytes)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(response_bytes)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        query = parse_qs(parsed.query)

        if path == "/health" or path == "":
            self._send_json(200, {
                "status": "ok",
                "service": "datamart-legacy-catalog-api",
                "categories": OFFICIAL_CATEGORIES,
            })
            return

        if path == "/products":
            limit = int(query.get("limit", [50])[0])
            offset = int(query.get("offset", [0])[0])
            all_items = list(_SEED_DATA.values())
            paginated = all_items[offset : offset + limit]
            self._send_json(200, {
                "total": len(all_items),
                "limit": limit,
                "offset": offset,
                "items": paginated,
            })
            return

        if path.startswith("/products/"):
            code = path.split("/products/")[1].strip().upper()
            if not code:
                self._send_json(400, {"error": "Product code required"})
                return
            product_info = infer_category_and_metadata(code)
            self._send_json(200, product_info)
            return

        self._send_json(404, {"error": "Not Found", "path": path})

    def log_message(self, format, *args):
        # Evitar ruido excesivo en logs de stdout
        pass


def run_server(host: str = "0.0.0.0", port: int = 8000):
    server = HTTPServer((host, port), CatalogApiHandler)
    print(f"[CATALOG-API] DataMart Catalog API running on http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    run_server()
