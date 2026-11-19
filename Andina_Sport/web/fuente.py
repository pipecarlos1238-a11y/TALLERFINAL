"""Descarga secuencial de reseñas. Conserva solo los campos necesarios del fichero oficial."""
import csv
import gzip
import json
import os
from pathlib import Path
from urllib.request import urlopen

URL = "https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/review_categories/Sports_and_Outdoors.jsonl.gz"
DESTINO = Path("work/amazon/resenas.tsv")
def descargar():
    DESTINO.parent.mkdir(parents=True, exist_ok=True)
    temporal = DESTINO.with_suffix(".tmp")
    total = 0
    with urlopen(URL, timeout=150) as respuesta, gzip.GzipFile(fileobj=respuesta) as origen, temporal.open("w", newline="",encoding="utf8") as archivo:
        tabla = csv.writer(archivo, delimiter="\t", lineterminator="\n")
        for linea in origen:
            r = json.loads(linea)
            if not (r.get("user_id") and r.get("parent_asin") and r.get("timestamp") and r.get("rating") in (1,2,3,4,5)):
                continue
            tabla.writerow([r["timestamp"],r["user_id"],r["parent_asin"],int(r["rating"]),int(r["verified_purchase"])])
            total += 1
            if total == 500_000:
                break
    if total != 500_000:
        raise RuntimeError(f"Descarga incompleta: llegaron {total} reseñas.")
    os.replace(temporal, DESTINO)
    print(f"{total} reseñas válidas en {DESTINO}")
if __name__ == "__main__":
    descargar()
