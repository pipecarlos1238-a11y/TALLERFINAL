"""Construye la tienda portátil. La documentación académica se entrega por separado."""
import base64
import collections
import gzip
import json
from pathlib import Path

WEB = Path(__file__).resolve().parent
MODELO = WEB / "modelo"
UI = WEB / "interfaz"
RECURSOS = UI / "recursos"


def leer(path):
    return json.loads(path.read_text(encoding="utf-8"))


def uri(path, mime):
    return "data:" + mime + ";base64," + base64.b64encode(path.read_bytes()).decode("ascii")


def caminos(cohorte, predicciones):
    historial = collections.defaultdict(set)
    personas = collections.defaultdict(set)
    with gzip.open(MODELO / "aristas.jsonl.gz", "rt", encoding="utf-8") as archivo:
        for linea in archivo:
            usuario, producto = json.loads(linea)
            historial[usuario].add(producto)
            personas[producto].add(usuario)
    explicaciones = {}
    for perfil in cohorte:
        usuario = perfil["u"]
        ranking = predicciones.get(usuario, [])
        if not ranking:
            continue
        vecinos = collections.Counter(v for p in historial[usuario] for v in personas[p] if v != usuario)
        top = sorted(vecinos, key=lambda v: (-vecinos[v], v))[:30]
        candidato = next((p for p in ranking if p in perfil["real"]), ranking[0])
        vecino = next(v for v in top if candidato in historial[v])
        explicaciones[usuario] = {
            "compartido": min(historial[usuario] & historial[vecino]),
            "vecino": vecino, "candidato": candidato,
        }
    return explicaciones


def compilar():
    catalogo = leer(RECURSOS / "catalogo.json")
    for p in catalogo:
        p["img"] = uri(RECURSOS / (p["id"] + ".webp"), "image/webp")
    nombres = {"stats": "spark", "comparacion": "comparacion", "pesos": "pesos_tienda",
               "spark": "predicciones_spark", "grafo": "predicciones_grafo", "cohorte": "cohorte"}
    datos = {k: leer(MODELO / (v + ".json")) for k, v in nombres.items()}
    datos.update(catalogo=catalogo, titulos=leer(RECURSOS / "titulos.json"), prueba=leer(WEB / "evidencia/ejecucion.json"))
    datos["caminos"] = caminos(datos["cohorte"], datos["grafo"])
    datos["perfilEjemplo"] = next((r["u"] for r in datos["cohorte"] if set(r["real"]) & set(datos["grafo"].get(r["u"], []))), datos["cohorte"][0]["u"])
    recursos_css = []
    for nombre, archivo, estilo, peso in (("AndinaTexto", "texto", "normal", "100 900"), ("AndinaTitulo", "titulo", "normal", "700")):
        fuente = uri(RECURSOS / (archivo + ".woff2"), "font/woff2")
        recursos_css.append(f'@font-face{{font-family:{nombre};font-style:{estilo};font-weight:{peso};font-display:swap;src:url("{fuente}") format("woff2")}}')
    fotos = {"heroimg": "pista-ancha", "futbolimg": "futbol", "gimnasioimg": "entrenamiento"}
    recursos_css.append(":root{" + "".join(f'--{k}:url("{uri(RECURSOS / (v + ".webp"), "image/webp")}");' for k, v in fotos.items()) + "}")
    css = (UI / "estilo.css").read_text(encoding="utf-8").replace("/*__FONTS__*/", "\n".join(recursos_css))
    js = "\n".join((UI / f).read_text(encoding="utf-8") for f in ("tienda.js", "lab.js", "acciones.js"))
    plantilla = (UI / "base.html").read_text(encoding="utf-8")
    seguro = json.dumps(datos, ensure_ascii=False, separators=(",", ":")).replace("<", "\\u003c")
    html = plantilla.replace("/*__CSS__*/", css).replace("__DATA__", seguro).replace("/*__JS__*/", js)
    destino = WEB.parent / "Tienda_Andina.html"
    destino.write_text(html, encoding="utf-8")
    print(f"{destino.name}: {len(catalogo)} productos, {destino.stat().st_size:,} bytes.")


if __name__ == "__main__":
    compilar()
