"""Proyecta el catálogo deportivo sobre las afinidades históricas de Spark."""
import json
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

MODELO = Path(__file__).resolve().parent / "modelo"


def proyectar(spark):
    seleccion = json.loads((MODELO / "seleccion.json").read_text(encoding="utf-8"))
    pares = (
        spark.read.text(str(MODELO / "aristas.jsonl.gz"))
        .select(F.from_json("value", "array<string>").alias("par"))
        .select(F.col("par")[0].alias("u"), F.col("par")[1].alias("asin"))
        .where(F.col("asin").isin(seleccion))
        .cache()
    )
    soporte = {r.asin: r["count"] for r in pares.groupBy("asin").count().collect()}
    faltantes = set(seleccion) - set(soporte)
    if faltantes:
        raise ValueError("Sin aristas positivas para " + ", ".join(sorted(faltantes)))
    por_usuario = pares.groupBy("u").agg(F.sort_array(F.collect_set("asin")).alias("items"))
    vecinos = {r.u: r["items"] for r in por_usuario.orderBy("u").collect()}
    izquierdas = pares.select("u", F.col("asin").alias("a"))
    derechas = pares.select("u", F.col("asin").alias("b"))
    comunes = izquierdas.join(derechas, "u").where("a <> b").groupBy("a", "b").count()
    similitud = [[r.a, r.b, r["count"] / (soporte[r.a] * soporte[r.b]) ** 0.5]
                for r in comunes.orderBy("a", "b").collect()]
    pesos = {"sim": similitud, "popularidad": soporte, "vecinos": vecinos}
    (MODELO / "pesos_tienda.json").write_text(json.dumps(pesos, separators=(",", ":")), encoding="utf-8")
    print(f"{len(seleccion)} productos y {len(similitud) // 2} pares de coocurrencia.", flush=True)
    pares.unpersist()


if __name__ == "__main__":
    spark = SparkSession.builder.appName("AndinaSportCatalogo").config("spark.sql.shuffle.partitions", 8).getOrCreate()
    try:
        proyectar(spark)
    finally:
        spark.stop()
