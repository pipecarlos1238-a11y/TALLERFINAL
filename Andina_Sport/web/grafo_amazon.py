"""Prepara la proyección de Amazon en Neo4j y evalúa el mismo corte usado en Spark."""
import gzip
import json
import math
import os
import time
from hashlib import sha256
from pathlib import Path
from statistics import median
from neo4j import GraphDatabase

DIR=Path(__file__).resolve().parent/"modelo"
BENCH="""
MATCH (u:RevisorAndino {id:$usuario})-[:VALORO]->(p:ArticuloAndino)<-[:VALORO]-(v:RevisorAndino)
WHERE v <> u
WITH u,v,count(DISTINCT p) AS comun ORDER BY comun DESC, v.id LIMIT 30
MATCH (v)-[:VALORO]->(c:ArticuloAndino)
WHERE NOT EXISTS {MATCH (u)-[:VALORO]->(c)}
RETURN c.asin AS id,sum(comun) AS puntos ORDER BY puntos DESC,id LIMIT 5
"""
TIENDA="""
UNWIND $senales AS s
MATCH (p:ArticuloAndino {asin:s.id})<-[:VALORO]-(v:RevisorAndino)
WITH v,sum(s.peso) AS afinidad ORDER BY afinidad DESC,v.id LIMIT 30
MATCH (v)-[:VALORO]->(otro:ArticuloAndino)
WHERE otro.asin IN $disponibles
RETURN otro.asin AS id, sum(afinidad) AS puntos ORDER BY puntos DESC,id LIMIT 5
"""
def conectar():
    return GraphDatabase.driver(os.getenv("NEO4J_URI","bolt://localhost:7687"),auth=("neo4j",os.environ["NEO4J_PASSWORD"]),connection_timeout=5)
def leer(nombre):
    return json.loads((DIR/nombre).read_text(encoding="utf8"))
def preparar(db):
    for label,key in [("RevisorAndino","id"),("ArticuloAndino","asin"),("VersionAndina","huella"),("VisitaAndina","id"),("EventoAndino","id")]:
        db.run(f"CREATE CONSTRAINT IF NOT EXISTS FOR (n:{label}) REQUIRE n.{key} IS UNIQUE").consume()
    path=DIR/"aristas.jsonl.gz"
    huella=sha256(path.read_bytes()).hexdigest()
    if db.run("MATCH (m:VersionAndina {huella:$h}) RETURN m.lista AS ok",h=huella).single():
        return
    q="""UNWIND $filas AS x MERGE (u:RevisorAndino {id:x[0]})
         MERGE (p:ArticuloAndino {asin:x[1]}) MERGE (u)-[:VALORO]->(p)"""
    batch=[]
    with gzip.open(path,"rt",encoding="utf8") as stream:
        for line in stream:
            batch.append(json.loads(line))
            if len(batch)==1600:
                db.run(q,filas=batch).consume()
                batch=[]
        if batch: db.run(q,filas=batch).consume()
    db.run("MERGE (m:VersionAndina {huella:$h}) SET m.lista=true",h=huella).consume()
def evaluar():
    cohort=leer("cohorte.json")
    spark=leer("predicciones_spark.json")
    with conectar() as d,d.session() as db:
        start=time.perf_counter()
        preparar(db)
        carga=time.perf_counter()-start
        graph={};times=[]
        for row in cohort:
            start=time.perf_counter()
            graph[row['u']]=[r['id'] for r in db.run(BENCH,usuario=row['u'])]
            times.append(1000*(time.perf_counter()-start))
    out={"carga_s":round(carga,3),"mediana_ms":round(median(times),2),"p95_ms":round(sorted(times)[math.ceil(.95*len(times))-1],2)}
    for name,pred in [("Spark",spark),("Neo4j",graph)]:
        results=[(len(set(row['real'])&set(pred.get(row['u'],[]))),len(row['real'])) for row in cohort]
        out[name]={"hit":sum(h>0 for h,k in results)/len(results),"recall":sum(h/k for h,k in results)/len(results),"aciertos":sum(h for h,k in results),"usuarios_acierto":sum(h>0 for h,k in results)}
    for name,data in [("comparacion.json",out),("predicciones_grafo.json",graph)]:
        (DIR/name).write_text(json.dumps(data,separators=(",",":")),encoding="utf8")
    print(json.dumps(out))
if __name__=="__main__":evaluar()
