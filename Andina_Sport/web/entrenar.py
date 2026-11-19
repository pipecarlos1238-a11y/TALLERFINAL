"""Calcula el benchmark temporal y exporta la afinidad de la tienda con Spark."""
import gzip
import hashlib
import json
import math
import time
from datetime import datetime,timezone
from pathlib import Path
from pyspark.sql import SparkSession, Window, functions as F
from pyspark import StorageLevel

OUT=Path("web/modelo")
s=SparkSession.builder.appName("AndinaAmazon").config("spark.sql.shuffle.partitions",12).getOrCreate()
s.sparkContext.setLogLevel("ERROR")
started=time.perf_counter()
def save(name,value):
    (OUT/name).write_text(json.dumps(value,ensure_ascii=False,separators=(",",":")),encoding="utf8")
def records(frame):
    return [r.asDict() for r in frame.collect()]
def utc(ts): return datetime.fromtimestamp(ts/1000,timezone.utc).isoformat()
store=json.loads((OUT/"seleccion.json").read_text())
src=Path("work/amazon/resenas.tsv")
raw=s.read.option("sep","\t").schema("ts long, cuenta string, asin string, estrellas int, verificada int").csv(str(src))
clean=raw.dropna().where("estrellas between 1 and 5 and ts>0 and verificada in (0,1)").dropDuplicates().withColumn("u",F.substring(F.sha2("cuenta",256),1,24)).drop("cuenta").cache()
n=clean.count()
cut=int(clean.approxQuantile("ts",[.90],0.0001)[0])
stats={"leidas":raw.count(),"limpias":n,"usuarios":clean.select("u").distinct().count(),"productos":clean.select("asin").distinct().count(),"verificadas":clean.agg(F.sum("verificada")).first()[0],"corte_ms":cut,"corte_utc":utc(cut),"entrenamiento":clean.where(F.col("ts")<cut).count(),"estrellas":{str(r.estrellas):r.n for r in clean.groupBy("estrellas").agg(F.count("*").alias("n")).collect()},"sha256_tsv":hashlib.sha256(src.read_bytes()).hexdigest()}
timebounds=clean.agg(F.min("ts"),F.max("ts")).first();stats["desde"]=utc(timebounds[0]);stats["hasta"]=utc(timebounds[1])
stats["prueba"]=n-stats["entrenamiento"]
pos=clean.where(F.col("ts")<cut).where("estrellas>=4")
top=pos.groupBy("asin").count().orderBy(F.desc("count"),"asin").limit(1200).select("asin")
anclas=json.loads((OUT/"seleccion_experimento.json").read_text())
allowed=top.unionByName(s.createDataFrame([(x,) for x in anclas],["asin"])).distinct()
train=pos.join(F.broadcast(allowed),"asin").select("u","asin").distinct().cache()
stats["aristas"]=train.count();stats["catalogo_modelo"]=train.select("asin").distinct().count()
target=clean.where(F.col("ts")>=cut).where("estrellas>=4").select("u","asin").distinct().join(allowed,"asin").join(train,["u","asin"],"left_anti").join(train.select("u").distinct(),"u").cache()
testids=target.select("u").distinct().orderBy("u").limit(200).cache()
truth=records(target.join(testids,"u").groupBy("u").agg(F.sort_array(F.collect_set("asin")).alias("real")).orderBy("u"))
stats["usuarios_evaluados"]=len(truth)
edges=train.orderBy("u","asin").collect()
with gzip.open(OUT/"aristas.jsonl.gz","wt",encoding="utf8") as f:
    for u,a in edges: f.write(json.dumps([u,a],separators=(",",":"))+"\n")
counts=train.groupBy("asin").agg(F.count("*").alias("n"))
t=time.perf_counter()
a=train.select("u",F.col("asin").alias("i"))
b=train.select("u",F.col("asin").alias("j"))
pairs=(a.join(b,"u").where("i<>j").groupBy("i","j").count()
    .join(counts.select(F.col("asin").alias("i"),F.col("n").alias("ni")),"i")
    .join(counts.select(F.col("asin").alias("j"),F.col("n").alias("nj")),"j")
    .withColumn("sim",F.col("count")/F.sqrt(F.col("ni")*F.col("nj"))).select("i","j","sim").cache())
rank=(a.join(testids,"u").join(pairs,"i").groupBy("u",F.col("j").alias("asin")).agg(F.sum("sim").alias("score")).join(train,["u","asin"],"left_anti")
      .withColumn("pos",F.row_number().over(Window.partitionBy("u").orderBy(F.desc("score"),"asin"))).where("pos<=5").orderBy("u","pos"))
pred={}
for r in rank.collect():pred.setdefault(r.u,[]).append(r.asin)
stats["spark_lote_s"]=round(time.perf_counter()-t,2)
local={"sim":[[r.i,r.j,r.sim] for r in pairs.where(F.col("i").isin(store)&F.col("j").isin(store)).orderBy("i","j").collect()],
       "popularidad":{r.asin:r.n for r in counts.where(F.col("asin").isin(store)).collect()},
       "vecinos":{}}
for u,item in edges:
    if item in store:local["vecinos"].setdefault(u,[]).append(item)
stats["total_spark_s"]=round(time.perf_counter()-started,2)
save("spark.json",stats);save("cohorte.json",truth);save("predicciones_spark.json",pred);save("pesos_tienda.json",local)
print(json.dumps(stats),flush=True)
s.stop()
