"""Puente local entre la tienda HTML y los servicios del reto 2."""
import hashlib
import json
import logging
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs

import boto3
from botocore.exceptions import ClientError
from cassandra.cluster import Cluster
from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer, TopicPartition
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from grafo_amazon import BENCH, TIENDA, conectar, leer, preparar

TOPIC="andina-web-v1"
BASE=Path(__file__).resolve().parent.parent
IDS=set(leer("seleccion.json"))
COHORTE={r["u"] for r in leer("cohorte.json")}
GRAPH_APPLY="""MERGE (e:EventoAndino {id:$eid}) WITH e WHERE e.proyectado IS NULL
MERGE (v:VisitaAndina {id:$usuario}) SET e.proyectado=true
WITH v UNWIND $lineas AS l MATCH (p:ArticuloAndino {asin:l.id})
MERGE (v)-[r:COMPRO]->(p) SET r.unidades=coalesce(r.unidades,0)+l.cantidad"""
SCHEMA=[
"""CREATE TABLE IF NOT EXISTS andina_web.perfiles (usuario text PRIMARY KEY, ultima_sesion text, ultimo_ms bigint)""",
"""CREATE TABLE IF NOT EXISTS andina_web.eventos_por_sesion (usuario text,sesion text,instante bigint,id text,tipo text,articulos text,particion int,posicion bigint,objeto text,PRIMARY KEY((usuario,sesion),instante,id)) WITH CLUSTERING ORDER BY (instante DESC,id ASC)""",
"""CREATE TABLE IF NOT EXISTS andina_web.recibos (id text PRIMARY KEY,huella text,particion int,posicion bigint,finalizado boolean)"""]
READ_SESSION="SELECT instante,id,tipo,articulos,particion,posicion,objeto FROM andina_web.eventos_por_sesion WHERE usuario=? AND sesion=? LIMIT 200"
def ident(s):
    try:return str(uuid.UUID(str(s)))
    except (ValueError,AttributeError):raise ValueError("Identificador de sesión o de evento inválido.")
def validar(evento):
    if not isinstance(evento,dict):raise ValueError("Se requiere un evento.")
    tipo=evento.get("tipo")
    if tipo not in ("vista","clic","compra"):raise ValueError("Acción desconocida.")
    items=evento.get("lineas")
    if not isinstance(items,list) or not 1<=len(items)<=len(IDS):raise ValueError("Seleccione al menos un artículo.")
    lines=[]; seen=set()
    for r in items:
        if not isinstance(r,dict) or r.get("id") not in IDS or r["id"] in seen:raise ValueError("Artículo desconocido o repetido.")
        n=r.get("cantidad")
        if type(n) is not int or not 1<=n<=20:raise ValueError("Cantidad fuera de rango.")
        seen.add(r["id"]);lines.append({"id":r["id"],"cantidad":n})
    ts=evento.get("ts")
    if type(ts) is not int or not 0<ts<4102444800000:raise ValueError("Fecha inválida.")
    return {"id":ident(evento.get("id")),"usuario":ident(evento.get("usuario")),"sesion":ident(evento.get("sesion")),"ts":ts,"tipo":tipo,"lineas":lines}
class Servicios:
    def __init__(self):
        self.lock=threading.RLock()
        self.ready=False
        self.starting=False
        self.fase="Esperando servicios"
        self.error=""
        self.bucket=os.environ["BUCKET"]
    def start(self):
        with self.lock:
            if self.starting or self.ready:return
            self.starting=True
        try:
            broker=os.getenv("BROKER","localhost:9092")
            self.fase="Kafka"
            admin=KafkaAdminClient(bootstrap_servers=broker,request_timeout_ms=10000)
            try:
                try:admin.create_topics([NewTopic(TOPIC,1,1)])
                except TopicAlreadyExistsError:pass
            finally:admin.close()
            self.prod=KafkaProducer(bootstrap_servers=broker,acks="all")
            self.consumer=KafkaConsumer(bootstrap_servers=broker,enable_auto_commit=False,max_poll_records=1)
            self.fase="Cassandra"
            self.cluster=Cluster([os.getenv("CASSANDRA_HOST","localhost")],protocol_version=4,connect_timeout=6)
            self.db=self.cluster.connect()
            self.db.execute("CREATE KEYSPACE IF NOT EXISTS andina_web WITH replication={'class':'SimpleStrategy','replication_factor':1}")
            for q in SCHEMA:self.db.execute(q)
            self.q={k:self.db.prepare(v) for k,v in {
                "buscar":"SELECT huella,particion,posicion,finalizado FROM andina_web.recibos WHERE id=?",
                "recibo":"INSERT INTO andina_web.recibos(id,huella,particion,posicion,finalizado) VALUES (?,?,?,?,false)",
                "hecho":"UPDATE andina_web.recibos SET finalizado=true WHERE id=?",
                "perfil":"INSERT INTO andina_web.perfiles(usuario,ultima_sesion,ultimo_ms) VALUES (?,?,?) USING TIMESTAMP ?",
                "guardar":"INSERT INTO andina_web.eventos_por_sesion(usuario,sesion,instante,id,tipo,articulos,particion,posicion,objeto) VALUES (?,?,?,?,?,?,?,?,?)",
                "leer":READ_SESSION,"leer_perfil":"SELECT ultima_sesion,ultimo_ms FROM andina_web.perfiles WHERE usuario=?"
            }.items()}
            self.fase="S3"
            self.s3=boto3.client("s3",endpoint_url=os.getenv("S3_ENDPOINT") or None)
            try:self.s3.head_bucket(Bucket=self.bucket)
            except ClientError as err:
                if err.response["ResponseMetadata"]["HTTPStatusCode"]!=404:raise
                reg=os.getenv("AWS_DEFAULT_REGION","us-east-1")
                kwargs={} if reg=="us-east-1" else {"CreateBucketConfiguration":{"LocationConstraint":reg}}
                self.s3.create_bucket(Bucket=self.bucket,**kwargs)
            self.fase="Neo4j · carga inicial de afinidades"
            self.driver=conectar()
            with self.driver.session() as graph:preparar(graph)
            self.ready=True;self.error="";self.fase="Disponible"
        except Exception as err:
            self.error=f"{self.fase}: {type(err).__name__}"
            logging.exception("Fallo al preparar la tienda")
            for name,method in [("driver","close"),("prod","close"),("consumer","close"),("cluster","shutdown")]:
                resource=getattr(self,name,None)
                if resource:
                    try:getattr(resource,method)()
                    except Exception:pass
        finally:self.starting=False
    def eventos(self,u,s):
        rows=self.db.execute(self.q["leer"],(u,s))
        return [dict(id=r.id,ts=r.instante,tipo=r.tipo,lineas=json.loads(r.articulos),offset=r.posicion,particion=r.particion,objeto=r.objeto) for r in rows]
    def registrar(self,event):
        e=validar(event)
        serialized=json.dumps(e,sort_keys=True,separators=(",",":")).encode()
        huella=hashlib.sha256(serialized).hexdigest()
        with self.lock:
            recibo=self.db.execute(self.q["buscar"],(e["id"],)).one()
            dia=datetime.fromtimestamp(e["ts"]/1000,timezone.utc).date()
            key=f"web/eventos/fecha={dia}/{e['id']}.json"
            if recibo:
                if recibo.huella!=huella:raise ValueError("El evento ya existe con otros datos.")
                part,offset=recibo.particion,recibo.posicion
            else:
                ack=self.prod.send(TOPIC,key=e["usuario"].encode(),value=serialized).get(timeout=12)
                part,offset=ack.partition,ack.offset
                self.db.execute(self.q["recibo"],(e["id"],huella,part,offset))
            result={"id":e["id"],"offset":offset,"particion":part,"objeto":key}
            if recibo and recibo.finalizado:return result
            tp=TopicPartition(TOPIC,part)
            self.consumer.assign([tp]);self.consumer.seek(tp,offset)
            deadline=time.monotonic()+12
            while time.monotonic()<deadline:
                records=self.consumer.poll(timeout_ms=650,max_records=1).get(tp,[])
                if records:break
            else:raise RuntimeError("Kafka no devolvió el evento a tiempo.")
            msg=records[0]
            if msg.offset!=offset or msg.value!=serialized:raise RuntimeError("Divergencia al releer la posición de Kafka.")
            e=json.loads(msg.value)
            self.s3.put_object(Bucket=self.bucket,Key=key,Body=msg.value,ContentType="application/json")
            self.db.execute(self.q["perfil"],(e["usuario"],e["sesion"],e["ts"],e["ts"]*1000))
            self.db.execute(self.q["guardar"],(e["usuario"],e["sesion"],e["ts"],e["id"],e["tipo"],json.dumps(e["lineas"]),part,offset,key))
            if e["tipo"]=="compra":
                with self.driver.session() as graph:
                    graph.run(GRAPH_APPLY,eid=e["id"],usuario=e["usuario"],lineas=e["lineas"]).consume()
            self.db.execute(self.q["hecho"],(e["id"],))
            return result
    def grafo_tienda(self,u,s):
        rows=self.eventos(u,s)
        weights={};bought=set()
        for e in rows:
            for line in e["lineas"]:
                pid=line["id"]
                weights[pid]=weights.get(pid,0)+{"vista":1,"clic":2,"compra":5}[e["tipo"]]*line["cantidad"]
        with self.driver.session() as graph:
            bought={r["id"] for r in graph.run("MATCH (:VisitaAndina {id:$u})-[:COMPRO]->(p:ArticuloAndino) RETURN p.asin AS id",u=u)}
            rows=list(graph.run(TIENDA,senales=[{"id":p,"peso":v} for p,v in weights.items()],disponibles=sorted(IDS-bought)))
        return {"ranking":[dict(r) for r in rows],"comprados":sorted(bought)}
    def archivo(self,u,s):
        rows=self.eventos(u,s)
        if not rows:return {"vacio":True}
        key=rows[0]["objeto"];head=self.s3.head_object(Bucket=self.bucket,Key=key)
        obj=self.s3.get_object(Bucket=self.bucket,Key=key)
        return {"bucket":self.bucket,"clave":key,"bytes":head["ContentLength"],"contenido":json.loads(obj["Body"].read())}
service=Servicios()
class Tienda(BaseHTTPRequestHandler):
    def enviar(self,status,body,ctype="application/json; charset=utf-8"):
        if not isinstance(body,bytes):body=json.dumps(body,ensure_ascii=False).encode()
        self.send_response(status);self.send_header("Content-Type",ctype);self.send_header("Cache-Control","no-store")
        self.send_header("X-Content-Type-Options","nosniff");self.send_header("Content-Length",str(len(body)));self.end_headers();self.wfile.write(body)
    def despachar(self,post):
        url=urlparse(self.path);path=url.path;args=parse_qs(url.query)
        if not post and path in ("/","/index.html","/Tienda_Andina.html"):
            return self.enviar(200,(BASE/"Tienda_Andina.html").read_bytes(),"text/html; charset=utf-8")
        if path=="/api/estado" and not post:
            return self.enviar(200,{"aplicacion":"andina-web","listo":service.ready,"etapa":service.fase,"error":service.error,"tema":TOPIC})
        if post and self.headers.get("Origin") and urlparse(self.headers["Origin"]).netloc!=self.headers.get("Host"):
            return self.enviar(403,{"error":"Se requiere el origen local de la tienda."})
        if path=="/api/reintentar" and post:
            threading.Thread(target=service.start,daemon=True).start()
            return self.enviar(202,{"ok":True})
        if not path.startswith("/api/"):return self.enviar(404,{"error":"Página no encontrada."})
        if not service.ready:return self.enviar(503,{"error":"Servicios en preparación: "+service.fase})
        if post and path=="/api/eventos":
            size=int(self.headers.get("Content-Length","0"))
            if size<=0 or size>16384 or self.headers.get_content_type()!="application/json":raise ValueError("Envíe un evento JSON válido.")
            return self.enviar(200,service.registrar(json.loads(self.rfile.read(size))))
        if not post and path=="/api/cohorte":
            u=args.get("u",[""])[0]
            if u not in COHORTE:raise ValueError("Perfil fuera de la cohorte.")
            with service.driver.session() as graph: rows=[r.data() for r in graph.run(BENCH,usuario=u)]
            return self.enviar(200,{"ranking":rows})
        if not post and path in ("/api/sesion","/api/archivo","/api/grafo"):
            u=ident(args.get("usuario",[""])[0]);s=ident(args.get("sesion",[""])[0])
            if path=="/api/sesion":
                p=service.db.execute(service.q["leer_perfil"],(u,)).one()
                return self.enviar(200,{"eventos":service.eventos(u,s),"perfil":({"ultima_sesion":p.ultima_sesion,"ultimo_ms":p.ultimo_ms} if p else None)})
            if path=="/api/archivo":return self.enviar(200,service.archivo(u,s))
            return self.enviar(200,service.grafo_tienda(u,s))
        return self.enviar(404,{"error":"Consulta desconocida."})
    def atender(self,post):
        try:self.despachar(post)
        except (ValueError,KeyError,TypeError) as err:self.enviar(400,{"error":str(err)})
        except Exception as err:
            logging.exception("No se completó la operación")
            self.enviar(503,{"error":"El servicio no pudo confirmar la operación. Puede reintentar el mismo evento.","detalle":type(err).__name__})
    def do_GET(self):self.atender(False)
    def do_POST(self):self.atender(True)
if __name__=="__main__":
    threading.Thread(target=service.start,daemon=True).start()
    print("Tienda Andina: http://localhost:8080",flush=True)
    ThreadingHTTPServer(("0.0.0.0",8080),Tienda).serve_forever()
