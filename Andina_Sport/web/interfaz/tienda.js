"use strict";
const D=JSON.parse(document.getElementById("dataset").textContent);
const products=D.catalogo, byID=new Map(products.map(p=>[p.id,p]));
const money=new Intl.NumberFormat("es-CO",{style:"currency",currency:"COP",maximumFractionDigits:0});
const num=n=>Number(n).toLocaleString("es-CO");
const pct=n=>(100*n).toLocaleString("es-CO",{maximumFractionDigits:2})+" %";
const fmtDate=t=>new Date(t).toLocaleString("es-CO",{dateStyle:"short",timeStyle:"short"});
const $=s=>document.querySelector(s);
const esc=v=>String(v??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const ICON={
 pin:'<path d="M19 10c0 5-7 11-7 11S5 15 5 10a7 7 0 1 1 14 0Z"/><circle cx="12" cy="10" r="2.3"/>',
 bag:'<path d="M5 8h14l1.2 13H3.8L5 8Z"/><path d="M8.5 9V6a3.5 3.5 0 0 1 7 0v3"/>',
 search:'<circle cx="10.5" cy="10.5" r="6.3"/><path d="m15.3 15.3 4.9 5"/>',
 heart:'<path d="M20.6 5.4a5 5 0 0 0-7.1 0L12 6.9l-1.5-1.5a5 5 0 0 0-7.1 7.1L12 21l8.6-8.5a5 5 0 0 0 0-7.1Z"/>',
 x:'<path d="m6 6 12 12M18 6 6 18"/>',plus:'<path d="M12 5v14M5 12h14"/>',
 minus:'<path d="M5 12h14"/>',trash:'<path d="M3 6h18M9 6V3h6v3M6 6l1 14h10l1-14"/>',
 mountain:'<path d="m2 19 7-12 4 7 3-5 6 10H2Z"/><path d="m7 10 2 2 1-2"/>'
};
function icons(scope=document){for(const el of scope.querySelectorAll("i[data-i]:empty")){el.setAttribute("aria-hidden","true");el.innerHTML='<svg viewBox="0 0 24 24">'+(ICON[el.dataset.i]||"")+'</svg>';}}
const ic=n=>'<i data-i="'+n+'"></i>';
const uid=()=>typeof crypto.randomUUID==="function"?crypto.randomUUID():"10000000-1000-4000-8000-100000000000".replace(/[018]/g,c=>(+c^crypto.getRandomValues(new Uint8Array(1))[0]&15>>+c/4).toString(16));
const key="andina.deporte.v1";
const deportes=["Fútbol","Baloncesto","Entrenamiento","Running","Natación","Ciclismo"];
let S;try{S=JSON.parse(localStorage.getItem(key)||"null")}catch{}
if(!S||!Array.isArray(S.eventos)||!S.usuario)S={usuario:uid(),bolsa:{},favoritos:[],comprados:[],eventos:[]};
let sid;try{sid=sessionStorage.getItem(key);if(!sid){sid=uid();sessionStorage.setItem(key,sid)}}catch{sid=uid()}
let filter="Todo",onlySaved=false,shown=8,sort="seleccion",search="",engine="Spark",tab="sesion",stage="Kafka",report="volumen",traceExample=false,profile=D.perfilEjemplo,cutStars=4,graphNote="",liveRank=null,srv=null,online=false,liveRows=null,liveProfile=null,raw=null,inflightSync=null,fetchEpoch=0,pollId=0;
let toastTimer=null;
const localHost=location.protocol!=="file:"&&["localhost","127.0.0.1","[::1]"].includes(location.hostname);
function toast(msg){$("#toast").textContent=msg;clearTimeout(toastTimer);toastTimer=setTimeout(()=>{$("#toast").textContent=""},2700)}
function save(){try{localStorage.setItem(key,JSON.stringify(S))}catch{toast("El navegador conservará los cambios durante esta visita.")}}
function pName(id){return byID.get(id)?.nombre||id}
function pInfo(id){return byID.get(id)?.nombre||D.titulos[id]||id}
function bagCount(){return Object.values(S.bolsa).reduce((a,b)=>a+b,0)}
function route(){return ["para-ti","laboratorio"].includes(location.hash.slice(1))?location.hash.slice(1):"tienda"}
function navigate(target){if(route()!==target){location.hash=target}else{renderPage();window.scrollTo({top:0,behavior:"auto"})}}

let scrollPendiente = null;
function verSeccion(id, enfocar = false) {
  scrollPendiente = {id, enfocar};
  if (route() !== "tienda") {
    location.hash = "tienda";
    return;
  }
  renderPage();
  desplazar();
}
function desplazar() {
  if (!scrollPendiente || route() !== "tienda") return;
  const {id, enfocar} = scrollPendiente;
  scrollPendiente = null;
  requestAnimationFrame(() => {
    document.getElementById(id).scrollIntoView({block:"start"});
    if (enfocar) $("#buscar").focus({preventScroll:true});
  });
}

function updateHeader(){$("#bolsaN").textContent=bagCount();$(".bag").setAttribute("aria-label","Abrir bolsa, "+bagCount()+" unidades");}
function renderPage(scroll=false){
 const active=route();
 for(const s of document.querySelectorAll(".page"))s.hidden=s.id!=="page-"+active;
 for(const a of document.querySelectorAll("[data-page]")){if(a.dataset.page===active)a.setAttribute("aria-current","page");else a.removeAttribute("aria-current")}
 updateHeader();
 if(active==="tienda")renderCatalog();
 if(active==="para-ti"){renderRecs();if(engine==="Neo4j")refreshGraph()}
 if(active==="laboratorio")renderLab();
 icons();
 if(scroll)window.scrollTo({top:0,behavior:"auto"})
}
function card(p,reason=""){
 const fav=S.favoritos.includes(p.id);
 return '<article class="card"><div class="tile"><button class="picButton" data-show="'+p.id+'" aria-label="Ver '+esc(p.nombre)+'"><img loading="lazy" alt="'+esc(p.nombre)+'" src="'+p.img+'"></button><button class="heart" data-fav="'+p.id+'" aria-pressed="'+fav+'" aria-label="'+(fav?"Quitar de favoritos":"Guardar en favoritos")+': '+esc(p.nombre)+'">'+ic("heart")+'</button></div><div class="cardMeta"><span class="brand">'+esc(p.marca)+'</span><span class="stars" title="'+p.cantidad+' reseñas en la muestra"><b>★</b> '+num(p.rating)+'</span></div><button class="cardTitle" data-show="'+p.id+'">'+esc(p.nombre)+'</button><div class="cardBottom"><span class="price">'+money.format(p.precio)+'</span><button class="quick" data-add="'+p.id+'" title="Añadir a la bolsa" aria-label="Añadir '+esc(p.nombre)+' a la bolsa">'+ic("plus")+'</button></div>'+(reason?'<div class="reason">'+esc(reason)+'</div>':'')+'</article>';
}
function renderCatalog(){
 const cats=["Todo",...deportes];
 $("#catalogTitle").textContent=onlySaved?"Tus favoritos":filter==="Todo"?"Equipamiento deportivo":filter;
 $("#categorias").innerHTML=cats.map(c=>'<button class="chip" data-cat="'+c+'" aria-pressed="'+(filter===c)+'">'+c+'</button>').join("");
 const norm=s=>s.toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g,"");
 let list=products.filter(p=>(!onlySaved||S.favoritos.includes(p.id))&&(filter==="Todo"||p.categoria===filter)&&norm(p.nombre+" "+p.marca+" "+p.categoria).includes(norm(search)));
 if(sort==="menor")list.sort((a,b)=>a.precio-b.precio);if(sort==="mayor")list.sort((a,b)=>b.precio-a.precio);
 if(sort==="valoracion")list.sort((a,b)=>b.rating-a.rating||b.cantidad-a.cantidad);
 $("#resultInfo").textContent=list.length+" "+(list.length===1?"producto":"productos")+(onlySaved?" en favoritos":"");
 $("#productos").innerHTML=list.length?list.slice(0,shown).map(p=>card(p)).join(""):'<div class="empty"><h3>'+ (onlySaved?"Aún no hay favoritos en esta selección":"Sin coincidencias")+'</h3><p>'+ (onlySaved?"Toca el corazón de un producto para guardarlo.":"Prueba otra marca, palabra o categoría.")+'</p><button class="outline" data-act="explorar">Ver todo el catálogo</button></div>';
 $("#mas").innerHTML=shown<list.length?'<button class="outline" data-act="mas">Ver los '+(list.length-shown)+' productos restantes</button>':"";
 $("#otrosDeportes").innerHTML=deportes.filter(c=>!["Fútbol","Entrenamiento","Running"].includes(c)).map(c=>'<button data-deporte="'+esc(c)+'">'+esc(c)+'</button>').join("");
 icons($("#page-tienda"));
}
function localEvents(){return S.eventos.filter(e=>e.sesion===sid).slice(-200).reverse()}
function signalStats(){
 const points={};let bought=new Set(S.comprados);
 for(const e of localEvents())for(const l of e.lineas)points[l.id]=(points[l.id]||0)+({vista:1,clic:2,compra:5}[e.tipo])*l.cantidad;
 return {points,bought};
}
function rankItems(){
 const {points,bought}=signalStats();
 const available=products.filter(p=>!bought.has(p.id));
 const scores={};
 if(engine==="Spark"){
  for(const [a,b,sim] of D.pesos.sim)if(points[a])scores[b]=(scores[b]||0)+points[a]*sim;
 }else if(online&&liveRank){
  for(const r of liveRank.ranking)scores[r.id]=r.puntos;
  liveRank.comprados.forEach(id=>bought.add(id));
 }else{
  const peers=Object.entries(D.pesos.vecinos).map(([u,items])=>({u,items,w:items.reduce((n,p)=>n+(points[p]||0),0)})).filter(r=>r.w>0).sort((a,b)=>b.w-a.w||a.u.localeCompare(b.u)).slice(0,30);
  for(const peer of peers)for(const item of peer.items)scores[item]=(scores[item]||0)+peer.w;
 }
 const sorted=available.filter(p=>!bought.has(p.id)).sort((a,b)=>(scores[b.id]||0)-(scores[a.id]||0)||((scores[a.id]||0)>0?a.id.localeCompare(b.id):(D.pesos.popularidad[b.id]||0)-(D.pesos.popularidad[a.id]||0)||a.id.localeCompare(b.id)));
 return sorted.slice(0,5).map(p=>({p,why:scores[p.id]>0?(engine==="Spark"?"Afinidad entre artículos":"Afinidad de reseñadores"):"Popularidad del histórico"}));
}
function renderRecs(){
 document.querySelectorAll("[data-motor]").forEach(b=>b.setAttribute("aria-pressed",String(b.dataset.motor===engine)));
 const rows=rankItems(), hasSignals=localEvents().length>0;
 let msg=hasSignals?(engine==="Spark"?"Spark aporta las similitudes calculadas con reseñas positivas.":"Se puntúan las propuestas de hasta 30 reseñadores con intereses comunes."):"Tu primera selección usa la popularidad histórica.";
 if(rows.some(r=>r.why.includes("Popularidad"))&&hasSignals)msg+=" Los lugares sin coincidencias se completan por popularidad.";
 if(engine==="Neo4j")msg+=online&&liveRank?" Consulta en Neo4j confirmada.":" Se usa la copia del modelo incluida en la página.";
 $("#motorNota").textContent=msg;
 $("#recs").innerHTML=rows.length?rows.map(x=>card(x.p,x.why)).join(""):'<div class="empty">Ya compraste todos los artículos de esta selección.</div>';icons($("#recs"));
}
async function api(url,opts){
 const ctrl=new AbortController(),timer=setTimeout(()=>ctrl.abort(),18000);
 try{const r=await fetch(url,{...opts,cache:"no-store",signal:ctrl.signal});const data=await r.json();if(!r.ok)throw Error(data.error||"No se completó la operación.");return data}finally{clearTimeout(timer)}
}
const params=()=>new URLSearchParams({usuario:S.usuario,sesion:sid});
async function probe(){
 if(!localHost)return;
 try{
  const r=await api("/api/estado");if(r.aplicacion!=="andina-web")return;
  srv=r;online=!!r.listo;if(online){syncEvents();if(engine==="Neo4j")refreshGraph()}else{clearTimeout(pollId);pollId=setTimeout(probe,4500)}
 }catch{if(srv||location.port==="8080"){online=false;if(srv)srv.error="Conexión interrumpida";clearTimeout(pollId);pollId=setTimeout(probe,6000)}}
 if(route()==="laboratorio"&&tab==="sesion")renderLab();
}
function syncEvents(){
 if(!online)return Promise.resolve();
 if(inflightSync)return inflightSync;
 inflightSync=(async()=>{
  try{
   let e;
   while((e=S.eventos.find(row=>row.envio==="pendiente"))){
    const {id,usuario,sesion,ts,tipo,lineas}=e;
    const ack=await api("/api/eventos",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({id,usuario,sesion,ts,tipo,lineas})});
    const saved=S.eventos.find(row=>row.id===id);
    if(saved)Object.assign(saved,{envio:"confirmado",offset:ack.offset,particion:ack.particion,objeto:ack.objeto});
    save();
   }
  }catch{toast("Evento pendiente de confirmación. Puedes reintentar en el laboratorio.")}
 })().finally(()=>{
  inflightSync=null;liveRows=null;raw=null;
  if(route()==="laboratorio"&&tab==="sesion")renderLab();
  if(engine==="Neo4j"&&route()==="para-ti")refreshGraph();
 });
 return inflightSync;
}
async function refreshGraph(){
 if(!online)return;
 const version=++fetchEpoch;
 try{const r=await api("/api/grafo?"+params());if(version===fetchEpoch){liveRank=r;if(route()==="para-ti")renderRecs()}}catch{if(version===fetchEpoch){liveRank=null;if(route()==="para-ti")renderRecs();toast("Neo4j sin respuesta. Se muestra la copia del modelo.")}}
}
function record(tipo,lineas){
 const e={id:uid(),usuario:S.usuario,sesion:sid,ts:Date.now(),tipo,lineas,envio:srv||(localHost&&location.port==="8080")?"pendiente":"local"};
 S.eventos.push(e);
 while(S.eventos.length>1500){const ix=S.eventos.findIndex(x=>x.envio!=="pendiente");if(ix<0)break;S.eventos.splice(ix,1)}
 liveRank=null;liveRows=null;raw=null;save();if(online)syncEvents();
 return e;
}
const mod=$("#modal");
function openModal(title,html){
 $("#modalBody").innerHTML='<div class="modalTop"><h2 id="modalTitle">'+esc(title)+'</h2><button class="close" data-act="cerrar" aria-label="Cerrar">'+ic("x")+'</button></div>'+html;
 mod.setAttribute("aria-labelledby","modalTitle");icons(mod);
 if(!mod.open)mod.showModal();
}
function closeModal(){if(mod.open)mod.close()}
function showProduct(id){
 const p=byID.get(id);if(!p)return;
 record("vista",[{id,cantidad:1}]);
 openModal(p.nombre,'<div class="productModal"><img class="modalPhoto" alt="'+esc(p.nombre)+'" src="'+p.img+'"><div><span class="brand">'+esc(p.marca)+' · '+esc(p.categoria)+'</span><p>'+esc(p.descripcion)+'</p><p class="stars"><b>★</b> '+num(p.rating)+' / 5 · '+num(p.cantidad)+' reseñas en la muestra</p><p class="price">'+money.format(p.precio)+'</p><div class="actions"><button class="solid" data-add="'+p.id+'">Añadir a la bolsa</button><button class="textbtn" data-act="bolsa">Ver mi bolsa</button></div><p class="small">Precio de demostración en pesos colombianos.</p><p id="feedbackProducto" class="small" role="status" aria-live="polite"></p></div></div><details><summary>Ficha de origen</summary><p class="small">'+esc(p.original)+'</p><p class="small">Amazon Reviews 2023 · ASIN padre '+p.id+'</p></details>');
}
function addBag(id){
 if(!byID.has(id))return;if((S.bolsa[id]||0)>=20){toast("Máximo de 20 unidades por artículo.");return}
 S.bolsa[id]=(S.bolsa[id]||0)+1;record("clic",[{id,cantidad:1}]);save();updateHeader();toast("Añadido a tu bolsa");if(mod.open&&$("#bagRows"))showBag();else if(mod.open&&$("#feedbackProducto"))$("#feedbackProducto").textContent="En tu bolsa: "+S.bolsa[id]+" "+(S.bolsa[id]===1?"unidad":"unidades");
}
function showBag(){
 const items=Object.entries(S.bolsa).filter(([id,n])=>byID.has(id)&&n>0);
 if(!items.length)return openModal("Tu bolsa",'<div class="empty"><h3>Tu bolsa está vacía</h3><p>Agrega un artículo para probar el recorrido de una compra.</p><button class="solid" data-act="explorar">Explorar equipo</button></div>');
 const total=items.reduce((t,[id,n])=>t+n*byID.get(id).precio,0);
 openModal("Tu bolsa · "+bagCount(),'<div id="bagRows">'+items.map(([id,n])=>{const p=byID.get(id);return '<div class="bagRow"><img src="'+p.img+'" alt=""><div><strong>'+esc(p.nombre)+'</strong><small>'+money.format(p.precio)+' por unidad</small></div><div class="quantity"><button data-minus="'+id+'" aria-label="Reducir cantidad de '+esc(p.nombre)+'">−</button><span>'+n+'</span><button data-add="'+id+'" aria-label="Aumentar cantidad de '+esc(p.nombre)+'">+</button></div><button class="iconBtn" data-delete="'+id+'" aria-label="Quitar '+esc(p.nombre)+' de la bolsa">'+ic("trash")+'</button></div>'}).join("")+'</div><div class="total"><span>Total en COP</span><strong>'+money.format(total)+'</strong></div><button class="solid full" data-act="comprar">Confirmar pedido de prueba</button><p class="small">Compra de demostración sin cobro ni envío.</p>');
}
function checkout(){
 const lines=Object.entries(S.bolsa).filter(([id,n])=>byID.has(id)&&n>0).map(([id,cantidad])=>({id,cantidad}));if(!lines.length)return;
 const total=lines.reduce((n,l)=>n+byID.get(l.id).precio*l.cantidad,0);
 S.bolsa={};S.comprados=Array.from(new Set([...S.comprados,...lines.map(l=>l.id)]));
 const e=record("compra",lines);save();updateHeader();liveRank=null;
 openModal("Pedido de prueba registrado",'<div class="confirm"><div class="notice">Pedido '+esc(e.id.slice(0,8))+' · '+money.format(total)+'</div><p>'+lines.map(l=>esc(l.cantidad+" × "+pName(l.id))).join("<br>")+'</p><p class="small">'+(srv?"El laboratorio muestra la confirmación de cada servicio.":"Guardado en este navegador.")+'</p><div class="actions"><button class="solid" data-act="misrecs">Ver mis recomendaciones</button><button class="outline" data-act="sesion">Seguir el evento</button></div></div>');
}
