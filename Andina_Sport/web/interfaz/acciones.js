function abrirCatalogo(categoria = "Todo", favoritos = false, enfocar = false) {
  onlySaved = favoritos;
  filter = categoria;
  search = "";
  shown = favoritos ? products.length : 8;
  $("#buscar").value = "";
  closeModal();
  verSeccion("catalogo", enfocar);
}
function openLab(which){closeModal();tab=which;navigate("laboratorio");renderLab()}
document.addEventListener("click",async ev=>{
 const b=ev.target.closest("button,a[data-page]");if(!b)return;
 try{
  if(b.dataset.page){
   if(b.dataset.page==="tienda"){onlySaved=false;filter="Todo";search="";shown=8;$("#buscar").value="";if(route()==="tienda"){renderCatalog();window.scrollTo({top:0,behavior:"auto"})}}
   return;
  }
  if(b.dataset.deporte){abrirCatalogo(b.dataset.deporte);return;}
  if(b.dataset.show)return showProduct(b.dataset.show);
  if(b.dataset.add)return addBag(b.dataset.add);
  if(b.dataset.fav){const id=b.dataset.fav,had=S.favoritos.includes(id);S.favoritos=had?S.favoritos.filter(x=>x!==id):[...S.favoritos,id];if(!had)record("clic",[{id,cantidad:1}]);save();renderPage();return toast(had?"Quitado de favoritos":"Guardado en favoritos");}
  if(b.dataset.minus||b.dataset.delete){const id=b.dataset.minus||b.dataset.delete;if(b.dataset.delete||S.bolsa[id]<=1)delete S.bolsa[id];else S.bolsa[id]--;save();updateHeader();return showBag();}
  if(b.dataset.cat){filter=b.dataset.cat;shown=8;return renderCatalog()}
  if(b.dataset.motor){engine=b.dataset.motor;liveRank=null;renderRecs();if(engine==="Neo4j")await refreshGraph();return}
  if(b.dataset.lab){tab=b.dataset.lab;return renderLab()}
  if(b.dataset.stage){stage=b.dataset.stage;return renderLab()}
  if(b.dataset.node){const path=D.caminos[profile];if(!path)return;graphNote={inicio:"Perfil de prueba "+profile+".",compartido:"Artículo en común: "+pInfo(path.compartido)+".",vecino:"Reseñador anonimizado "+path.vecino+".",candidato:"Sugerencia explicada: "+pInfo(path.candidato)+"."}[b.dataset.node];$("#graphNote").textContent=graphNote;return}
  switch(b.dataset.act){
   case "explorar":abrirCatalogo();break;
   case "deportes":closeModal();verSeccion("deportes");break;
   case "buscar":abrirCatalogo("Todo",false,true);break;
   case "guardados":abrirCatalogo("Todo",true);break;
   case "mas":shown=products.length;renderCatalog();break;
   case "bolsa":showBag();break;
   case "cerrar":closeModal();break;
   case "comprar":checkout();break;
   case "misrecs":closeModal();navigate("para-ti");break;
   case "graforecs":engine="Neo4j";closeModal();navigate("para-ti");break;
   case "sesion":traceExample=false;openLab("sesion");break;
   case "metodo":openLab("modelos");break;
   case "traza":traceExample=true;renderLab();break;
   case "sesionpropia":traceExample=false;if(online)await readServices();renderLab();break;
   case "recargar":b.disabled=true;await readServices();renderLab();break;
   case "exportar":exportJSON();break;
   case "consultar":b.disabled=true;if(["sesion","archivo"].includes(report)&&online&&!traceExample)await readServices();queryRan=true;renderLab();break;
   case "archivoejemplo":traceExample=true;queryRan=true;renderLab();break;
   case "cohorte":b.disabled=true;{const u=profile,r=await api("/api/cohorte?u="+encodeURIComponent(u));if(u===profile)peerLive={u,ranking:r.ranking.map(x=>x.id)};}renderLab();break;
  }
 }catch{toast("La operación no se pudo confirmar. Revisa los servicios y vuelve a intentarlo.");if(b.isConnected)b.disabled=false}
});
document.addEventListener("change",e=>{
 if(e.target.id==="orden"){sort=e.target.value;renderCatalog()}
 if(e.target.id==="perfilModelo"){profile=e.target.value;peerLive=null;graphNote="";renderLab()}
 if(e.target.id==="tipoConsulta"){report=e.target.value;queryRan=false;renderLab()}
 if(e.target.id==="minEstrellas"){cutStars=Number(e.target.value);queryRan=false;renderLab()}
});
$("#buscar").addEventListener("input",e=>{search=e.target.value;shown=8;renderCatalog()});
mod.addEventListener("click",e=>{if(e.target===mod){const r=mod.getBoundingClientRect();if(e.clientX<r.left||e.clientX>r.right||e.clientY<r.top||e.clientY>r.bottom)closeModal()}});
window.addEventListener("hashchange",()=>{renderPage(!scrollPendiente);desplazar()});
window.addEventListener("storage",e=>{if(e.key===key&&e.newValue){try{let other=JSON.parse(e.newValue);if(other.usuario===S.usuario&&Array.isArray(other.eventos)){S=other;renderPage();if($("#bagRows")&&mod.open)showBag()}}catch{}}});
save();renderPage();probe();
