import {h,useState,useEffect,useMemo,useCallback,useRef} from "../deps.js";
import {T} from "../theme.js";
import {GET} from "../api.js";
import {Badge, Card, KPICard, ProgressBar, Loader, SectionTitle} from "../ui.js";
import {fmtARS, fmtPct} from "../format.js";

function DashboardPage(){
  const[firmantes,setF]=useState([]);const[grupos,setG]=useState([]);const[loading,setL]=useState(true);
  useEffect(()=>{Promise.all([GET("/api/firmantes?view_scope=3ros"),GET("/api/grupos")]).then(([fd,gd])=>{setF(fd?.firmantes||[]);setG(gd?.grupos||[]);setL(false);}).catch(()=>setL(false));},[]);
  if(loading)return h`<${Loader}/>`;
  const f=firmantes;
  const tLim=f.reduce((s,x)=>s+(x.lim3||0)+(x.limp||0)+(x.limf||0),0);
  const tUsed=f.reduce((s,x)=>s+(x.used3||0)+(x.usedp||0)+(x.usedf||0),0);
  const tBlocked=f.reduce((s,x)=>s+(x.blocked3||0)+(x.blockedp||0)+(x.blockedf||0),0);
  const tAvail=tLim-tUsed-tBlocked;const excedidos=f.filter(x=>(x.avail3||0)<0||(x.availp||0)<0);const util=tLim>0?tUsed/tLim:0;
  return h`<div style=${{display:"flex",flexDirection:"column",gap:"20px"}}>
    <${SectionTitle} title="Panel de control" sub="Vista consolidada del portfolio crediticio" />
    <div style=${{display:"flex",gap:"12px",flexWrap:"wrap"}}>
      <${KPICard} label="Límite total" value=${fmtARS(tLim)} sub=${f.length+" firmantes"} />
      <${KPICard} label="Utilización" value=${fmtPct(util)} variant=${util>.85?"danger":util>.7?"warning":"success"} sub=${fmtARS(tUsed)+" usado"} />
      <${KPICard} label="Disponible neto" value=${fmtARS(tAvail)} variant=${tAvail<0?"danger":"success"} sub=${fmtARS(tBlocked)+" bloqueado"} />
      <${KPICard} label="Excedidos" value=${String(excedidos.length)} variant=${excedidos.length?"danger":"success"} sub="Firmantes sobre límite" />
    </div>
    <div style=${{display:"grid",gridTemplateColumns:"1fr 1fr",gap:"16px"}}>
      <${Card}><h3 style=${{fontSize:"14px",fontWeight:500,margin:"0 0 14px"}}>Firmantes excedidos</h3>${excedidos.length===0?h`<p style=${{color:T.t.m,fontSize:"13px"}}>Portfolio saludable.</p>`:excedidos.slice(0,8).map(x=>h`<div key=${x.id} style=${{padding:"10px 0",borderBottom:"1px solid "+T.bd.s,display:"flex",justifyContent:"space-between",alignItems:"center"}}><div><div style=${{fontSize:"13px",fontWeight:500}}>${x.razon_social}</div><div style=${{fontSize:"11px",color:T.t.m,fontFamily:T.f.m}}>${x.cuit}</div></div><div style=${{textAlign:"right"}}><div style=${{fontSize:"13px",fontWeight:600,color:T.t.err}}>${fmtARS(x.avail3)}</div><div style=${{fontSize:"11px",color:T.t.m}}>disp. 3ros</div></div></div>`)}<//>
      <${Card}><h3 style=${{fontSize:"14px",fontWeight:500,margin:"0 0 14px"}}>Grupos económicos</h3><div style=${{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(200px,1fr))",gap:"10px"}}>${grupos.map(g=>{const av=g.avail!=null?g.avail:g.limite-g.used-g.blocked;const pct=g.limite>0?g.used/g.limite:0;return h`<div key=${g.id} style=${{padding:"12px 14px",borderRadius:T.r.md,background:T.bg.surface,border:"1px solid "+T.bd.s}}><div style=${{display:"flex",justifyContent:"space-between",marginBottom:"8px"}}><span style=${{fontSize:"13px",fontWeight:500}}>${g.nombre}</span><${Badge} variant=${av<0?"danger":pct>.85?"warning":"default"} size="xs">${fmtPct(pct)}<//></div><${ProgressBar} pct=${pct*100} color=${pct>.9?T.c.red:pct>.75?T.c.amber:T.c.blue} /><div style=${{display:"flex",justifyContent:"space-between",fontSize:"11px",color:T.t.m,marginTop:"6px"}}><span>Disp: <span style=${{color:av<0?T.t.err:T.t.p,fontWeight:500}}>${fmtARS(av)}</span></span><span>Lím: ${fmtARS(g.limite)}</span></div></div>`;})}</div><//>
    </div>
  </div>`;
}

export {DashboardPage};
