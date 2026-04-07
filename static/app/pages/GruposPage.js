import {h,useState,useEffect,useMemo} from "../deps.js";
import {T} from "../theme.js";
import {GET, POST, PUT} from "../api.js";
import {Btn, Inp, Card, Tbl, SectionTitle, Loader, ProgressBar} from "../ui.js";
import {fmtARS, fmtPct} from "../format.js";

function GruposPage({user,notify,themeName}){
  const[grupos,setG]=useState([]);
  const[loading,setL]=useState(true);
  const canEdit=user?.role==="admin"||user?.role==="risk";
  const[nombre,setNombre]=useState("");
  const[limite,setLimite]=useState("");

  const load=async()=>{ setL(true); const r=await GET("/api/grupos"); setG(r?.grupos||[]); setL(false); };
  useEffect(()=>{load();},[]);

  const crear=async()=>{ if(!nombre.trim()) return; const r=await POST("/api/grupos",{nombre,limite}); notify?.(r?.message||r?.error||"OK",r?.ok===false?"error":"ok"); setNombre(""); setLimite(""); await load(); };
  const updateLim=async g=>{ const v=window.prompt("Nuevo límite para "+g.nombre,String(g.limite||0)); if(v===null) return; const r=await PUT(`/api/grupos/${g.id}/limit`,{limite:v}); notify?.(r?.message||r?.error||"OK",r?.ok===false?"error":"ok"); await load(); };

  const cols=useMemo(()=>[
    {header:"Grupo",exportHeader:"Grupo",render:r=>h`<span style=${{fontWeight:500}}>${r.nombre}</span>`,exportValue:r=>r.nombre},
    {header:"Límite",exportHeader:"Límite",render:r=>fmtARS(r.limite),exportValue:r=>Number(r.limite||0),align:"right",mono:true},
    {header:"Usado",exportHeader:"Usado",render:r=>fmtARS(r.used),exportValue:r=>Number(r.used||0),align:"right",mono:true},
    {header:"Bloq.",exportHeader:"Bloqueado",render:r=>r.blocked>0?fmtARS(r.blocked):"—",exportValue:r=>Number(r.blocked||0),align:"right",mono:true},
    {header:"Disponible",exportHeader:"Disponible",render:r=>h`<span style=${{fontWeight:700,color:(r.avail!=null?r.avail:r.limite-r.used-r.blocked)<0?T.t.err:T.t.ok}}>${fmtARS(r.avail!=null?r.avail:r.limite-r.used-r.blocked)}</span>`,exportValue:r=>Number(r.avail!=null?r.avail:r.limite-r.used-r.blocked),align:"right",mono:true},
    {header:"Utiliz.",exportHeader:"Utilización",render:r=>{const p=r.limite>0?r.used/r.limite:0;return h`<div style=${{display:"flex",alignItems:"center",gap:"8px"}}><div style=${{flex:1,minWidth:"40px"}}><${ProgressBar} pct=${p*100} color=${p>.9?T.c.red:p>.75?T.c.amber:T.c.blue} /></div><span style=${{fontSize:"11px",color:T.t.m,minWidth:"36px"}}>${fmtPct(p)}</span></div>`;},exportValue:r=>r.limite>0?(r.used/r.limite):0},
    ...(canEdit?[{header:"",exportHeader:"",render:r=>h`<${Btn} onClick=${()=>updateLim(r)} size="xs" variant="ghost">Editar<//>`,align:"center"}]:[]),
  ],[canEdit,themeName]);

  if(loading) return h`<${Loader}/>`;
  return h`<div style=${{display:"flex",flexDirection:"column",gap:"18px"}}>
    <${SectionTitle} title="Grupos económicos" sub=${grupos.length+" grupos activos"} />
    ${canEdit?h`<${Card}><div style=${{display:"flex",gap:"8px",alignItems:"center",flexWrap:"wrap"}}><${Inp} value=${nombre} onChange=${setNombre} placeholder="Nombre del grupo" style=${{maxWidth:"260px"}} /><${Inp} value=${limite} onChange=${setLimite} placeholder="Límite grupal" style=${{maxWidth:"200px"}} /><${Btn} onClick=${crear} variant="primary">Crear grupo<//></div><//>`:null}
    <${Card} style=${{overflow:"hidden"}}><${Tbl} columns=${cols} data=${grupos} exportFileName="grupos-economicos.xlsx" exportSheetName="Grupos" onExportResult=${ok=>notify?.(ok?"Excel exportado.":"No se pudo exportar el Excel.",ok?"ok":"error")} /><//>
  </div>`;
}

export {GruposPage};
