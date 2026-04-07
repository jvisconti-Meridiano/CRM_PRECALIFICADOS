import {h,useState,useEffect,useMemo} from "../deps.js";
import {T} from "../theme.js";
import {GET, POST} from "../api.js";
import {Badge, Btn, Card, Tbl, SectionTitle, Loader, TabBar} from "../ui.js";
import {fmtARS, fmtDT} from "../format.js";

function ApprovalsPage({notify,themeName}){
  const[requests,setR]=useState([]);
  const[loading,setL]=useState(true);
  const[filter,setFilter]=useState("pending");

  const load=async()=>{
    setL(true);
    const q=filter?`?status=${encodeURIComponent(filter)}`:"";
    const r=await GET("/api/approvals"+q);
    setR(r?.rows||[]);
    setL(false);
  };
  useEffect(()=>{load();},[filter]);

  const decide=async(id,action)=>{
    const r=await POST(`/api/approvals/${id}/${action}`,{});
    notify?.(r?.message||r?.error||"OK",r?.ok===false?"error":"ok");
    await load();
  };

  const AC_MAP={create:"Alta",update:"Modificación",deactivate:"Baja",reactivate:"Reactivación"};
  const ST_MAP={pending:{v:"warning",l:"Pendiente"},approved:{v:"success",l:"Aprobada"},rejected:{v:"danger",l:"Rechazada"}};

  const cols=useMemo(()=>[
    {header:"#",exportHeader:"ID",render:r=>r.id,exportValue:r=>r.id},
    {header:"Fecha",exportHeader:"Fecha",render:r=>fmtDT(r.created_at),exportValue:r=>fmtDT(r.created_at),mono:true},
    {header:"Solicitante",exportHeader:"Solicitante",render:r=>r.requested_by,exportValue:r=>r.requested_by},
    {header:"Acción",exportHeader:"Acción",render:r=>h`<${Badge} variant="blue" size="xs">${AC_MAP[r.action]||r.action}<//>`,exportValue:r=>AC_MAP[r.action]||r.action},
    {header:"Firmante",exportHeader:"Firmante",render:r=>{const p=r.payload||{};return h`<div><div style=${{fontWeight:500}}>${p.razon_social||r.entity_key}</div><div style=${{fontSize:"11px",color:T.t.m,fontFamily:T.f.m}}>${p.cuit||r.entity_key}</div></div>`;},exportValue:r=>{const p=r.payload||{};return `${p.razon_social||r.entity_key||""} (${p.cuit||r.entity_key||""})`;},wrap:true},
    {header:"Detalle",exportHeader:"Detalle",render:r=>{const p=r.payload||{};const parts=[];if(p.lim3!=null)parts.push("3ros: "+fmtARS(p.lim3));if(p.limp!=null)parts.push("prop: "+fmtARS(p.limp));if(p.observacion)parts.push("Obs: "+p.observacion);return h`<span style=${{fontSize:"12px",color:T.t.s}}>${parts.join(" · ")||"—"}</span>`;},exportValue:r=>{const p=r.payload||{};const parts=[];if(p.lim3!=null)parts.push(`3ros: ${p.lim3}`);if(p.limp!=null)parts.push(`prop: ${p.limp}`);if(p.observacion)parts.push(`Obs: ${p.observacion}`);return parts.join(" | ")||"";},wrap:true,maxW:"300px"},
    {header:"Estado",exportHeader:"Estado",render:r=>{const s=ST_MAP[r.status]||ST_MAP.pending;return h`<${Badge} variant=${s.v} size="xs">${s.l}<//>`;},exportValue:r=>(ST_MAP[r.status]||ST_MAP.pending).l},
    {header:"",exportHeader:"",render:r=>r.status==="pending"?h`<div style=${{display:"flex",gap:"4px"}}><${Btn} onClick=${()=>decide(r.id,"approve")} size="xs" variant="success">Aprobar<//><${Btn} onClick=${()=>decide(r.id,"reject")} size="xs" variant="danger">Rechazar<//></div>`:r.decision_note?h`<span style=${{fontSize:"11px",color:T.t.m}}>${r.decision_note}</span>`:null},
  ],[filter,themeName]);

  if(loading) return h`<${Loader}/>`;
  return h`<div style=${{display:"flex",flexDirection:"column",gap:"18px"}}>
    <${SectionTitle} title="Solicitudes de aprobación" sub=${requests.length+" solicitudes"}>
      <${TabBar} tabs=${[{id:"pending",label:"Pendientes"},{id:"",label:"Todas"},{id:"approved",label:"Aprobadas"},{id:"rejected",label:"Rechazadas"}]} active=${filter} onChange=${setFilter} />
    <//>
    <${Card} style=${{overflow:"hidden"}}><${Tbl} columns=${cols} data=${requests} exportFileName="aprobaciones.xlsx" exportSheetName="Aprobaciones" onExportResult=${ok=>notify?.(ok?"Excel exportado.":"No se pudo exportar el Excel.",ok?"ok":"error")} /><//>
  </div>`;
}

export {ApprovalsPage};
