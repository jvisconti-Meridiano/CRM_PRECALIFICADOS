import {h,useState,useEffect,useMemo} from "../deps.js";
import {T} from "../theme.js";
import {GET, DEL} from "../api.js";
import {Badge, Btn, Card, Tbl, SearchInput, SectionTitle, Loader} from "../ui.js";

function AuditPage({user,notify,themeName}){
  const[rows,setR]=useState([]);
  const[loading,setL]=useState(true);
  const[search,setSearch]=useState("");

  const load=async()=>{
    setL(true);
    const q=search.trim()?("?q="+encodeURIComponent(search.trim())):"";
    const d=await GET("/api/audit"+q);
    setR(d?.rows||[]);
    setL(false);
  };
  useEffect(()=>{load();},[]);

  const filtered=useMemo(()=>!search.trim()?rows:rows.filter(a=>(a.razon_social||"").toLowerCase().includes(search.toLowerCase())||(a.firmante_cuit_digits||"").includes(search)||(a.action||"").toLowerCase().includes(search.toLowerCase())||(a.username||"").toLowerCase().includes(search.toLowerCase())),[rows,search]);
  const AC={ALTA:"success",BAJA:"danger",MODIFICACION:"blue",VENCIMIENTO:"warning",RENOVACION:"success",ELIMINACION:"danger",SOLICITUD:"blue",RECHAZO:"danger"};
  const isAdmin=user?.role==="admin";

  const delOne=async id=>{ if(!confirm("¿Eliminar registro?")) return; await DEL("/api/audit/"+id); await load(); };
  const delAll=async()=>{ if(!confirm("¿Eliminar TODOS los registros de auditoría?")) return; await DEL("/api/audit/all"); await load(); };

  const cols=useMemo(()=>[
    {header:"Fecha",exportHeader:"Fecha",render:r=>h`<span style=${{fontSize:"12px",color:T.t.s}}>${r.created_at}</span>`,exportValue:r=>r.created_at,mono:true},
    {header:"Firmante",exportHeader:"Firmante",render:r=>h`<div><div style=${{fontWeight:500,fontSize:"13px"}}>${r.razon_social||r.firmante_cuit_digits}</div>${r.cuit?h`<div style=${{fontSize:"11px",color:T.t.m,fontFamily:T.f.m}}>${r.cuit}</div>`:null}</div>`,exportValue:r=>`${r.razon_social||r.firmante_cuit_digits||""} (${r.cuit||r.firmante_cuit_digits||""})`,wrap:true},
    {header:"Acción",exportHeader:"Acción",render:r=>h`<${Badge} variant=${AC[r.action]||"default"} size="xs">${r.action}<//>`,exportValue:r=>r.action},
    {header:"Usuario",exportHeader:"Usuario",render:r=>h`<span style=${{color:T.t.s,fontSize:"12px"}}>${r.username}</span>`,exportValue:r=>r.username},
    {header:"Detalle",exportHeader:"Detalle",render:r=>h`<span style=${{fontSize:"12px",color:T.t.s,lineHeight:1.4}}>${r.details}</span>`,exportValue:r=>r.details,wrap:true,maxW:"400px"},
    ...(isAdmin?[{header:"",exportHeader:"",render:r=>h`<button onClick=${()=>delOne(r.id)} style=${{background:"transparent",border:"none",color:T.t.m,cursor:"pointer",fontSize:"12px"}}>✕</button>`}]:[]),
  ],[isAdmin,themeName]);

  if(loading) return h`<${Loader}/>`;
  return h`<div style=${{display:"flex",flexDirection:"column",gap:"18px"}}>
    <${SectionTitle} title="Auditoría ABM" sub=${filtered.length+" registros"}>
      <${SearchInput} value=${search} onChange=${setSearch} onSubmit=${load} placeholder="Buscar CUIT, razón, acción…" />
      ${isAdmin?h`<${Btn} onClick=${delAll} size="sm" variant="danger">Limpiar todo<//>`:null}
    <//>
    <${Card} style=${{overflow:"hidden"}}><${Tbl} columns=${cols} data=${filtered} maxH="72vh" exportFileName="auditoria.xlsx" exportSheetName="Auditoria" onExportResult=${ok=>notify?.(ok?"Excel exportado.":"No se pudo exportar el Excel.",ok?"ok":"error")} /><//>
  </div>`;
}

export {AuditPage};
