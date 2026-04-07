import {h,useState,useEffect,useMemo} from "../deps.js";
import {T} from "../theme.js";
import {GET, PUT, POST, DEL} from "../api.js";
import {Btn, Inp, Card, SectionTitle, Badge, KPICard, Loader, Modal, Tbl} from "../ui.js";
import {fmtARS} from "../format.js";

function PoliticaPage({user,notify,themeName}){
  const[policy,setP]=useState(null);
  const[extras,setE]=useState([]);
  const[loading,setL]=useState(true);
  const[editP,setEditP]=useState(null);
  const[showExtra,setShowExtra]=useState(false);
  const[ef,setEf]=useState({cuit:"",razon_social:"",segmento:"",limite:"",id:null});
  const isAdmin=user?.role==="admin";

  const load=async()=>{ setL(true); const d=await GET("/api/policy"); setP(d?.config||{}); setE(d?.extraordinary||[]); setL(false); };
  useEffect(()=>{load();},[]);

  const savePolicy=async()=>{ if(!editP) return; const r=await PUT("/api/policy/config",editP); notify?.(r?.message||r?.error||"OK",r?.ok===false?"error":"ok"); setEditP(null); await load(); };
  const saveExtra=async()=>{ const r=ef.id?await PUT("/api/policy/extraordinary/"+ef.id,ef):await POST("/api/policy/extraordinary",ef); notify?.(r?.message||r?.error||"OK",r?.ok===false?"error":"ok"); setShowExtra(false); setEf({cuit:"",razon_social:"",segmento:"",limite:"",id:null}); await load(); };
  const delExtra=async id=>{ if(!confirm("¿Desactivar este límite?")) return; await DEL("/api/policy/extraordinary/"+id); await load(); };
  const editEx=ex=>{ setEf({cuit:ex.cuit||"",razon_social:ex.razon_social||"",segmento:ex.segmento||"",limite:String(ex.limite||0),id:ex.id}); setShowExtra(true); };

  const lims=[{k:"nyps",l:"NYPS"},{k:"pymes",l:"PYMES"},{k:"t1_t2_ar",l:"T1, T2 y AR"},{k:"t2_directo",l:"T2 Directo"},{k:"garantizado",l:"Garantizado"}];
  const editing=editP!==null;

  const extraCols=useMemo(()=>[
    {header:"CUIT",exportHeader:"CUIT",render:r=>h`<span style=${{fontFamily:T.f.m}}>${r.cuit}</span>`,exportValue:r=>r.cuit},
    {header:"Razón social",exportHeader:"Razón social",render:r=>r.razon_social,exportValue:r=>r.razon_social},
    {header:"Segmento",exportHeader:"Segmento",render:r=>h`<${Badge} size="xs">${r.segmento}<//>`,exportValue:r=>r.segmento},
    {header:"Límite",exportHeader:"Límite",render:r=>fmtARS(r.limite||0),exportValue:r=>Number(r.limite||0),align:"right",mono:true},
    ...(isAdmin?[{header:"",exportHeader:"",render:r=>h`<div style=${{display:"flex",gap:"4px"}}><${Btn} onClick=${()=>editEx(r)} size="xs" variant="ghost">Editar<//><${Btn} onClick=${()=>delExtra(r.id)} size="xs" variant="danger">Quitar<//></div>`}]:[]),
  ],[isAdmin,themeName]);

  if(loading) return h`<${Loader}/>`;
  return h`<div style=${{display:"flex",flexDirection:"column",gap:"18px"}}>
    <${SectionTitle} title="Política crediticia" sub="Límites de concentración y extraordinarios">
      ${isAdmin&&!editing?h`<${Btn} onClick=${()=>setEditP({...policy})} variant="primary" size="sm">Editar política<//>`:null}
    <//>
    ${editing?h`<${Card}><h3 style=${{fontSize:"14px",fontWeight:600,margin:"0 0 12px"}}>Editar límites</h3>
      <div style=${{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(180px,1fr))",gap:"10px"}}>${lims.map(l=>h`<div key=${l.k}><label style=${{fontSize:"11px",color:T.t.m,textTransform:"uppercase"}}>${l.l}</label><${Inp} value=${String(editP[l.k]??"")} onChange=${v=>setEditP({...editP,[l.k]:v})} /></div>`)}</div>
      <div style=${{display:"flex",gap:"8px",marginTop:"12px"}}><${Btn} onClick=${savePolicy} variant="primary">Guardar<//><${Btn} onClick=${()=>setEditP(null)}>Cancelar<//></div>
    <//>`:h`<div style=${{display:"flex",gap:"12px",flexWrap:"wrap"}}>${lims.map(l=>h`<${KPICard} key=${l.k} label=${l.l} value=${fmtARS(policy?.[l.k]||0)} />`)}</div>`}

    <${Card}><div style=${{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:"14px",gap:"12px",flexWrap:"wrap"}}><h3 style=${{fontSize:"14px",fontWeight:500,margin:0}}>Límites extraordinarios</h3>${isAdmin?h`<${Btn} onClick=${()=>{setEf({cuit:"",razon_social:"",segmento:"",limite:"",id:null});setShowExtra(true);}} size="sm" variant="primary">Agregar<//>`:null}</div>
      ${extras.length===0?h`<p style=${{color:T.t.m,fontSize:"13px"}}>Sin límites extraordinarios activos.</p>`:h`<${Tbl} columns=${extraCols} data=${extras} exportFileName="politica-limites-extraordinarios.xlsx" exportSheetName="Extraordinarios" onExportResult=${ok=>notify?.(ok?"Excel exportado.":"No se pudo exportar el Excel.",ok?"ok":"error")} />`}
    <//>
    <${Modal} open=${showExtra} onClose=${()=>setShowExtra(false)} title=${ef.id?"Editar límite":"Nuevo límite extraordinario"}>
      <div style=${{display:"grid",gridTemplateColumns:"1fr 1fr",gap:"10px"}}><div><label style=${{fontSize:"11px",color:T.t.m}}>CUIT</label><${Inp} value=${ef.cuit} onChange=${v=>setEf({...ef,cuit:v})} /></div><div><label style=${{fontSize:"11px",color:T.t.m}}>Razón social</label><${Inp} value=${ef.razon_social} onChange=${v=>setEf({...ef,razon_social:v})} /></div><div><label style=${{fontSize:"11px",color:T.t.m}}>Segmento</label><${Inp} value=${ef.segmento} onChange=${v=>setEf({...ef,segmento:v})} /></div><div><label style=${{fontSize:"11px",color:T.t.m}}>Límite</label><${Inp} value=${ef.limite} onChange=${v=>setEf({...ef,limite:v})} /></div></div>
      <div style=${{display:"flex",gap:"8px",marginTop:"14px"}}><${Btn} onClick=${saveExtra} variant="primary">Guardar<//><${Btn} onClick=${()=>setShowExtra(false)}>Cancelar<//></div>
    <//>
  </div>`;
}

export {PoliticaPage};
