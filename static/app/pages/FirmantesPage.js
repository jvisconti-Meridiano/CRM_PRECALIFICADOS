import {h,useState,useEffect,useMemo,useCallback,useRef} from "../deps.js";
import {T} from "../theme.js";
import {GET, POST, PUT, DEL, UPLOAD} from "../api.js";
import {Badge, Btn, Inp, Sel, SearchInput, Card, Tbl, Modal, SectionTitle} from "../ui.js";
import {fmtARS, fmtDate} from "../format.js";

function FirmantesPage({user,notify,themeName}){
  const[rows,setRows]=useState([]);const[loading,setL]=useState(true);
  const[search,setSearch]=useState("");const[scope,setScope]=useState("3ros");
  const[showInactive,setSI]=useState(false);const[firstLine,setFL]=useState("all");
  const[selId,setSelId]=useState("");const[busy,setBusy]=useState(false);
  const[importOpen,setImportOpen]=useState(false);const[createOpen,setCreateOpen]=useState(false);
  const hasLine=(r,kind)=>{const lim=Number(kind==="3ros"?r?.lim3:r?.limp)||0;const exp=((kind==="3ros"?r?.exp3:r?.expp)||"").trim();return lim>0||!!exp;};
  const[admins,setAdmins]=useState([]);const[approval,setApproval]=useState({require:false,approver:""});
  const canEdit=user?.role==="admin"||user?.role==="risk";
  const isAdmin=user?.role==="admin";const isRisk=user?.role==="risk";

  const load=useCallback(async()=>{
    setL(true);
    const qs=new URLSearchParams({view_scope:scope,first_line:firstLine,show_inactive:showInactive?"1":"0"});
    if(search.trim())qs.set("q",search.trim());
    const res=await GET("/api/firmantes?"+qs);
    setRows(res?.firmantes||[]);setL(false);
  },[scope,firstLine,showInactive,search]);

  useEffect(()=>{load();},[scope,firstLine,showInactive]);
  useEffect(()=>{if(!isRisk)return;GET("/api/users/admins").then(r=>setAdmins(r?.users||[])).catch(()=>setAdmins([]));},[isRisk]);

  const selected=useMemo(()=>rows.find(x=>x.cuit_digits===selId)||null,[rows,selId]);
  useEffect(()=>{if(!selected){setApproval({require:false,approver:""});return;}setApproval({require:!!selected._require_approval,approver:selected._approver_username||""});},[selected]);
  useEffect(()=>{if(!selId&&rows[0])setSelId(rows[0].cuit_digits);},[rows]);

  const lK=scope==="3ros"?"lim3":scope==="propio"?"limp":"limf";
  const uK=scope==="3ros"?"used3":scope==="propio"?"usedp":"usedf";
  const bK=scope==="3ros"?"blocked3":scope==="propio"?"blockedp":"blockedf";
  const aK=scope==="3ros"?"avail3":scope==="propio"?"availp":"availf";

  const saveLimits=async()=>{
    if(!selected)return;setBusy(true);
    const payload={lim3:selected.lim3,exp3:selected.exp3||"",limp:selected.limp,expp:selected.expp||"",limf:selected.limf,group_name:selected.grupo_name||"",primera_linea:!!selected.primera_linea,observacion:selected._obs||""};
    if(isRisk){payload.require_approval=approval.require;payload.approver_username=approval.approver||"";}
    const res=await PUT("/api/firmantes/"+selected.cuit_digits+"/limits",payload);
    setBusy(false);notify?.(res?.message||res?.error||"OK",res?.ok===false?"error":"ok");await load();
  };
  const toggleActive=async(active)=>{
    if(!selected)return;
    const payload={observacion:selected._obs||""};
    if(isRisk){payload.require_approval=approval.require;payload.approver_username=approval.approver||"";}
    const res=await POST("/api/firmantes/"+selected.cuit_digits+"/"+(active?"reactivate":"deactivate"),payload);
    notify?.(res?.message||res?.error||"OK",res?.ok===false?"error":"ok");await load();
  };
  const addBlock=async()=>{
    if(!selected)return;const amt=window.prompt("Monto a bloquear (ARS):","0");if(!amt)return;
    const sc=window.prompt("Tipo: 3ros / propio / fce",scope);if(!sc)return;
    const res=await POST("/api/firmantes/"+selected.cuit_digits+"/blocks",{amount:amt,scope:sc});
    notify?.(res?.message||res?.error||"OK",res?.ok===false?"error":"ok");await load();
  };
  const rmBlock=async(id)=>{const res=await DEL("/api/firmantes/blocks/"+id);notify?.(res?.message||"OK",res?.ok===false?"error":"ok");await load();};
  const destroy=async()=>{if(!selected||!confirm("\u00bfEliminar definitivamente a "+selected.razon_social+"?"))return;const res=await DEL("/api/firmantes/"+selected.cuit_digits);notify?.(res?.message||"OK",res?.ok===false?"error":"ok");setSelId("");await load();};
  const setField=(cd,k,v)=>setRows(rows.map(r=>r.cuit_digits===cd?{...r,[k]:v}:r));

  const cols=[
    {header:"Firmante",exportHeader:"Firmante",render:r=>h`<div><button onClick=${()=>setSelId(r.cuit_digits)} style=${{background:"transparent",border:"none",color:T.t.p,padding:0,cursor:"pointer",fontWeight:700,textAlign:"left",fontFamily:T.f.s,fontSize:"13px"}}>${r.razon_social}</button><div style=${{display:"flex",gap:"4px",flexWrap:"wrap",marginTop:"3px"}}><span style=${{fontSize:"11px",opacity:.6,fontFamily:T.f.m}}>${r.cuit}</span>${r.grupo_name?h`<${Badge} size="xs">${r.grupo_name}<//>`:null}${r.primera_linea?h`<${Badge} variant="primera" size="xs">1RA<//>`:null}${r.blocked_any?h`<${Badge} variant="warning" size="xs">BLQ<//>`:null}</div></div>`,exportValue:r=>`${r.razon_social||""} (${r.cuit||""})`,wrap:true},
    {header:"Límite",exportHeader:"Límite",render:r=>fmtARS(r[lK]),exportValue:r=>Number(r[lK]||0),align:"right",mono:true},
    {header:"Usado",exportHeader:"Usado",render:r=>fmtARS(r[uK]),exportValue:r=>Number(r[uK]||0),align:"right",mono:true},
    {header:"Bloq.",exportHeader:"Bloqueado",render:r=>r[bK]>0?fmtARS(r[bK]):"\u2014",exportValue:r=>Number(r[bK]||0),align:"right",mono:true},
    {header:"Disponible",exportHeader:"Disponible",render:r=>h`<span style=${{fontWeight:700,color:r[aK]<0?T.t.err:r[aK]<r[lK]*.1?T.t.w:T.t.ok}}>${fmtARS(r[aK])}</span>`,exportValue:r=>Number(r[aK]||0),align:"right",mono:true},
    {header:"Estado",exportHeader:"Estado",render:r=>{const noLine=(scope==="3ros"&&!hasLine(r,"3ros"))||(scope==="propio"&&!hasLine(r,"propio"));return h`<${Badge} variant=${!r.is_active?"danger":noLine?"default":((scope==="3ros"&&!r.line3_active)||(scope==="propio"&&!r.linep_active))?"warning":"success"} size="xs">${!r.is_active?"Inactivo":noLine?"Sin línea":((scope==="3ros"&&!r.line3_active)||(scope==="propio"&&!r.linep_active))?"Vencida":"Activa"}<//>`},exportValue:r=>{const noLine=(scope==="3ros"&&!hasLine(r,"3ros"))||(scope==="propio"&&!hasLine(r,"propio"));return !r.is_active?"Inactivo":noLine?"Sin línea":((scope==="3ros"&&!r.line3_active)||(scope==="propio"&&!r.linep_active))?"Vencida":"Activa";}},
  ];

  return h`<div style=${{display:"flex",flexDirection:"column",gap:"16px"}}>
    <${SectionTitle} title="Firmantes precalificados" sub=${rows.length+" firmantes \u00b7 Vista: "+scope.toUpperCase()}>
      <${SearchInput} value=${search} onChange=${setSearch} onSubmit=${load} placeholder="Buscar CUIT o raz\u00f3n social\u2026" />
      <${Sel} value=${scope} onChange=${setScope} options=${[{value:"3ros",label:"Terceros"},{value:"propio",label:"Propios"},{value:"fce",label:"FCE"}]} />
      <${Sel} value=${firstLine} onChange=${setFL} options=${[{value:"all",label:"Todos"},{value:"yes",label:"1ra l\u00ednea"},{value:"no",label:"Sin 1ra"}]} />
      <label style=${{display:"flex",alignItems:"center",gap:"4px",fontSize:"12px",color:T.t.s,cursor:"pointer"}}><input type="checkbox" checked=${showInactive} onChange=${e=>setSI(e.target.checked)} /> Inactivos</label>
      <${Btn} onClick=${load} size="sm">Refrescar<//>
      ${canEdit?h`<${Btn} onClick=${()=>setCreateOpen(true)} size="sm" variant="primary">Alta manual<//><${Btn} onClick=${()=>setImportOpen(true)} size="sm">Importar CSV<//>`:null}
    <//>

    <div style=${{display:"grid",gridTemplateColumns:"1.2fr 0.9fr",gap:"16px",alignItems:"start"}}>
      <${Card} style=${{minWidth:0,overflow:"hidden"}}><${Tbl} columns=${cols} data=${rows} empty=${loading?"Cargando\u2026":"Sin firmantes"} maxH="65vh" exportFileName=${`firmantes-${scope}.xlsx`} exportSheetName="Firmantes" onExportResult=${ok=>notify?.(ok?"Excel exportado.":"No se pudo exportar el Excel.",ok?"ok":"error")} /><//>

      <${Card}>${selected?h`<div>
        <div style=${{display:"flex",justifyContent:"space-between",alignItems:"flex-start",gap:"10px",marginBottom:"12px"}}><div><h3 style=${{margin:"0 0 4px",fontSize:"15px"}}>${selected.razon_social}</h3><div style=${{fontSize:"12px",color:T.t.s,fontFamily:T.f.m}}>${selected.cuit}</div><div style=${{display:"flex",gap:"4px",marginTop:"6px"}}><${Badge} variant=${selected.is_active?"success":"danger"} size="xs">${selected.is_active?"Activo":"Inactivo"}<//>${selected.grupo_name?h`<${Badge} size="xs">${selected.grupo_name}<//>`:null}${selected.primera_linea?h`<${Badge} variant="primera" size="xs">1RA L\u00cdNEA<//>`:null}</div></div></div>

        ${canEdit?h`<div style=${{display:"grid",gridTemplateColumns:"1fr 1fr",gap:"8px"}}>
          <div><label style=${{fontSize:"11px",color:T.t.m}}>Límite 3ros</label><${Inp} value=${String(selected.lim3??"")} onChange=${v=>setField(selected.cuit_digits,"lim3",v)} /></div>
          <div><label style=${{fontSize:"11px",color:T.t.m}}>Vto 3ros</label><${Inp} type="date" value=${selected.exp3||""} onChange=${v=>setField(selected.cuit_digits,"exp3",v)} /></div>
          <div><label style=${{fontSize:"11px",color:T.t.m}}>Límite propio</label><${Inp} value=${String(selected.limp??"")} onChange=${v=>setField(selected.cuit_digits,"limp",v)} /></div>
          <div><label style=${{fontSize:"11px",color:T.t.m}}>Vto propio</label><${Inp} type="date" value=${selected.expp||""} onChange=${v=>setField(selected.cuit_digits,"expp",v)} /></div>
          <div><label style=${{fontSize:"11px",color:T.t.m}}>Límite FCE</label><${Inp} value=${String(selected.limf??"")} onChange=${v=>setField(selected.cuit_digits,"limf",v)} /></div>
          <div><label style=${{fontSize:"11px",color:T.t.m}}>Grupo económico</label><${Inp} value=${selected.grupo_name||""} onChange=${v=>setField(selected.cuit_digits,"grupo_name",v)} placeholder="Nombre del grupo" /></div>
          <div style=${{gridColumn:"1 / span 2",display:"flex",alignItems:"center",gap:"8px",paddingTop:"4px"}}><label style=${{display:"flex",alignItems:"center",gap:"6px",fontSize:"12px",color:T.t.s,cursor:"pointer"}}><input type="checkbox" checked=${!!selected.primera_linea} onChange=${e=>setField(selected.cuit_digits,"primera_linea",e.target.checked)} /> Primera línea</label></div>
        </div>
        <div style=${{marginTop:"8px"}}><textarea rows="2" placeholder="Observación para auditoría" value=${selected._obs||""} onInput=${e=>setField(selected.cuit_digits,"_obs",e.target.value)} style=${{width:"100%",padding:"10px 12px",borderRadius:T.r.md,border:"1px solid "+T.bd.d,background:T.bg.input,color:T.t.p,outline:"none",fontSize:"13px",resize:"vertical",fontFamily:T.f.s}} /></div>
        <${ApprovalFields} show=${isRisk} enabled=${approval.require} onToggle=${v=>setApproval(a=>({...a,require:v}))} approver=${approval.approver} onApproverChange=${v=>setApproval(a=>({...a,approver:v}))} approvers=${admins} />
        <div style=${{display:"flex",gap:"6px",flexWrap:"wrap",marginTop:"10px"}}>
          <${Btn} onClick=${saveLimits} variant="primary" disabled=${busy}>Guardar<//>
          <${Btn} onClick=${addBlock}>Bloquear<//>
          ${selected.is_active?h`<${Btn} onClick=${()=>toggleActive(false)} variant="ghost" size="sm">Desactivar<//>`:h`<${Btn} onClick=${()=>toggleActive(true)} variant="ghost" size="sm">Reactivar<//>`}
          ${isAdmin?h`<${Btn} onClick=${destroy} variant="danger" size="sm">Eliminar<//>`:null}
        </div>`:h`<div style=${{padding:"8px 0",display:"grid",gridTemplateColumns:"1fr 1fr",gap:"6px",fontSize:"13px"}}>

          <div>L\u00edm. 3ros: <b>${fmtARS(selected.lim3)}</b></div><div>Vto: ${fmtDate(selected.exp3)}</div>
          <div>L\u00edm. propio: <b>${fmtARS(selected.limp)}</b></div><div>Vto: ${fmtDate(selected.expp)}</div>
          <div>L\u00edm. FCE: <b>${fmtARS(selected.limf)}</b></div>
        </div>`}
        ${(selected.blocks||[]).length>0?h`<div style=${{marginTop:"14px",borderTop:"1px solid "+T.bd.s,paddingTop:"12px"}}><div style=${{fontWeight:600,fontSize:"12px",marginBottom:"6px"}}>Bloqueos del d\u00eda</div><${Tbl} columns=${[{header:"Usuario",exportHeader:"Usuario",key:"username"},{header:"Tipo",exportHeader:"Tipo",key:"scope"},{header:"Monto",exportHeader:"Monto",render:b=>fmtARS(b.amount),exportValue:b=>Number(b.amount||0),align:"right",mono:true},{header:"",exportHeader:"",render:b=>b.can_delete?h`<button onClick=${()=>rmBlock(b.id)} style=${{background:"transparent",border:"none",color:T.t.err,cursor:"pointer",fontSize:"12px"}}>\u2715</button>`:null}]} data=${selected.blocks} exportFileName=${`bloqueos-${selected.cuit_digits}.xlsx`} exportSheetName="Bloqueos" onExportResult=${ok=>notify?.(ok?"Excel exportado.":"No se pudo exportar el Excel.",ok?"ok":"error")} /></div>`:null}
      </div>`:h`<div style=${{color:T.t.m,padding:"20px",textAlign:"center"}}>Seleccion\u00e1 un firmante</div>`}<//>
    </div>
    <${ManualFirmanteModal} open=${createOpen} onClose=${()=>setCreateOpen(false)} notify=${notify} user=${user} onDone=${async()=>{setCreateOpen(false);await load();}} />
    <${ImportModal} open=${importOpen} onClose=${()=>setImportOpen(false)} notify=${notify} onDone=${async()=>{setImportOpen(false);await load();}} />
  </div>`;
}


function ApprovalFields({enabled,onToggle,approver,onApproverChange,approvers,show}){
  if(!show)return null;
  return h`<div style=${{marginTop:"10px",padding:"12px",borderRadius:T.r.md,border:"1px solid "+T.bd.s,background:T.bg.input,display:"grid",gap:"10px"}}>
    <label style=${{display:"flex",alignItems:"center",gap:"6px",fontSize:"12px",color:T.t.s,cursor:"pointer"}}><input type="checkbox" checked=${enabled} onChange=${e=>onToggle?.(e.target.checked)} /> Pedir aprobación</label>
    ${enabled?h`<div><label style=${{fontSize:"11px",color:T.t.m,display:"block",marginBottom:"4px"}}>Aprobador admin</label><select value=${approver||""} onChange=${e=>onApproverChange?.(e.target.value)} style=${{width:"100%",padding:"10px 12px",borderRadius:T.r.md,border:"1px solid "+T.bd.d,background:T.bg.input,color:T.t.p,fontSize:"13px",outline:"none",fontFamily:T.f.s}}><option value="">Seleccionar aprobador…</option>${(approvers||[]).map(a=>h`<option key=${a.username} value=${a.username}>${a.username}</option>`)}</select></div>`:null}
  </div>`;
}

function ManualFirmanteModal({open,onClose,notify,onDone,user}){
  const blank=()=>({razon_social:"",cuit:"",lim3:"",limp:"",limf:"",exp3:"",expp:"",primera_linea:false,observacion:"",require_approval:false,approver_username:""});
  const[form,setForm]=useState(blank());
  const[busy,setBusy]=useState(false);
  const[admins,setAdmins]=useState([]);
  const isRisk=user?.role==="risk";
  useEffect(()=>{
    if(!open)return;
    setForm(blank());
    if(isRisk)GET("/api/users/admins").then(r=>setAdmins(r?.users||[])).catch(()=>setAdmins([]));
  },[open,isRisk]);
  const setField=(k,v)=>setForm(f=>({...f,[k]:v}));
  const submit=async()=>{
    if(!form.razon_social.trim()||!form.cuit.trim()){
      notify?.("Razón social y CUIT son obligatorios.","error");
      return;
    }
    setBusy(true);
    const payload={...form,razon_social:form.razon_social.trim(),cuit:form.cuit.trim(),observacion:(form.observacion||"").trim()};
    const res=await POST("/api/firmantes",payload);
    setBusy(false);
    notify?.(res?.message||res?.error||"OK",res?.ok===false?"error":"ok");
    if(res?.ok!==false){onDone?.();onClose?.();}
  };
  return h`<${Modal} open=${open} onClose=${busy?()=>{}:onClose} title="Alta manual de firmante" width="640px">
    <div style=${{display:"grid",gridTemplateColumns:"1fr 1fr",gap:"10px"}}>
      <div style=${{gridColumn:"1 / span 2"}}><label style=${{fontSize:"11px",color:T.t.m}}>Razón social</label><${Inp} value=${form.razon_social} onChange=${v=>setField("razon_social",v)} placeholder="Nombre del firmante" /></div>
      <div><label style=${{fontSize:"11px",color:T.t.m}}>CUIT</label><${Inp} value=${form.cuit} onChange=${v=>setField("cuit",v)} placeholder="30-12345678-9" /></div>
      <div style=${{display:"flex",alignItems:"end"}}><label style=${{display:"flex",alignItems:"center",gap:"6px",fontSize:"12px",color:T.t.s,cursor:"pointer"}}><input type="checkbox" checked=${form.primera_linea} onChange=${e=>setField("primera_linea",e.target.checked)} /> Primera línea</label></div>
      <div><label style=${{fontSize:"11px",color:T.t.m}}>Límite 3ros</label><${Inp} value=${form.lim3} onChange=${v=>setField("lim3",v)} placeholder="0" /></div>
      <div><label style=${{fontSize:"11px",color:T.t.m}}>Vto 3ros</label><${Inp} type="date" value=${form.exp3} onChange=${v=>setField("exp3",v)} /></div>
      <div><label style=${{fontSize:"11px",color:T.t.m}}>Límite propio</label><${Inp} value=${form.limp} onChange=${v=>setField("limp",v)} placeholder="0" /></div>
      <div><label style=${{fontSize:"11px",color:T.t.m}}>Vto propio</label><${Inp} type="date" value=${form.expp} onChange=${v=>setField("expp",v)} /></div>
      <div><label style=${{fontSize:"11px",color:T.t.m}}>Límite FCE</label><${Inp} value=${form.limf} onChange=${v=>setField("limf",v)} placeholder="0" /></div>
      <div></div>
      <div style=${{gridColumn:"1 / span 2"}}><label style=${{fontSize:"11px",color:T.t.m}}>Observación</label><textarea rows="3" value=${form.observacion} onInput=${e=>setField("observacion",e.target.value)} style=${{width:"100%",padding:"10px 12px",borderRadius:T.r.md,border:"1px solid "+T.bd.d,background:T.bg.input,color:T.t.p,outline:"none",fontSize:"13px",resize:"vertical",fontFamily:T.f.s}} placeholder="Comentario para auditoría" /></div>
    </div>
    <${ApprovalFields} show=${isRisk} enabled=${form.require_approval} onToggle=${v=>setField("require_approval",v)} approver=${form.approver_username} onApproverChange=${v=>setField("approver_username",v)} approvers=${admins} />
    <div style=${{display:"flex",gap:"8px",marginTop:"14px",justifyContent:"flex-end"}}><${Btn} onClick=${onClose} disabled=${busy}>Cancelar<//><${Btn} onClick=${submit} variant="primary" disabled=${busy}>${busy?"Guardando…":"Crear firmante"}<//></div>
  <//>`;
}

/* ═══ IMPORT MODAL ═══ */
function ImportModal({open,onClose,notify,onDone}){
  const fileRef=useRef(null);
  const[step,setStep]=useState("upload");const[result,setResult]=useState(null);
  const[batchId,setBatchId]=useState("");const[unknownGroups,setUnknown]=useState([]);
  const[decisions,setDecisions]=useState({});const[busy,setBusy]=useState(false);
  const reset=()=>{setStep("upload");setResult(null);setBatchId("");setUnknown([]);setDecisions({});setBusy(false);};
  useEffect(()=>{if(open)reset();},[open]);

  const doUpload=async()=>{
    const file=fileRef.current?.files?.[0];if(!file)return;setBusy(true);
    const res=await UPLOAD("/api/firmantes/import",file);setBusy(false);
    if(res?.needs_confirmation){setBatchId(res.batch_id);setUnknown(res.unknown_groups||[]);
      const d={};(res.unknown_groups||[]).forEach(n=>{d[n]={create:false,limit:""};});setDecisions(d);setStep("groups");
    }else{setResult(res);setStep("done");}
  };
  const doConfirm=async()=>{setBusy(true);
    const decs=Object.entries(decisions).map(([name,d])=>({name,create:d.create,limit:d.limit}));
    const res=await POST("/api/firmantes/import/confirm",{batch_id:batchId,decisions:decs});
    setBusy(false);setResult(res);setStep("done");
  };

  return h`<${Modal} open=${open} onClose=${onClose} title="Importar firmantes desde CSV" width="600px">
    ${step==="upload"?h`<div>
      <p style=${{fontSize:"13px",color:T.t.s,marginBottom:"14px"}}>Sub\u00ed un archivo CSV con columnas: Raz\u00f3n Social, CUIT, l\u00edmites, vencimientos, grupo, primera l\u00ednea.</p>
      <input ref=${fileRef} type="file" accept=".csv,.txt" style=${{fontSize:"13px",color:T.t.p}} />
      <div style=${{display:"flex",gap:"8px",marginTop:"14px"}}><${Btn} onClick=${doUpload} variant="primary" disabled=${busy}>${busy?"Procesando\u2026":"Importar"}<//><${Btn} onClick=${onClose}>Cancelar<//></div>
    </div>`:null}
    ${step==="groups"?h`<div>
      <p style=${{fontSize:"13px",color:T.t.w,marginBottom:"14px"}}>Se encontraron grupos desconocidos:</p>
      ${unknownGroups.map(name=>h`<div key=${name} style=${{padding:"10px",borderRadius:T.r.md,border:"1px solid "+T.bd.s,marginBottom:"8px"}}>
        <div style=${{display:"flex",alignItems:"center",gap:"8px",marginBottom:"6px"}}><b style=${{fontSize:"13px"}}>${name}</b><label style=${{display:"flex",alignItems:"center",gap:"4px",fontSize:"12px",color:T.t.s}}><input type="checkbox" checked=${decisions[name]?.create||false} onChange=${e=>setDecisions({...decisions,[name]:{...decisions[name],create:e.target.checked}})} /> Crear</label></div>
        ${decisions[name]?.create?h`<${Inp} value=${decisions[name]?.limit||""} onChange=${v=>setDecisions({...decisions,[name]:{...decisions[name],limit:v}})} placeholder="L\u00edmite grupal" style=${{maxWidth:"200px"}} />`:null}
      </div>`)}
      <div style=${{display:"flex",gap:"8px",marginTop:"12px"}}><${Btn} onClick=${doConfirm} variant="primary" disabled=${busy}>${busy?"Procesando\u2026":"Confirmar"}<//><${Btn} onClick=${onClose}>Cancelar<//></div>
    </div>`:null}
    ${step==="done"?h`<div>
      <div style=${{padding:"14px",borderRadius:T.r.md,background:result?.ok!==false?T.c.greenG:T.c.redG,border:"1px solid "+(result?.ok!==false?"rgba(16,185,129,0.2)":"rgba(239,68,68,0.2)"),marginBottom:"12px",fontSize:"13px",color:result?.ok!==false?T.t.ok:T.t.err}}>${result?.message||"Completado"}</div>
      ${result?.summary?h`<div style=${{fontSize:"13px",color:T.t.s,marginBottom:"8px"}}>OK: ${result.summary.ok} \u00b7 Fallidos: ${result.summary.failed}</div>`:null}
      ${result?.summary?.reasons?.length?h`<div style=${{maxHeight:"150px",overflowY:"auto",fontSize:"12px",color:T.t.m}}>${result.summary.reasons.map((r,i)=>h`<div key=${i} style=${{padding:"3px 0"}}>${r}</div>`)}</div>`:null}
      <${Btn} onClick=${onDone} variant="primary" style=${{marginTop:"12px"}}>Cerrar<//>
    </div>`:null}
  <//>`;
}

export {FirmantesPage, ApprovalFields, ManualFirmanteModal, ImportModal};
