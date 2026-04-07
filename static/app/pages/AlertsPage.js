import {h,useState,useEffect,useRef,useMemo} from "../deps.js";
import {T} from "../theme.js";
import {GET, POST, PUT, DEL} from "../api.js";
import {Badge, Btn, Card, Tbl, SectionTitle, Inp, KPICard, Loader, ProgressBar, TabBar} from "../ui.js";
import {fmtARS, fmtDT, fmtDate} from "../format.js";

function AlertsPage({notify,themeName}){
  const[tab,setTab]=useState("events");
  const[stats,setS]=useState({});
  const[events,setEv]=useState([]);
  const[runs,setRuns]=useState([]);
  const[whitelist,setWL]=useState([]);
  const[settings,setSettings]=useState({local_workers:1,lambda_enabled:false,lambda_workers:0,lambda_region:"",lambda_function_name:""});
  const[loading,setL]=useState(true);
  const[progress,setProg]=useState(null);
  const pollRef=useRef(null);
  const[wlCuit,setWlCuit]=useState("");
  const[wlLabel,setWlLabel]=useState("");

  const loadAll=async()=>{
    setL(true);
    const [s,e,r,w,p,st]=await Promise.all([
      GET("/api/alerts/stats"),
      GET("/api/alerts/events?limit=60"),
      GET("/api/alerts/runs?limit=20"),
      GET("/api/alerts/whitelist"),
      GET("/api/alerts/progress"),
      GET("/api/alerts/settings"),
    ]);
    setS(s||{});
    setEv(e?.events||[]);
    setRuns(r?.runs||[]);
    setWL(w?.items||[]);
    setProg(p?.status&&p.status!=="idle"?p:null);
    setSettings({
      local_workers: String(st?.local_workers ?? 1),
      lambda_enabled: !!st?.lambda_enabled,
      lambda_workers: String(st?.lambda_workers ?? 0),
      lambda_region: st?.lambda_region || "",
      lambda_function_name: st?.lambda_function_name || "",
    });
    setL(false);
  };
  useEffect(()=>{loadAll();},[]);

  const stopPoll=()=>{ if(pollRef.current){ clearInterval(pollRef.current); pollRef.current=null; } };
  const startPoll=()=>{
    if(pollRef.current) return;
    pollRef.current=setInterval(async()=>{
      const p=await GET("/api/alerts/progress");
      setProg(p?.status&&p.status!=="idle"?p:null);
      if(!p?.status||p?.status==="done"||p?.status==="error"||p?.status==="idle"){
        stopPoll();
        await loadAll();
      }
    },2000);
  };
  useEffect(()=>()=>stopPoll(),[]);
  useEffect(()=>{ if(progress?.status==="running") startPoll(); },[progress?.status]);

  const run=async alerts=>{
    const r=await POST(alerts?"/api/alerts/run":"/api/alerts/sync",{scope:"all"});
    notify?.(r?.message||r?.error||"OK",r?.ok===false?"error":"ok");
    if(r?.ok!==false) startPoll();
  };

  const refreshWL=async()=>{
    const w=await GET("/api/alerts/whitelist");
    setWL(w?.items||[]);
    const s=await GET("/api/alerts/stats");
    setS(s||{});
  };
  const addWL=async()=>{
    if(!wlCuit.trim()) return;
    const r=await POST("/api/alerts/whitelist",{cuit2:wlCuit,label:wlLabel});
    notify?.(r?.message||r?.error||"OK",r?.ok===false?"error":"ok");
    if(r?.ok!==false){ setWlCuit(""); setWlLabel(""); await refreshWL(); }
  };
  const rmWL=async id=>{
    const r=await DEL("/api/alerts/whitelist/"+id);
    notify?.(r?.message||r?.error||"OK",r?.ok===false?"error":"ok");
    if(r?.ok!==false) await refreshWL();
  };
  const saveSettings=async()=>{
    const r=await PUT("/api/alerts/settings",settings);
    notify?.(r?.message||r?.error||"OK",r?.ok===false?"error":"ok");
    if(r?.ok!==false){
      setSettings({
        local_workers:String(r.local_workers ?? settings.local_workers),
        lambda_enabled:!!r.lambda_enabled,
        lambda_workers:String(r.lambda_workers ?? settings.lambda_workers),
        lambda_region:r.lambda_region||"",
        lambda_function_name:r.lambda_function_name||"",
      });
    }
  };
  const resetRuntime=async()=>{
    const r=await POST("/api/alerts/runtime/reset",{});
    notify?.(r?.message||r?.error||"OK",r?.ok===false?"error":"ok");
    if(r?.ok!==false){ setProg(null); await loadAll(); }
  };

  const eventsCols=useMemo(()=>[
    {header:"Detectado",exportHeader:"Detectado",render:r=>fmtDT(r.detected_at),exportValue:r=>fmtDT(r.detected_at),mono:true},
    {header:"Fecha rechazo",exportHeader:"Fecha rechazo",render:r=>fmtDate(r.fecha_rechazo),exportValue:r=>fmtDate(r.fecha_rechazo)},
    {header:"Firmante",exportHeader:"Firmante",render:r=>h`<div><div style=${{fontWeight:500}}>${r.razon_social||"—"}</div><div style=${{fontSize:"11px",color:T.t.m,fontFamily:T.f.m}}>${r.cuit_digits}</div></div>`,exportValue:r=>`${r.razon_social||""} (${r.cuit_digits||""})`,wrap:true},
    {header:"Cheque",exportHeader:"Cheque",render:r=>"#"+(r.nro_cheque||"—"),exportValue:r=>r.nro_cheque||"—",mono:true},
    {header:"Monto",exportHeader:"Monto",render:r=>fmtARS(r.monto||0),exportValue:r=>Number(r.monto||0),align:"right",mono:true},
    {header:"Causal",exportHeader:"Causal",render:r=>h`<span style=${{fontSize:"12px"}}>${r.causal||"SIN FONDOS"}</span>`,exportValue:r=>r.causal||"SIN FONDOS"},
    {header:"Estado",exportHeader:"Estado",render:r=>h`<${Badge} variant=${r.pagado?"success":"danger"} size="xs">${r.pagado?"Pagado":"Impago"}<//>`,exportValue:r=>r.pagado?"Pagado":"Impago"},
    {header:"Notif",exportHeader:"Notificado",render:r=>r.notified?h`<${Badge} variant="success" size="xs">✓<//>`:h`<${Badge} variant="default" size="xs">—<//>`,exportValue:r=>r.notified?"Sí":"No"},
  ],[themeName]);

  const runsCols=useMemo(()=>[
    {header:"Inicio",exportHeader:"Inicio",render:r=>fmtDT(r.started_at),exportValue:r=>fmtDT(r.started_at),mono:true},
    {header:"Fin",exportHeader:"Fin",render:r=>fmtDT(r.ended_at),exportValue:r=>fmtDT(r.ended_at),mono:true},
    {header:"Tipo",exportHeader:"Tipo",render:r=>h`<${Badge} size="xs">${r.run_type}<//>`,exportValue:r=>r.run_type},
    {header:"CUITs",exportHeader:"CUITs",render:r=>String(r.total_cuits||0),exportValue:r=>Number(r.total_cuits||0),align:"right"},
    {header:"Nuevos",exportHeader:"Nuevos",render:r=>h`<span style=${{color:r.new_events>0?T.t.w:T.t.s,fontWeight:r.new_events>0?700:400}}>${r.new_events||0}</span>`,exportValue:r=>Number(r.new_events||0),align:"right"},
    {header:"Alertas",exportHeader:"Alertas",render:r=>String(r.alerts_sent||0),exportValue:r=>Number(r.alerts_sent||0),align:"right"},
    {header:"Errores",exportHeader:"Errores",render:r=>h`<span style=${{color:r.errors>0?T.t.err:T.t.s}}>${r.errors||0}</span>`,exportValue:r=>Number(r.errors||0),align:"right"},
    {header:"Notas",exportHeader:"Notas",render:r=>h`<span style=${{fontSize:"11px",color:T.t.m}}>${r.notes||""}</span>`,exportValue:r=>r.notes||"",wrap:true,maxW:"280px"},
  ],[themeName]);

  const whitelistCols=useMemo(()=>[
    {header:"CUIT2",exportHeader:"CUIT2",render:r=>h`<span style=${{fontFamily:T.f.m}}>${r.cuit2_digits}</span>`,exportValue:r=>r.cuit2_digits},
    {header:"Etiqueta",exportHeader:"Etiqueta",render:r=>r.label||"—",exportValue:r=>r.label||""},
    {header:"Agregado por",exportHeader:"Agregado por",render:r=>r.created_by||"—",exportValue:r=>r.created_by||""},
    {header:"Fecha",exportHeader:"Fecha",render:r=>fmtDT(r.created_at),exportValue:r=>fmtDT(r.created_at)},
    {header:"",exportHeader:"",render:r=>h`<${Btn} onClick=${()=>rmWL(r.id)} size="xs" variant="danger">Quitar<//>`},
  ],[themeName]);

  if(loading) return h`<${Loader}/>`;
  const isRunning=progress&&progress.status==="running";
  const pct=isRunning&&progress.total>0?Math.round(progress.done/progress.total*100):0;

  return h`<div style=${{display:"flex",flexDirection:"column",gap:"18px"}}>
    <${SectionTitle} title="Alertas — Cheques rechazados" sub="Monitoreo BCRA · Sin fondos">
      <${Btn} variant="primary" size="sm" onClick=${()=>run(true)} disabled=${isRunning}>Ejecutar monitoreo<//>
      <${Btn} size="sm" onClick=${()=>run(false)} disabled=${isRunning}>Sync sin alertas<//>
    <//>

    ${progress?h`<${Card}><div style=${{display:"flex",justifyContent:"space-between",fontSize:"13px",marginBottom:"8px"}}><span style=${{color:T.t.a}}>${isRunning?`Ejecutando… ${progress.done}/${progress.total}`:(progress.status==="error"?"Corrida con error":"Última corrida finalizada")}</span><span style=${{color:T.t.m}}>${isRunning?`${pct}%`:""}</span></div>${isRunning?h`<${ProgressBar} pct=${pct} color=${T.c.blue} />`:null}${progress.message?h`<div style=${{fontSize:"12px",color:T.t.m,marginTop:"6px"}}>${progress.message}</div>`:null}<//>`:null}

    <div style=${{display:"flex",gap:"12px",flexWrap:"wrap"}}>
      <${KPICard} label="Monitoreables" value=${String(stats.monitoreables||0)} />
      <${KPICard} label="Eventos totales" value=${String(stats.total_events||0)} variant="warning" />
      <${KPICard} label="Última corrida" value=${stats.last_run||"—"} />
      <${KPICard} label="Whitelist CUIT2" value=${String(stats.whitelist_count??whitelist.length)} />
    </div>

    <${TabBar} tabs=${[
      {id:"events",label:"Últimos eventos"},
      {id:"runs",label:"Historial corridas"},
      {id:"whitelist",label:"Whitelist CUIT2"},
      {id:"settings",label:"Infra / workers"},
    ]} active=${tab} onChange=${setTab} />

    ${tab==="events"?h`<${Card} style=${{overflow:"hidden"}}><${Tbl} columns=${eventsCols} data=${events} maxH="55vh" exportFileName="alertas-eventos.xlsx" exportSheetName="Eventos" onExportResult=${ok=>notify?.(ok?"Excel exportado.":"No se pudo exportar el Excel.",ok?"ok":"error")} /><//>`:null}

    ${tab==="runs"?h`<${Card} style=${{overflow:"hidden"}}><${Tbl} columns=${runsCols} data=${runs} exportFileName="alertas-corridas.xlsx" exportSheetName="Corridas" onExportResult=${ok=>notify?.(ok?"Excel exportado.":"No se pudo exportar el Excel.",ok?"ok":"error")} /><//>`:null}

    ${tab==="whitelist"?h`<${Card}>
      <p style=${{fontSize:"13px",color:T.t.s,marginBottom:"14px"}}>Los firmantes consultados solo por cartera para estos CUIT2 quedan silenciados en Slack, pero igual se consultan y se registran en el CRM.</p>
      <div style=${{display:"flex",gap:"8px",alignItems:"center",marginBottom:"16px",flexWrap:"wrap"}}>
        <${Inp} value=${wlCuit} onChange=${setWlCuit} placeholder="CUIT2 a silenciar" style=${{maxWidth:"220px"}} />
        <${Inp} value=${wlLabel} onChange=${setWlLabel} placeholder="Etiqueta (opcional)" style=${{maxWidth:"260px"}} />
        <${Btn} onClick=${addWL} variant="primary" size="sm">Agregar<//>
      </div>
      ${whitelist.length===0?h`<p style=${{color:T.t.m,fontSize:"13px"}}>Whitelist vacía — no hay CUIT2 silenciados.</p>`:h`<${Tbl} columns=${whitelistCols} data=${whitelist} exportFileName="alertas-whitelist-cuit2.xlsx" exportSheetName="Whitelist" onExportResult=${ok=>notify?.(ok?"Excel exportado.":"No se pudo exportar el Excel.",ok?"ok":"error")} />`}
    <//>`:null}

    ${tab==="settings"?h`<${Card}>
      <div style=${{display:"grid",gridTemplateColumns:"repeat(auto-fit,minmax(220px,1fr))",gap:"12px"}}>
        <div>
          <label style=${{fontSize:"11px",color:T.t.m,display:"block",marginBottom:"4px"}}>Workers locales</label>
          <${Inp} value=${settings.local_workers} onChange=${v=>setSettings(s=>({...s,local_workers:v}))} placeholder="1" />
        </div>
        <div>
          <label style=${{fontSize:"11px",color:T.t.m,display:"block",marginBottom:"4px"}}>Workers Lambda</label>
          <${Inp} value=${settings.lambda_workers} onChange=${v=>setSettings(s=>({...s,lambda_workers:v}))} placeholder="2" />
        </div>
        <div>
          <label style=${{fontSize:"11px",color:T.t.m,display:"block",marginBottom:"4px"}}>AWS region</label>
          <${Inp} value=${settings.lambda_region} onChange=${v=>setSettings(s=>({...s,lambda_region:v}))} placeholder="sa-east-1" />
        </div>
        <div>
          <label style=${{fontSize:"11px",color:T.t.m,display:"block",marginBottom:"4px"}}>Lambda function</label>
          <${Inp} value=${settings.lambda_function_name} onChange=${v=>setSettings(s=>({...s,lambda_function_name:v}))} placeholder="meridiano-bcra-monitor" />
        </div>
      </div>
      <div style=${{display:"flex",alignItems:"center",gap:"8px",marginTop:"12px",flexWrap:"wrap"}}>
        <label style=${{display:"flex",alignItems:"center",gap:"6px",fontSize:"13px",color:T.t.s,cursor:"pointer"}}><input type="checkbox" checked=${!!settings.lambda_enabled} onChange=${e=>setSettings(s=>({...s,lambda_enabled:e.target.checked}))} /> Habilitar ruteo Lambda</label>
        <${Badge} size="xs" variant=${settings.lambda_enabled?"blue":"default"}>${settings.lambda_enabled?"Preparado para offload":"Solo local"}<//>
      </div>
      <div style=${{marginTop:"12px",padding:"12px",borderRadius:T.r.md,background:T.bg.accent,border:"1px solid "+T.bd.s,color:T.t.s,fontSize:"13px",lineHeight:1.5}}>
        La UI ya te deja administrar la configuración de workers y Lambda. En esta etapa queda persistido el setup y el monitoreo ya toma el valor de <b>workers locales</b>; el offload efectivo a AWS Lambda queda preparado para la siguiente iteración backend.
      </div>
      <div style=${{display:"flex",gap:"8px",marginTop:"14px",flexWrap:"wrap"}}>
        <${Btn} onClick=${saveSettings} variant="primary">Guardar configuración<//>
        <${Btn} onClick=${resetRuntime} variant="ghost">Resetear estado del monitor<//>
      </div>
    <//>`:null}
  </div>`;
}

export {AlertsPage};
