import {h} from "./deps.js";
import {T} from "./theme.js";

const ease = "background .32s cubic-bezier(.22,1,.36,1), background-color .32s cubic-bezier(.22,1,.36,1), color .24s ease, border-color .32s cubic-bezier(.22,1,.36,1), box-shadow .34s cubic-bezier(.22,1,.36,1), transform .2s ease";

function badgeMap(){
  return {
    default:{bg:T.bg.input,bd:T.bd.s,c:T.t.s},
    success:{bg:T.c.greenG,bd:"1px solid rgba(16,185,129,0.24)",c:T.t.ok},
    danger:{bg:T.c.redG,bd:"1px solid rgba(239,68,68,0.2)",c:T.t.err},
    warning:{bg:T.c.amberG,bd:"1px solid rgba(245,158,11,0.22)",c:T.t.w},
    blue:{bg:T.c.blueG,bd:"1px solid rgba(59,130,246,0.22)",c:T.t.a},
    primera:{bg:T.c.greenG,bd:"1px solid rgba(16,185,129,0.24)",c:T.t.ok},
  };
}

function Badge({children,variant="default",size="sm"}){
  const c=badgeMap()[variant]||badgeMap().default;
  return h`<span style=${{
    display:"inline-flex",alignItems:"center",padding:size==="xs"?"3px 7px":"5px 10px",borderRadius:T.r.pill,
    fontSize:size==="xs"?"10px":"11px",fontWeight:700,letterSpacing:"0.02em",
    background:c.bg,border:typeof c.bd==="string"&&c.bd.startsWith("1px")?c.bd:"1px solid "+c.bd,color:c.c,whiteSpace:"nowrap",
    boxShadow:T.shadow.inset,backdropFilter:"blur(10px)",transition:ease
  }}>${children}</span>`;
}

function Btn({children,onClick,type="button",variant="default",size="md",disabled,style:xs={}}){
  const base={
    padding:size==="sm"?"8px 12px":size==="xs"?"6px 9px":"11px 15px",
    borderRadius:T.r.md,cursor:disabled?"not-allowed":"pointer",fontSize:size==="sm"||size==="xs"?"12px":"13px",
    fontWeight:700,border:"1px solid transparent",display:"inline-flex",alignItems:"center",justifyContent:"center",gap:"7px",
    opacity:disabled?.5:1,fontFamily:T.f.s,backdropFilter:"blur(12px)",boxShadow:T.shadow.inset,transition:ease
  };
  const V={
    default:{...base,background:T.bg.surface,color:T.t.p,border:"1px solid "+T.bd.d,boxShadow:T.shadow.glow},
    primary:{...base,background:`linear-gradient(135deg, ${T.c.blue2} 0%, ${T.c.blue} 100%)`,color:T.t.inv,border:"1px solid rgba(255,255,255,0.08)",boxShadow:"0 14px 30px rgba(46,102,240,0.26)"},
    danger:{...base,background:T.c.redG,color:T.t.err,border:"1px solid rgba(239,68,68,0.2)"},
    ghost:{...base,background:"transparent",color:T.t.s,border:"1px solid "+T.bd.s},
    success:{...base,background:`linear-gradient(135deg, ${T.c.green2} 0%, ${T.c.green} 100%)`,color:T.t.inv,border:"1px solid rgba(255,255,255,0.08)",boxShadow:"0 14px 30px rgba(0,169,125,0.2)"},
  };
  return h`<button disabled=${disabled} type=${type} onClick=${disabled?undefined:onClick} style=${{...(V[variant]||V.default),...xs}}>${children}</button>`;
}

function Inp({value="",onChange,placeholder="",type="text",style={}}){
  return h`<input type=${type} value=${value} placeholder=${placeholder} onInput=${e=>onChange?.(e.target.value)} style=${{
    width:"100%",padding:"11px 13px",borderRadius:T.r.md,border:"1px solid "+T.bd.d,background:T.bg.input,color:T.t.p,
    outline:"none",fontSize:"13px",fontFamily:T.f.s,boxShadow:T.shadow.inset,backdropFilter:"blur(8px)",transition:ease,...style
  }} />`;
}

function Sel({value,onChange,options,style={}}){
  return h`<select value=${value} onChange=${e=>onChange?.(e.target.value)} style=${{
    padding:"11px 13px",borderRadius:T.r.md,border:"1px solid "+T.bd.d,background:T.bg.input,color:T.t.p,fontSize:"13px",
    outline:"none",fontFamily:T.f.s,boxShadow:T.shadow.inset,backdropFilter:"blur(8px)",transition:ease,...style
  }}>${options.map(o=>h`<option key=${o.value} value=${o.value}>${o.label}</option>`)}</select>`;
}

function SearchInput({value,onChange,placeholder,onSubmit,style={}}){
  return h`<div style=${{position:"relative",minWidth:"220px",flex:"1 1 260px",...style}}>
    <span style=${{position:"absolute",left:"12px",top:"50%",transform:"translateY(-50%)",color:T.t.m}}>⌕</span>
    <input value=${value} onInput=${e=>onChange?.(e.target.value)} onKeyDown=${e=>{if(e.key==="Enter")onSubmit?.();}} placeholder=${placeholder} style=${{
      width:"100%",padding:"11px 13px 11px 34px",borderRadius:T.r.md,border:"1px solid "+T.bd.d,background:T.bg.input,color:T.t.p,
      outline:"none",fontSize:"13px",fontFamily:T.f.s,boxShadow:T.shadow.inset,backdropFilter:"blur(8px)",transition:ease
    }} />
  </div>`;
}

function Card({children,style={}}){
  return h`<div style=${{
    background:T.bg.card,border:"1px solid "+T.bd.s,borderRadius:T.r.lg,padding:"18px",boxShadow:T.shadow.glow,
    backdropFilter:"blur(18px)",position:"relative",overflow:"hidden",transition:ease,...style
  }}>${children}</div>`;
}

function KPICard({label,value,sub,variant}){
  const c=variant==="danger"?T.t.err:variant==="success"?T.t.ok:variant==="warning"?T.t.w:T.t.p;
  const accent=variant==="danger"?T.c.redG:variant==="success"?T.c.greenG:variant==="warning"?T.c.amberG:T.c.blueG;
  return h`<div style=${{
    flex:"1 1 180px",minWidth:"180px",padding:"18px",borderRadius:T.r.lg,background:T.bg.card,border:"1px solid "+T.bd.s,
    boxShadow:T.shadow.glow,position:"relative",overflow:"hidden",backdropFilter:"blur(16px)",transition:ease
  }}>
    <div style=${{position:"absolute",inset:"0 auto auto 0",width:"100%",height:"3px",background:accent,opacity:.95}}></div>
    <div style=${{fontSize:"11px",color:T.t.m,textTransform:"uppercase",letterSpacing:"0.1em",fontWeight:700}}>${label}</div>
    <div style=${{fontSize:"24px",fontWeight:800,color:c,marginTop:"10px",lineHeight:1.05}}>${value}</div>
    ${sub?h`<div style=${{fontSize:"11px",color:T.t.m,marginTop:"8px"}}>${sub}</div>`:null}
  </div>`;
}

function _exportHeader(columns,i){
  const col=columns[i]||{};
  const raw=col.exportHeader ?? col.header;
  if(typeof raw==="string"||typeof raw==="number") return String(raw).trim();
  return "Columna "+(i+1);
}
function _exportCell(col,row){
  try{
    if(typeof col.exportValue==="function") return col.exportValue(row);
    if(col.exportKey) return row?.[col.exportKey];
    if(col.key) return row?.[col.key];
    if(typeof col.render==="function"){
      const v=col.render(row);
      return (typeof v==="string"||typeof v==="number"||typeof v==="boolean")?v:"";
    }
  }catch(_){ return ""; }
  return "";
}
async function exportTableToExcel({filename="export.xlsx",sheetName="Datos",columns=[],data=[]}){
  const XLSX = await import("https://esm.sh/xlsx@0.18.5");
  const exportable=(columns||[]).filter((c,i)=>String(_exportHeader(columns,i)||"").trim()!=="");
  const rows=(data||[]).map(row=>{
    const out={};
    exportable.forEach((col,i)=>{ out[_exportHeader(exportable,i)] = _exportCell(col,row) ?? ""; });
    return out;
  });
  const ws=XLSX.utils.json_to_sheet(rows.length?rows:[{}]);
  const wb=XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb,ws,(sheetName||"Datos").slice(0,31)||"Datos");
  XLSX.writeFile(wb, filename.endsWith(".xlsx")?filename:`${filename}.xlsx`);
}

function Tbl({columns,data,empty="Sin datos",maxH,exportFileName="",exportSheetName="Datos",onExportResult}){
  const canExport=!!exportFileName;
  const doExport=async()=>{
    try{
      await exportTableToExcel({filename:exportFileName,sheetName:exportSheetName,columns,data});
      onExportResult?.(true);
    }catch(e){
      console.error(e);
      onExportResult?.(false,e);
    }
  };
  return h`<div>
    ${canExport?h`<div style=${{display:"flex",justifyContent:"flex-end",marginBottom:"12px"}}><${Btn} size="xs" variant="ghost" onClick=${doExport}>Exportar Excel<//></div>`:null}
    <div style=${{overflowX:"auto",maxHeight:maxH||undefined,overflowY:maxH?"auto":undefined,border:"1px solid "+T.bd.s,borderRadius:T.r.lg,background:T.bg.panel,boxShadow:T.shadow.inset,backdropFilter:"blur(16px)",transition:ease}}>
      <table style=${{width:"100%",borderCollapse:"separate",borderSpacing:0,fontSize:"13px"}}>
        <thead><tr>${columns.map((c,i)=>h`<th key=${i} style=${{textAlign:c.align||"left",padding:"12px 14px",fontSize:"11px",fontWeight:800,color:T.t.m,textTransform:"uppercase",letterSpacing:"0.08em",borderBottom:"1px solid "+T.bd.s,whiteSpace:"nowrap",position:"sticky",top:0,background:T.bg.tableHead,zIndex:1,backdropFilter:"blur(14px)",transition:ease}}>${c.header}</th>`)}</tr></thead>
        <tbody>${!data||!data.length?h`<tr><td colspan=${columns.length} style=${{padding:"22px 14px",color:T.t.m}}>${empty}</td></tr>`:data.map((row,i)=>h`<tr key=${i} onMouseEnter=${e=>{e.currentTarget.style.background=T.bg.hover;e.currentTarget.style.transform="translateY(-1px)";}} onMouseLeave=${e=>{e.currentTarget.style.background="transparent";e.currentTarget.style.transform="translateY(0)";}}>${columns.map((c,j)=>h`<td key=${j} style=${{padding:"12px 14px",borderBottom:"1px solid "+T.bd.s,textAlign:c.align||"left",whiteSpace:c.wrap?"normal":"nowrap",verticalAlign:"top",fontFamily:c.mono?T.f.m:T.f.s,maxWidth:c.maxW||undefined,overflow:"hidden",textOverflow:"ellipsis",transition:ease}}>${c.render?c.render(row):row[c.key]}</td>`)}</tr>`)}</tbody>
      </table>
    </div>
  </div>`;
}

function Modal({open,onClose,title,children,width="520px"}){
  if(!open)return null;
  return h`<div onClick=${onClose} style=${{position:"fixed",inset:0,zIndex:900,background:T.bg.overlay,display:"flex",alignItems:"center",justifyContent:"center",padding:"20px",backdropFilter:"blur(16px)",transition:ease}}>
    <div onClick=${e=>e.stopPropagation()} style=${{width,maxWidth:"95vw",maxHeight:"90vh",overflowY:"auto",background:T.bg.card,border:"1px solid "+T.bd.strong,borderRadius:T.r.lg,padding:"24px",boxShadow:T.shadow.pop,backdropFilter:"blur(18px)",transition:ease}}>
      <div style=${{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:"18px"}}><h3 style=${{margin:0,fontSize:"16px",fontWeight:700}}>${title}</h3><button onClick=${onClose} style=${{background:"transparent",border:"none",color:T.t.m,cursor:"pointer",fontSize:"18px",padding:"4px",transition:ease}}>✕</button></div>
      ${children}
    </div>
  </div>`;
}

function TabBar({tabs,active,onChange}){
  return h`<div style=${{display:"flex",gap:"3px",background:T.bg.surface,borderRadius:T.r.md,padding:"4px",border:"1px solid "+T.bd.s,backdropFilter:"blur(14px)",boxShadow:T.shadow.inset,transition:ease}}>${tabs.map(t=>h`<button key=${t.id} onClick=${()=>onChange(t.id)} style=${{padding:"9px 14px",borderRadius:T.r.sm,border:"1px solid transparent",background:active===t.id?T.bg.accent:"transparent",color:active===t.id?T.t.p:T.t.s,cursor:"pointer",fontSize:"12px",fontWeight:active===t.id?700:500,fontFamily:T.f.s,boxShadow:active===t.id?T.shadow.inset:"none",transition:ease}}>${t.label}</button>`)}</div>`;
}

function ProgressBar({pct,color}){
  const p=Math.min(Math.max(pct||0,0),100);
  return h`<div style=${{height:"8px",borderRadius:"999px",background:T.bg.input,overflow:"hidden",boxShadow:T.shadow.inset,transition:ease}}><div style=${{height:"100%",width:p+"%",borderRadius:"999px",background:color||`linear-gradient(135deg, ${T.c.blue2} 0%, ${T.c.blue} 100%)`,transition:"width .35s cubic-bezier(.22,1,.36,1), background .35s ease"}}/></div>`;
}

function Toast({toast,onClose}){
  if(!toast)return null;const isErr=toast.kind==="error";
  return h`<div onClick=${onClose} style=${{position:"fixed",right:"18px",bottom:"18px",zIndex:1000,maxWidth:"420px",padding:"13px 15px",background:isErr?T.c.redG:T.c.greenG,border:"1px solid "+(isErr?"rgba(239,68,68,0.2)":"rgba(16,185,129,0.2)"),borderRadius:T.r.md,color:isErr?T.t.err:T.t.ok,fontSize:"13px",boxShadow:T.shadow.glow,cursor:"pointer",backdropFilter:"blur(14px)",transition:ease}}>${toast.msg}</div>`;
}

function Loader(){return h`<div style=${{padding:"60px",textAlign:"center",color:T.t.m}}>Cargando…</div>`;}

function SectionTitle({title,sub,children}){
  return h`<div style=${{display:"flex",justifyContent:"space-between",alignItems:"flex-end",gap:"12px",flexWrap:"wrap"}}><div><h2 style=${{fontSize:"24px",fontWeight:800,margin:0,letterSpacing:"-0.02em"}}>${title}</h2>${sub?h`<p style=${{fontSize:"13px",color:T.t.m,margin:"6px 0 0"}}>${sub}</p>`:null}</div>${children?h`<div style=${{display:"flex",gap:"8px",alignItems:"center",flexWrap:"wrap"}}>${children}</div>`:null}</div>`;
}

export {Badge,Btn,Inp,Sel,SearchInput,Card,KPICard,Tbl,Modal,TabBar,ProgressBar,Toast,Loader,SectionTitle,exportTableToExcel};
