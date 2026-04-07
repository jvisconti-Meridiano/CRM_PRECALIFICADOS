import {h,useMemo,useState} from "../deps.js";
import {T,ThemeToggle} from "../theme.js";
import {POST} from "../api.js";

const BASE_NAV=[
  {id:"dashboard",label:"Dashboard",icon:"◫"},
  {id:"firmantes",label:"Firmantes",icon:"◩"},
  {id:"grupos",label:"Grupos",icon:"◧"},
  {id:"cartera",label:"Cartera",icon:"▤"},
  {id:"politica",label:"Política",icon:"◈"},
  {id:"audit",label:"Auditoría",icon:"◆"},
  {id:"alertas",label:"Alertas",icon:"▲"},
];

function Sidebar({active,onNavigate,collapsed,onToggle,user,pendingApprovals,themeName,onThemeChange}){
  const [hovered,setHovered]=useState(false);
  const expanded=!collapsed||hovered;
  const nav=useMemo(()=>{
    const b=[...BASE_NAV];
    if(user?.role==="admin"){
      b.push({id:"approvals",label:"Aprobaciones",icon:"✓",badge:pendingApprovals||0});
      b.push({id:"users",label:"Usuarios",icon:"◎"});
    }
    return b;
  },[user,pendingApprovals]);

  return h`<nav onMouseEnter=${()=>setHovered(true)} onMouseLeave=${()=>setHovered(false)} style=${{
    width:expanded?"244px":"78px",minHeight:"100vh",background:T.bg.shell,borderRight:"1px solid "+T.bd.s,display:"flex",
    flexDirection:"column",transition:"width .26s cubic-bezier(.22,1,.36,1), background .32s ease, border-color .32s ease, box-shadow .32s ease",
    position:"sticky",top:0,flexShrink:0,overflow:"hidden",backdropFilter:"blur(18px)",boxShadow:T.shadow.glow
  }}>
    <div onClick=${onToggle} style=${{padding:collapsed?"18px 16px":"18px 18px",display:"flex",alignItems:"center",gap:"12px",cursor:"pointer",borderBottom:"1px solid "+T.bd.s,minHeight:"74px"}}>
      <div style=${{width:"38px",height:"38px",borderRadius:"12px",background:T.bg.accent,border:"1px solid "+T.bd.s,display:"grid",placeItems:"center",boxShadow:T.shadow.inset,flexShrink:0}}>
        <img src="/static/logo.png" alt="" style=${{width:"24px",height:"24px",objectFit:"contain"}} />
      </div>
      ${expanded?h`<div style=${{overflow:"hidden"}}><div style=${{fontWeight:800,fontSize:"14px",whiteSpace:"nowrap"}}>Meridiano</div><div style=${{fontSize:"10px",color:T.t.m,letterSpacing:"0.12em",whiteSpace:"nowrap"}}>CRM CREDITICIO</div></div>`:null}
    </div>
    <div style=${{padding:"12px 8px",display:"flex",flexDirection:"column",gap:"6px",flex:1}}>
      ${nav.map(item=>{
        const isActive=active===item.id;
        return h`<button key=${item.id} onClick=${()=>onNavigate(item.id)} style=${{
          display:"flex",alignItems:"center",gap:"12px",padding:"11px 13px",background:isActive?T.bg.accent:"transparent",border:isActive?"1px solid "+T.bd.d:"1px solid transparent",
          borderRadius:T.r.md,color:isActive?T.t.p:T.t.s,cursor:"pointer",textAlign:"left",fontFamily:T.f.s,fontSize:"13px",
          whiteSpace:"nowrap",overflow:"hidden",position:"relative",boxShadow:isActive?T.shadow.inset:"none"
        }} onMouseEnter=${e=>{if(!isActive)e.currentTarget.style.background=T.bg.hover;}} onMouseLeave=${e=>{if(!isActive)e.currentTarget.style.background="transparent";}}>
          <span style=${{width:"20px",height:"20px",display:"grid",placeItems:"center",flexShrink:0,fontSize:"14px",borderRadius:"8px",background:isActive?T.bg.input:"transparent"}}>${item.icon}</span>
          ${expanded?item.label:null}
          ${expanded&&item.badge>0?h`<span style=${{marginLeft:"auto",background:T.c.red,color:T.t.inv,borderRadius:T.r.pill,padding:"2px 7px",fontSize:"10px",fontWeight:800,boxShadow:T.shadow.inset}}>${item.badge}</span>`:null}
          ${!expanded&&item.badge>0?h`<span style=${{position:"absolute",top:"4px",right:"6px",width:"8px",height:"8px",borderRadius:"50%",background:T.c.red}}/>`:null}
        </button>`;
      })}
    </div>
    <div style=${{padding:"14px 12px",borderTop:"1px solid "+T.bd.s,display:"grid",gap:"12px",background:T.bg.shell2}}>
      <div style=${{display:"flex",justifyContent:expanded?"flex-start":"center"}}><${ThemeToggle} themeName=${themeName} onChange=${onThemeChange} compact=${true} /></div>
      <div style=${{padding:expanded?"12px":"8px",borderRadius:T.r.md,background:T.bg.input,border:"1px solid "+T.bd.s,boxShadow:T.shadow.inset}}>
        <div style=${{fontSize:"12px",fontWeight:800,color:T.t.p}}>${collapsed?(user?.username||"").slice(0,2).toUpperCase():user?.username}</div>
        ${expanded?h`<div style=${{fontSize:"10px",color:T.t.m,marginTop:"3px",letterSpacing:"0.08em",textTransform:"uppercase"}}>${user?.role_label||""}</div>`:null}
        ${expanded?h`<button onClick=${async()=>{await POST("/api/auth/logout",{});window.__setAuth?.(null);}} style=${{marginTop:"10px",background:"transparent",border:"none",padding:0,color:T.t.m,cursor:"pointer",fontSize:"12px",fontFamily:T.f.s,fontWeight:700}}>Cerrar sesión</button>`:null}
      </div>
    </div>
  </nav>`;
}

export {Sidebar};
