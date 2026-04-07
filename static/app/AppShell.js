import {h,useState,useEffect,useCallback} from "./deps.js";
import {T,getStoredTheme,applyTheme} from "./theme.js";
import {GET} from "./api.js";
import {Toast} from "./ui.js";
import {LoginPage} from "./pages/LoginPage.js";
import {DashboardPage} from "./pages/DashboardPage.js";
import {FirmantesPage} from "./pages/FirmantesPage.js";
import {GruposPage} from "./pages/GruposPage.js";
import {CarteraPage} from "./pages/CarteraPage.js";
import {PoliticaPage} from "./pages/PoliticaPage.js";
import {AuditPage} from "./pages/AuditPage.js";
import {AlertsPage} from "./pages/AlertsPage.js";
import {ApprovalsPage} from "./pages/ApprovalsPage.js";
import {UsersPage} from "./pages/UsersPage.js";
import {Sidebar} from "./layout/Sidebar.js";

function App(){
  const[user,setUser]=useState(undefined);
  const[page,setPage]=useState("dashboard");
  const[collapsed,setCollapsed]=useState(false);
  const[toast,setToast]=useState(null);
  const[pendingApprovals,setPending]=useState(0);
  const[themeName,setThemeNameState]=useState(()=>applyTheme(getStoredTheme()));
  const setThemeName=useCallback((nextName)=>{
    const applied=applyTheme(nextName);
    setThemeNameState(applied);
  },[]);

  window.__setAuth=setUser;

  const notify=(msg,kind="ok")=>{
    setToast({msg,kind});
    clearTimeout(window.__tt);
    window.__tt=setTimeout(()=>setToast(null),4000);
  };

  useEffect(()=>{
    GET("/api/auth/me").then(r=>{
      r?.authenticated
        ? setUser({username:r.username,role:r.role,role_label:r.role_label})
        : setUser(null);
    }).catch(()=>setUser(null));
  },[]);

  useEffect(()=>{
    if(!user||user.role!=="admin") return;
    const ck=()=>GET("/api/approvals/pending-count").then(r=>setPending(r?.count||0)).catch(()=>{});
    ck();
    const iv=setInterval(ck,30000);
    return ()=>clearInterval(iv);
  },[user]);

  useEffect(()=>{
    if(user&&user.role!=="admin"&&(page==="users"||page==="approvals")) setPage("dashboard");
  },[user,page]);

  if(user===undefined){
    return h`<div style=${{minHeight:"100vh",display:"flex",alignItems:"center",justifyContent:"center",color:T.t.m,background:T.bg.base}}>Cargando…</div>`;
  }

  if(!user){
    return h`<${LoginPage} onLogin=${setUser} themeName=${themeName} onThemeChange=${setThemeName} />`;
  }

  const pages={
    dashboard:h`<${DashboardPage} themeName=${themeName} />`,
    firmantes:h`<${FirmantesPage} user=${user} notify=${notify} themeName=${themeName} />`,
    grupos:h`<${GruposPage} user=${user} notify=${notify} themeName=${themeName} />`,
    audit:h`<${AuditPage} user=${user} notify=${notify} themeName=${themeName} />`,
    alertas:h`<${AlertsPage} user=${user} notify=${notify} themeName=${themeName} />`,
    politica:h`<${PoliticaPage} user=${user} notify=${notify} themeName=${themeName} />`,
    cartera:h`<${CarteraPage} themeName=${themeName} notify=${notify} />`,
    approvals:user.role==="admin"?h`<${ApprovalsPage} user=${user} notify=${notify} themeName=${themeName} />`:null,
    users:user.role==="admin"?h`<${UsersPage} themeName=${themeName} notify=${notify} />`:null,
  };

  return h`<div style=${{minHeight:"100vh",display:"flex",background:T.bg.base,color:T.t.p,fontFamily:T.f.s}}>
    <${Sidebar}
      active=${page}
      onNavigate=${p=>{setPage(p);if(p==="approvals")setPending(0);}}
      collapsed=${collapsed}
      onToggle=${()=>setCollapsed(v=>!v)}
      user=${user}
      pendingApprovals=${pendingApprovals}
      themeName=${themeName}
      onThemeChange=${setThemeName}
    />
    <main style=${{flex:1,minWidth:0,minHeight:"100vh",overflow:"auto",padding:"24px 28px 30px",position:"relative"}}>
      <div style=${{position:"absolute",left:"28px",right:"28px",top:"18px",height:"72px",borderRadius:T.r.lg,background:T.bg.accent,opacity:.9,filter:"blur(38px)",pointerEvents:"none"}}></div>
      <div style=${{position:"relative",zIndex:1,maxWidth:"1600px",margin:"0 auto",display:"flex",flexDirection:"column",gap:"18px"}}>
        <div style=${{display:"flex",justifyContent:"space-between",alignItems:"center",gap:"12px",padding:"14px 18px",borderRadius:T.r.lg,background:T.bg.surface,border:"1px solid "+T.bd.s,boxShadow:T.shadow.glow,backdropFilter:"blur(18px)"}}>
          <div>
            <div style=${{fontSize:"11px",fontWeight:800,color:T.t.m,letterSpacing:"0.12em",textTransform:"uppercase"}}>Meridiano CRM</div>
            <div style=${{fontSize:"15px",fontWeight:700,marginTop:"3px"}}>${page==="dashboard"?"Panel ejecutivo":page.charAt(0).toUpperCase()+page.slice(1)}</div>
          </div>
          <div style=${{display:"flex",alignItems:"center",gap:"10px",color:T.t.s,fontSize:"12px"}}>
            <span style=${{padding:"6px 10px",borderRadius:T.r.pill,background:T.bg.input,border:"1px solid "+T.bd.s,fontWeight:700}}>${user?.role_label||""}</span>
            <span style=${{fontWeight:700}}>${user?.username||""}</span>
          </div>
        </div>
        <div style=${{display:"flex",flexDirection:"column",gap:"18px"}}>${pages[page]||pages.dashboard}</div>
      </div>
    </main>
    <${Toast} toast=${toast} onClose=${()=>setToast(null)} />
  </div>`;
}

export {App};
