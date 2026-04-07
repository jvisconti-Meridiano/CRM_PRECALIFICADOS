import {h,useState} from "../deps.js";
import {T,ThemeToggle} from "../theme.js";
import {POST} from "../api.js";
import {Inp, Btn} from "../ui.js";

function LoginPage({onLogin,themeName,onThemeChange}){
  const[u,setU]=useState("");
  const[p,setP]=useState("");
  const[err,setErr]=useState("");

  const submit=async e=>{
    e.preventDefault();
    setErr("");
    const r=await POST("/api/auth/login",{username:u,password:p});
    if(r?.ok!==false&&r?.user) onLogin(r.user);
    else setErr(r?.error||"Error");
  };

  return h`<div style=${{minHeight:"100vh",display:"grid",gridTemplateColumns:"minmax(320px,520px) minmax(320px,560px)",alignItems:"center",justifyContent:"center",gap:"30px",padding:"32px",background:T.bg.base}}>
    <div style=${{padding:"10px 6px 10px 0"}}>
      <div style=${{display:"inline-flex",alignItems:"center",gap:"10px",padding:"8px 12px",borderRadius:T.r.pill,background:T.bg.surface,border:"1px solid "+T.bd.s,boxShadow:T.shadow.glow,backdropFilter:"blur(18px)"}}>
        <div style=${{width:"34px",height:"34px",borderRadius:"12px",background:T.bg.accent,border:"1px solid "+T.bd.s,display:"grid",placeItems:"center"}}><img src="/static/logo.png" alt="" style=${{width:"22px",height:"22px",objectFit:"contain"}} /></div>
        <div>
          <div style=${{fontSize:"14px",fontWeight:800}}>Meridiano CRM</div>
          <div style=${{fontSize:"10px",color:T.t.m,letterSpacing:"0.12em",textTransform:"uppercase"}}>Fintech credit workflow</div>
        </div>
      </div>
      <div style=${{marginTop:"26px"}}>
        <div style=${{fontSize:"44px",lineHeight:1.02,fontWeight:800,letterSpacing:"-0.04em",maxWidth:"540px"}}>Crédito, monitoreo y operación con experiencia fintech real.</div>
        <p style=${{marginTop:"16px",fontSize:"15px",lineHeight:1.7,color:T.t.s,maxWidth:"520px"}}>Un workspace premium para firmantes, política, cartera y alertas BCRA. Visual limpio, lectura rápida y foco en decisión operativa.</p>
      </div>
      <div style=${{display:"grid",gridTemplateColumns:"repeat(3,minmax(0,1fr))",gap:"12px",marginTop:"24px"}}>
        ${[
          ["Monitoreo", "BCRA + trazabilidad"],
          ["Workflow", "Aprobaciones y control"],
          ["Portfolio", "Vista ejecutiva"],
        ].map(([a,b])=>h`<div key=${a} style=${{padding:"16px",borderRadius:T.r.lg,background:T.bg.card,border:"1px solid "+T.bd.s,boxShadow:T.shadow.glow,backdropFilter:"blur(16px)"}}><div style=${{fontSize:"12px",fontWeight:800}}>${a}</div><div style=${{fontSize:"12px",color:T.t.m,marginTop:"6px",lineHeight:1.5}}>${b}</div></div>`)}
      </div>
    </div>

    <form onSubmit=${submit} style=${{width:"100%",maxWidth:"520px",justifySelf:"end",background:T.bg.card,border:"1px solid "+T.bd.s,borderRadius:T.r.lg,padding:"30px",boxShadow:T.shadow.pop,backdropFilter:"blur(22px)"}}>
      <div style=${{display:"flex",justifyContent:"space-between",alignItems:"flex-start",gap:"12px",marginBottom:"24px"}}>
        <div>
          <div style=${{fontSize:"12px",fontWeight:800,color:T.t.m,letterSpacing:"0.12em",textTransform:"uppercase"}}>Acceso seguro</div>
          <div style=${{fontSize:"28px",fontWeight:800,letterSpacing:"-0.03em",marginTop:"6px"}}>Ingresar</div>
          <div style=${{fontSize:"13px",color:T.t.s,marginTop:"8px"}}>Entrá al CRM con tu usuario operativo.</div>
        </div>
        <${ThemeToggle} themeName=${themeName} onChange=${onThemeChange} compact=${true} />
      </div>

      ${err?h`<div style=${{padding:"11px 14px",borderRadius:T.r.md,background:T.c.redG,border:"1px solid rgba(239,68,68,0.2)",color:T.t.err,fontSize:"13px",marginBottom:"14px",boxShadow:T.shadow.inset}}>${err}</div>`:null}

      <label style=${{display:"block",fontSize:"12px",fontWeight:700,color:T.t.s,marginBottom:"6px",letterSpacing:"0.03em"}}>Usuario</label>
      <${Inp} value=${u} onChange=${setU} placeholder="Ingresá tu usuario" />

      <label style=${{display:"block",fontSize:"12px",fontWeight:700,color:T.t.s,marginTop:"16px",marginBottom:"6px",letterSpacing:"0.03em"}}>Clave</label>
      <${Inp} type="password" value=${p} onChange=${setP} placeholder="Ingresá tu clave" />

      <${Btn} type="submit" variant="primary" style=${{width:"100%",marginTop:"20px",padding:"13px 14px",fontSize:"14px"}}>Ingresar al workspace<//>
    </form>
  </div>`;
}

export {LoginPage};
