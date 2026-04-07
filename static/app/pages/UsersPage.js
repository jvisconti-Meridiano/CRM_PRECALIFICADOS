import {h,useState,useEffect,useMemo} from "../deps.js";
import {GET, POST} from "../api.js";
import {Badge, Btn, Inp, Sel, Card, Tbl, SectionTitle, Loader} from "../ui.js";

function UsersPage({notify,themeName}){
  const[users,setU]=useState([]);
  const[loading,setL]=useState(true);
  const[nu,setNu]=useState("");
  const[np,setNp]=useState("");
  const[nr,setNr]=useState("sales");

  const load=async()=>{ setL(true); const r=await GET("/api/users"); setU(r?.users||[]); setL(false); };
  useEffect(()=>{load();},[]);

  const crear=async()=>{ if(!nu||!np) return; const r=await POST("/api/users",{username:nu,password:np,role:nr}); notify?.(r?.message||r?.error||"OK",r?.ok===false?"error":"ok"); setNu(""); setNp(""); await load(); };
  const toggle=async(u,active)=>{ await POST(`/api/users/${u}/${active?"reactivate":"deactivate"}`,{}); await load(); };

  const cols=useMemo(()=>[
    {header:"Usuario",exportHeader:"Usuario",key:"username"},
    {header:"Rol",exportHeader:"Rol",render:r=>h`<${Badge} size="xs">${r.role_label||r.role}<//>`,exportValue:r=>r.role_label||r.role},
    {header:"Estado",exportHeader:"Estado",render:r=>h`<${Badge} variant=${r.is_active?"success":"danger"} size="xs">${r.is_active?"Activo":"Inactivo"}<//>`,exportValue:r=>r.is_active?"Activo":"Inactivo"},
    {header:"",exportHeader:"",render:r=>r.is_active?h`<${Btn} size="sm" variant="ghost" onClick=${()=>toggle(r.username,false)}>Desactivar<//>`:h`<${Btn} size="sm" onClick=${()=>toggle(r.username,true)}>Reactivar<//>`},
  ],[themeName]);

  if(loading) return h`<${Loader}/>`;
  return h`<div style=${{display:"flex",flexDirection:"column",gap:"18px"}}>
    <${SectionTitle} title="Usuarios" />
    <${Card}><div style=${{display:"flex",gap:"8px",alignItems:"center",flexWrap:"wrap"}}><${Inp} value=${nu} onChange=${setNu} placeholder="Username" style=${{maxWidth:"180px"}} /><${Inp} type="password" value=${np} onChange=${setNp} placeholder="Password" style=${{maxWidth:"180px"}} /><${Sel} value=${nr} onChange=${setNr} options=${[{value:"admin",label:"Admin"},{value:"risk",label:"Riesgos"},{value:"sales",label:"Comercial"}]} /><${Btn} onClick=${crear} variant="primary">Crear<//></div><//>
    <${Card} style=${{overflow:"hidden"}}><${Tbl} columns=${cols} data=${users} exportFileName="usuarios.xlsx" exportSheetName="Usuarios" onExportResult=${ok=>notify?.(ok?"Excel exportado.":"No se pudo exportar el Excel.",ok?"ok":"error")} /><//>
  </div>`;
}

export {UsersPage};
