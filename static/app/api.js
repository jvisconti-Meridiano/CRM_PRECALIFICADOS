async function api(path,opts={}){
  const hd=new Headers(opts.headers||{});
  if(!(opts.body instanceof FormData)&&!hd.has("Content-Type"))hd.set("Content-Type","application/json");
  hd.set("Accept","application/json");
  try{
    const r=await fetch(path,{credentials:"same-origin",...opts,headers:hd});
    const ct=r.headers.get("content-type")||"";
    const body=ct.includes("json")?await r.json().catch(()=>null):await r.text().catch(()=>null);
    if(r.status===401){window.__setAuth?.(null);return{ok:false,error:"No autenticado."};}
    if(!r.ok){const e=typeof body==="object"&&body?(body.error||body.message||"Error"):String(body||"Error");return{ok:false,error:e,...(typeof body==="object"&&body?body:{})};}
    return typeof body==="object"&&body?body:{ok:true,data:body};
  }catch(e){return{ok:false,error:e.message||"Error de red"};}
}
const GET=p=>api(p);
const POST=(p,b)=>api(p,{method:"POST",body:b instanceof FormData?b:JSON.stringify(b||{})});
const PUT=(p,b)=>api(p,{method:"PUT",body:JSON.stringify(b||{})});
const DEL=(p,b)=>api(p,{method:"DELETE",body:b?JSON.stringify(b):undefined});
const UPLOAD=(p,file)=>{const fd=new FormData();fd.append("file",file);return api(p,{method:"POST",body:fd,headers:{Accept:"application/json"}});};


export {api,GET,POST,PUT,DEL,UPLOAD};
