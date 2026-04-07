import {h,useState,useEffect,useMemo} from "../deps.js";
import {T} from "../theme.js";
import {GET} from "../api.js";
import {SearchInput, Card, Tbl, SectionTitle, Sel} from "../ui.js";

function CarteraPage({notify,themeName}){
  const[kind,setKind]=useState("cheques");
  const[data,setD]=useState({headers:[],rows:[],status:"OK"});
  const[loading,setL]=useState(true);
  const[search,setSearch]=useState("");
  const[sortCol,setSortCol]=useState("");
  const[sortDir,setSortDir]=useState("asc");

  const load=async()=>{
    setL(true);
    const r=await GET("/api/cartera/"+kind);
    setD({headers:r?.headers||[],rows:r?.rows||[],status:r?.status||"OK"});
    setL(false);
    setSearch("");
    setSortCol("");
  };
  useEffect(()=>{load();},[kind]);

  const filtered=useMemo(()=>{
    let out=data.rows||[];
    if(search.trim()){
      const q=search.toLowerCase().trim();
      out=out.filter(r=>Object.values(r).some(v=>String(v||"").toLowerCase().includes(q)));
    }
    if(sortCol&&data.headers.includes(sortCol)){
      out=[...out].sort((a,b)=>{
        const va=String(a[sortCol]||"");
        const vb=String(b[sortCol]||"");
        const na=parseFloat(va.replace(/[^0-9,.\-]/g,"").replace(",","."));
        const nb=parseFloat(vb.replace(/[^0-9,.\-]/g,"").replace(",","."));
        if(!isNaN(na)&&!isNaN(nb)) return sortDir==="asc"?na-nb:nb-na;
        return sortDir==="asc"?va.localeCompare(vb):vb.localeCompare(va);
      });
    }
    return out;
  },[data.rows,search,sortCol,sortDir]);

  const toggleSort=col=>{ if(sortCol===col) setSortDir(d=>d==="asc"?"desc":"asc"); else { setSortCol(col); setSortDir("asc"); } };

  const cols=useMemo(()=>data.headers.map(hd=>({
    header:h`<span onClick=${()=>toggleSort(hd)} style=${{cursor:"pointer",userSelect:"none"}}>${hd} ${sortCol===hd?(sortDir==="asc"?"↑":"↓"):""}</span>`,
    exportHeader:hd,
    render:r=>r[hd]!=null?String(r[hd]):"",
    exportValue:r=>r[hd]!=null?String(r[hd]):"",
    mono:/cuit|importe|monto|cheque/i.test(hd),
    align:/importe|monto|saldo/i.test(hd)?"right":"left",
  })),[data.headers,sortCol,sortDir]);

  return h`<div style=${{display:"flex",flexDirection:"column",gap:"16px"}}>
    <${SectionTitle} title="Cartera" sub=${filtered.length+" de "+data.rows.length+" filas · "+data.status}>
      <${SearchInput} value=${search} onChange=${setSearch} placeholder="Filtrar por CUIT, firmante, monto…" style=${{maxWidth:"300px"}} />
      <${Sel} value=${kind} onChange=${setKind} options=${[{value:"cheques",label:"Cheques"},{value:"facturas",label:"Facturas"}]} />
    <//>
    <${Card} style=${{overflow:"hidden"}}>${loading?h`<div style=${{padding:"30px",color:T.t.m}}>Cargando…</div>`:h`<${Tbl} columns=${cols} data=${filtered} empty="Sin datos. Colocá cartera.xlsx / Facturas.xlsx en el directorio." maxH="72vh" exportFileName=${kind==="cheques"?"cartera-cheques.xlsx":"cartera-facturas.xlsx"} exportSheetName=${kind==="cheques"?"Cheques":"Facturas"} onExportResult=${ok=>notify?.(ok?"Excel exportado.":"No se pudo exportar el Excel.",ok?"ok":"error")} />`}<//>
  </div>`;
}

export {CarteraPage};
