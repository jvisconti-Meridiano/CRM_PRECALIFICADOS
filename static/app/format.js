const fmtARS=v=>{const n=Number(v)||0;const x=Math.abs(n);const s=x.toLocaleString("en-US",{minimumFractionDigits:2,maximumFractionDigits:2}).replace(/,/g,"X").replace(/\./g,",").replace(/X/g,".");return`${n<0?"\u2212":""}$ ${s}`;};
const fmtDate=d=>{if(!d||d==="\u2014")return"\u2014";try{const p=d.split("-");if(p.length===3)return`${p[2]}/${p[1]}/${p[0]}`;return d;}catch{return d;}};
const fmtPct=v=>`${(Number(v||0)*100).toFixed(1)}%`;
const fmtDT=d=>d||"\u2014";


export {fmtARS,fmtDate,fmtPct,fmtDT};
