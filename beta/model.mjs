export const NAMES = {KC6OYN:"La Cumbre",SE068:"VOR",SE234:"AntFarm",MTIC1:"Montecito",MPWC1:"SM Pass","421SE":"Parma",KSBA:"Airport"};
export const COVER = {FEW:"Few",SCT:"Scattered",BKN:"Broken",OVC:"Overcast"};
export function finite(value, low=-Infinity, high=Infinity) {
  return typeof value === "number" && Number.isFinite(value) && value >= low && value <= high;
}
export function utc(value) {
  if (typeof value !== "string" || !/^\d{4}-\d{2}-\d{2}T/.test(value)) return NaN;
  return Date.parse(/(?:Z|[+-]\d{2}:?\d{2})$/.test(value) ? value : value + "Z");
}
export function age(value, now=Date.now()) { return (now - utc(value))/60000; }
export function fresh(value, minutes, now=Date.now()) { const a=age(value,now); return Number.isFinite(a)&&a>=0&&a<=minutes; }
export function temp(value, unit) { return unit === "imperial" ? value*9/5+32 : value; }
export function altitude(value, unit) { return unit === "imperial" ? value/0.3048 : value; }
export function spread(value, unit) { return unit === "imperial" ? value*9/5 : value; }
export function stationRows(profile, now=Date.now()) {
  return Object.keys(NAMES).map(id=>{
    const source=profile?.stations?.[id]||{};
    const temperature=finite(source.temp_c,-100,60)?source.temp_c:null;
    const dew=finite(source.dew_c,-100,60)&&temperature!==null&&source.dew_c<=temperature?source.dew_c:null;
    const elevation=finite(source.elev_m,-500,9000)?source.elev_m:null;
    const current=fresh(source.temp_ob_time,60,now)&&temperature!==null;
    const cached=String(source.provider||"").includes("(last-good)");
    const delta=dew===null||temperature===null?null:temperature-dew;
    return {...source,id,name:NAMES[id],temp_c:temperature,dew_c:dew,elev_m:elevation,current,cached,delta,
      near:current&&!cached&&delta!==null&&delta<=1,
      status:!current?"Unavailable / stale":cached?"Cached reading":dew===null?"Dew point missing":delta<=1?"Near saturation":"Below saturation"};
  }).sort((a,b)=>(a.elev_m??Infinity)-(b.elev_m??Infinity));
}
export function lcl(rows) {
  const vor=rows.find(s=>s.id==="SE068");
  return vor?.current&&!vor.cached&&vor.delta!==null&&vor.elev_m!==null ? vor.elev_m+125*vor.delta : null;
}
export function cloudLayers(airport, now=Date.now()) {
  if (!fresh(airport?.observed_at,90,now)) return [];
  return (Array.isArray(airport?.layers)?airport.layers:[]).filter(l=>COVER[l.cover]&&finite(l.base_msl_m,-500,20000)&&finite(l.base_agl_ft,0,60000));
}
export function safeImage(value, kind) {
  if(typeof value!=="string") return null;
  const pattern=kind==="camera"
    ? /^https:\/\/img\.cdn\.prod\.alertwest\.com\/data\/img\/1986\/\d{4}\/\d{2}\/\d{2}\/Gibraltar_2_\d{10}_\d+\.jpg$/
    : /^https:\/\/cdn\.star\.nesdis\.noaa\.gov\/WFO\/lox\/GEOCOLOR\/\d{11}_GOES18-ABI-lox-GEOCOLOR-600x600\.jpg$/;
  return pattern.test(value)?value:null;
}
