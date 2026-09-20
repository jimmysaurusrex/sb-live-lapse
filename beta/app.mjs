import {COVER,finite,utc,age,fresh,temp,altitude,spread,stationRows,lcl,cloudLayers,safeImage} from "./model.mjs";
const $=id=>document.getElementById(id);
const NS="http://www.w3.org/2000/svg";
let data=null, unit="metric", loading=false, loadFailed=false;
try { if(localStorage.getItem("sb_beta_units")==="imperial") unit="imperial"; } catch {}
const temperatureUnit=()=>unit==="imperial"?"°F":"°C";
const altitudeUnit=()=>unit==="imperial"?"ft":"m";
const fmtAltitude=v=>`${Math.round(altitude(v,unit)).toLocaleString()} ${altitudeUnit()}`;
const fmtTemp=v=>finite(v)?`${temp(v,unit).toFixed(1)}${temperatureUnit()}`:"—";
function timeLabel(value) {
  const date=utc(value);
  return Number.isFinite(date)?new Intl.DateTimeFormat("en-US",{timeZone:"America/Los_Angeles",hour:"numeric",minute:"2-digit",timeZoneName:"short"}).format(date):"time unavailable";
}
function ageLabel(value) {
  const a=age(value);
  return !Number.isFinite(a)?"time unavailable":a<0?"invalid future time":`${Math.floor(a)} min ago`;
}
function node(tag,attrs={},content=null,parent=$("plot")) {
  const element=document.createElementNS(NS,tag);
  for(const [k,v] of Object.entries(attrs)) element.setAttribute(k,String(v));
  if(content!==null) element.textContent=content;
  parent.appendChild(element);return element;
}
const line=(x1,y1,x2,y2,attrs={})=>node("line",{x1,y1,x2,y2,...attrs});
const text=(x,y,value,attrs={})=>node("text",{x,y,fill:"#5b7179","font-size":12,...attrs},value);
function wind(row) {
  if(!fresh(row.wind_ob_time,60)||!finite(row.wind_spd_mps,0,125)) return "—";
  const speed=unit==="imperial"?row.wind_spd_mps*2.23694:row.wind_spd_mps;
  const gust=finite(row.wind_gust_mps,0,125)?(unit==="imperial"?row.wind_gust_mps*2.23694:row.wind_gust_mps):null;
  return `${finite(row.wind_dir,0,360)?Math.round(row.wind_dir)+"° ":""}${speed.toFixed(1)}${gust!==null?" g"+gust.toFixed(1):""} ${unit==="imperial"?"mph":"m/s"}`;
}
function summaries(rows,estimated,layers) {
  const airport=data.airport||{}, current=fresh(airport.observed_at,90);
  if(layers.length) {
    $("cloud-value").textContent=fmtAltitude(layers[0].base_msl_m);
    $("cloud-detail").textContent=`${COVER[layers[0].cover]} base · above sea level · ${timeLabel(airport.observed_at)}`;
  } else {
    $("cloud-value").textContent=!current?"Unavailable":airport.sky==="clear"?"No cloud reported":airport.sky==="obscured"?"Sky obscured":"Not reported";
    $("cloud-detail").textContent=current?`KSBA · ${timeLabel(airport.observed_at)} · ${ageLabel(airport.observed_at)}`:"No current airport cloud observation";
  }
  const near=rows.filter(r=>r.near),valid=rows.filter(r=>r.current&&!r.cached&&r.delta!==null);
  $("saturation-value").textContent=valid.length?`${near.length} of ${valid.length} stations`:"Unavailable";
  $("saturation-detail").textContent=near.length?near.map(r=>r.name).join(", ")+" · possible cloud / fog":valid.length?"No near-saturation readings in current data":"No current paired temperature / dew point";
  $("lcl-value").textContent=estimated===null?"Unavailable":fmtAltitude(estimated);
  const vor=rows.find(r=>r.id==="SE068");
  $("lcl-detail").textContent=estimated===null?"Requires current VOR temperature & dew point":`Estimated, above sea level · ${timeLabel(vor.temp_ob_time)}`;
  $("metar-report").textContent=airport.raw||"No airport report available.";
  $("airport-time").textContent=airport.observed_at?`Observed ${timeLabel(airport.observed_at)} · ${ageLabel(airport.observed_at)}${!current?" · stale, excluded from chart":""}${airport.fetch_ok===false?" · source fetch failed; retained report":""}`:"";
}
function renderTable(rows) {
  $("station-rows").replaceChildren();
  for(const row of rows) {
    const tr=document.createElement("tr");
    const entries=[row.name,row.elev_m===null?"—":fmtAltitude(row.elev_m),row.current?fmtTemp(row.temp_c):"—",row.current?fmtTemp(row.dew_c):"—",
      row.current&&row.delta!==null?`${spread(row.delta,unit).toFixed(1)}${temperatureUnit()}`:"—",wind(row),`${timeLabel(row.temp_ob_time)}`,row.status];
    entries.forEach((entry,index)=>{
      const td=document.createElement("td");td.textContent=entry;
      if(index===0) {const name=document.createElement("b");name.textContent=entry;td.replaceChildren(name);const small=document.createElement("small");small.textContent=row.id==="KC6OYN"?"KC60YN":row.id;td.appendChild(small);}
      if(index===3) td.className="dew-value";
      if(index===6) {const small=document.createElement("small");small.textContent=ageLabel(row.temp_ob_time);td.appendChild(small);}
      if(index===7) {const badge=document.createElement("span");badge.className="status"+(row.near?" near":!row.current||row.cached?" stale":"");badge.textContent=entry;td.replaceChildren(badge);}
      td.title=index===0?String(row.provider||"Source unavailable"):entry;tr.appendChild(td);
    });$("station-rows").appendChild(tr);
  }
}
function renderPlot(rows,estimated,layers) {
  const plot=$("plot");plot.replaceChildren();
  node("title",{id:"plot-title"},"Santa Barbara temperature, dew point, and observed airport cloud bases");
  node("desc",{id:"plot-description"},"Orange squares show station temperatures; cyan dots show dew point at each station elevation. Gray cloud-base markers refer only to the airport. Dashed gold marks the estimated VOR LCL, not observed cloud.");
  const valid=rows.filter(r=>r.current&&r.elev_m!==null),rass=data.profile?.rass||{};
  const rassCurrent=fresh(rass.ob_time_utc,360);
  const rassPoints=rassCurrent&&Array.isArray(rass.points_100m_c)?rass.points_100m_c.filter(p=>Array.isArray(p)&&finite(p[0],0,15000)&&finite(p[1],-100,60)):[];
  const values=[...valid.flatMap(s=>[s.temp_c,...($("dew-toggle").checked&&s.dew_c!==null?[s.dew_c]:[])]),...rassPoints.map(p=>p[1])];
  const xValues=values.map(v=>temp(v,unit));
  const xMin=Math.floor((Math.min(...xValues,unit==="imperial"?50:10)-3)/5)*5;
  const xMax=Math.ceil((Math.max(...xValues,unit==="imperial"?75:25)+3)/5)*5;
  const heights=[1800,...valid.map(s=>s.elev_m),...rassPoints.map(p=>p[0])];
  // Keep high cloud layers from crushing the mountain profile; list them above it.
  if($("lcl-toggle").checked&&estimated!==null&&estimated<=3500) heights.push(estimated+100);
  if($("cloud-toggle").checked) layers.filter(l=>l.base_msl_m<=3500).forEach(l=>heights.push(l.base_msl_m+100));
  const yStep=unit==="imperial"?1000:500;
  const yMax=Math.ceil(altitude(Math.max(...heights),unit)/yStep)*yStep;
  const left=72,right=716,top=58,bottom=552;
  const x=v=>left+(temp(v,unit)-xMin)/(xMax-xMin)*(right-left);
  const y=v=>bottom-altitude(v,unit)/yMax*(bottom-top);
  const grid="#e9efee";
  for(let tick=0;tick<=yMax;tick+=yStep) {
    const py=bottom-tick/yMax*(bottom-top);line(left,py,1040,py,{stroke:grid});text(left-12,py+4,tick.toLocaleString(),{"text-anchor":"end"});
  }
  const xStep=(xMax-xMin)>45?10:5;
  for(let tick=xMin;tick<=xMax;tick+=xStep) {
    const px=left+(tick-xMin)/(xMax-xMin)*(right-left);line(px,top,px,bottom,{stroke:grid});text(px,bottom+24,tick,{"text-anchor":"middle"});
  }
  line(left,top,left,bottom,{stroke:"#9dadb0"});line(left,bottom,right,bottom,{stroke:"#9dadb0"});
  text(18,(top+bottom)/2,`Elevation (${altitudeUnit()} MSL)`,{transform:`rotate(-90 18 ${(top+bottom)/2})`,"text-anchor":"middle"});
  text((left+right)/2,610,`Temperature (${temperatureUnit()})`,{"text-anchor":"middle"});
  text(left,28,"SURFACE STATIONS & RASS",{"font-size":11,"letter-spacing":1});
  text(807,28,"CLOUD BASES & LCL",{"font-size":11,"letter-spacing":1});
  text(807,45,"Observed KSBA · estimated VOR",{"font-size":11,fill:"#8a989e"});
  line(782,top,782,bottom,{stroke:"#d5dfe0","stroke-dasharray":"3 5"});
  const reference=valid.find(r=>r.id==="KSBA")||valid[0];
  const defs=node("defs");const clip=node("clipPath",{id:"profile-clip"},null,defs);node("rect",{x:left,y:top,width:right-left,height:bottom-top},null,clip);
  if(reference) {
    const refPoints=[reference.elev_m,Math.max(...heights)].map(z=>`${x(reference.temp_c-.0098*(z-reference.elev_m))},${y(z)}`).join(" ");
    node("polyline",{points:refPoints,fill:"none",stroke:"#9aa7ac","stroke-dasharray":"6 5","stroke-width":1.3,"clip-path":"url(#profile-clip)"});
  }
  if(rassPoints.length>1) {
    node("polyline",{points:rassPoints.map(([z,t])=>`${x(t)},${y(z)}`).join(" "),fill:"none",stroke:"#243138","stroke-width":2.1});
    rassPoints.forEach(([z,t])=>node("circle",{cx:x(t),cy:y(z),r:2.3,fill:"#517b8e"}));
  }
  if(valid.length>1) node("polyline",{points:valid.map(s=>`${x(s.temp_c)},${y(s.elev_m)}`).join(" "),fill:"none",stroke:"#a7aca6","stroke-dasharray":"3 5","stroke-width":1.2});
  const labelRows=[...valid].reverse().map(s=>({row:s,py:y(s.elev_m)}));
  for(let i=0;i<labelRows.length;i++) labelRows[i].ly=Math.max(labelRows[i].py-10,i?labelRows[i-1].ly+30:top+5);
  for(let i=labelRows.length-1;i>=0;i--) labelRows[i].ly=Math.min(labelRows[i].ly,i<labelRows.length-1?labelRows[i+1].ly-30:bottom-4);
  for(const {row,py,ly} of labelRows) {
    const px=x(row.temp_c),point=node("g",{"data-station":row.id});
    node("title",{},`${row.name}: ${fmtTemp(row.temp_c)}; dew point ${fmtTemp(row.dew_c)}; ${row.status}; ${timeLabel(row.temp_ob_time)}`,point);
    if($("dew-toggle").checked&&row.dew_c!==null) {
      if(row.near) node("circle",{cx:px,cy:py,r:11,fill:"#def3f1",stroke:"#71bfc4","stroke-width":1.4},null,point);
      node("line",{x1:x(row.dew_c),y1:py,x2:px,y2:py,stroke:"#1593a4","stroke-width":2,"stroke-dasharray":row.cached?"3 3":"none"},null,point);
      node("circle",{cx:x(row.dew_c),cy:py,r:4,fill:"#087f91",stroke:"white","stroke-width":1},null,point);
    }
    node("rect",{x:px-3.5,y:py-3.5,width:7,height:7,fill:"#e99959",stroke:"#a76838","stroke-width":1},null,point);
    const labelX=600;
    line(px+6,py,labelX-8,ly-4,{stroke:"#aab7ba","stroke-width":.8});
    text(labelX,ly,`${row.name}  ${fmtTemp(row.temp_c)}`,{"font-size":12,fill:"#32474d","paint-order":"stroke",stroke:"white","stroke-width":4,"stroke-linejoin":"round"});
    const lower=valid[valid.indexOf(row)-1],dz=lower?row.elev_m-lower.elev_m:0;
    const lapse=dz>0?(row.temp_c-lower.temp_c)/dz*1000:null;
    const lapseText=lapse===null?"":`${(unit==="imperial"?lapse*.54864:lapse).toFixed(1)} ${unit==="imperial"?"°F/1000 ft":"°C/km"}`;
    text(labelX,ly+13,row.near?"Near saturation":lapseText,{"font-size":10,fill:row.near?"#087f91":"#6b7c82"});
  }
  const annotations=[];
  if($("cloud-toggle").checked) {
    layers.forEach(l=>annotations.push({z:l.base_msl_m,title:COVER[l.cover]+" base",detail:fmtAltitude(l.base_msl_m)+" MSL",color:"#607782",kind:"cloud"}));
    if(!layers.length) {
      text(807,90,$("cloud-value").textContent,{"font-size":13});
      text(807,109,"No cloud base plotted",{"font-size":11});
    }
  }
  if($("lcl-toggle").checked&&estimated!==null) annotations.push({z:estimated,title:"Estimated VOR LCL",detail:fmtAltitude(estimated)+" MSL",color:"#a4772d",kind:"lcl"});
  annotations.sort((a,b)=>b.z-a.z);
  let labelY=top-40;
  for(const a of annotations) {
    const py=y(a.z),outside=py<top;
    const ly=Math.max(outside?top+12:py-8,labelY+43);labelY=ly;
    const markerY=Math.max(top,py);
    line(795,markerY,820,markerY,{stroke:a.color,"stroke-width":a.kind==="cloud"?4:2,"stroke-dasharray":a.kind==="lcl"?"4 3":"none"});
    if(Math.abs(ly+5-markerY)>12) line(820,markerY,832,ly+5,{stroke:a.color,"stroke-width":.8});
    text(839,ly,a.title,{fill:a.color,"font-size":12});
    text(839,ly+17,a.detail+(outside?" ↑":""),{fill:a.color,"font-size":12});
  }
  if(!valid.length) text(350,280,"No current station observations",{"text-anchor":"middle","font-size":17});
  $("rass-status").textContent=rassPoints.length?`RASS: ${timeLabel(rass.ob_time_utc)} · ${ageLabel(rass.ob_time_utc)}${rass.source!=="live"?" · retained profile":""}.` : "RASS unavailable or older than six hours; no profile plotted.";
}
function media(kind,limit) {
  const source=data[kind]||{},image=$(kind+"-image"),placeholder=$(kind+"-placeholder");
  const url=safeImage(source.image_url,kind),current=fresh(source.observed_at,limit)&&!source.offline;
  image.hidden=true;placeholder.hidden=false;
  placeholder.textContent=current?"Loading image…":source.observed_at?"Latest image is stale — open the source for current conditions":"Preview unavailable — open the source below";
  const time=$(kind+"-time");
  time.textContent=source.observed_at?`${timeLabel(source.observed_at)} · ${ageLabel(source.observed_at)}${!current?" · stale / offline":""}${source.fetch_ok===false?" · retained image":""}`:"Image timestamp unavailable";
  if(kind==="camera"&&finite(source.heading_deg,0,360)) time.textContent+=` · facing ${Math.round(source.heading_deg)}°`;
  if(url&&current) {
    image.onload=()=>{image.hidden=false;placeholder.hidden=true;};
    image.onerror=()=>{image.hidden=true;placeholder.hidden=false;placeholder.textContent="Image could not load — open the source below";};
    if(image.src===url&&image.complete&&image.naturalWidth) {image.hidden=false;placeholder.hidden=true;} else image.src=url;
  } else image.removeAttribute("src");
}
function render() {
  if(!data) return;
  $("metric").setAttribute("aria-pressed",String(unit==="metric"));$("imperial").setAttribute("aria-pressed",String(unit==="imperial"));
  const rows=stationRows(data.profile),estimated=lcl(rows),layers=cloudLayers(data.airport);
  summaries(rows,estimated,layers);renderPlot(rows,estimated,layers);renderTable(rows);
  media("satellite",60);media("camera",15);
  const date=utc(data.generated_at);
  $("updated").textContent=Number.isFinite(date)?`Updated ${timeLabel(data.generated_at)} · ${ageLabel(data.generated_at)}`:"Refresh time unavailable";
  const issues=[];
  if(loadFailed) issues.push("Refresh failed; retained observations are shown with their original times.");
  if(!fresh(data.generated_at,15)) issues.push("Beta data refresh is delayed.");
  const count=rows.filter(r=>r.current).length;
  if(count<7) issues.push(`${7-count} station${7-count===1?"":"s"} missing or stale.`);
  if(!fresh(data.airport?.observed_at,90)) issues.push("No current airport cloud report.");
  $("notice").hidden=!issues.length;$("notice").textContent=issues.join(" ");
}
async function refresh() {
  if(loading) return;loading=true;$("refresh").disabled=true;
  try {
    const response=await fetch("./data.json",{cache:"no-store",signal:AbortSignal.timeout(20000)});
    if(!response.ok) throw new Error("Data unavailable");
    const result=await response.json();if(result.version!==1) throw new Error("Unsupported data");
    data=result;loadFailed=false;render();
  } catch {
    loadFailed=true;
    if(data) render();else {$("notice").hidden=false;$("notice").textContent="Beta observations could not load. Try Refresh observations, or return to the primary chart.";$("updated").textContent="Observations unavailable";}
  } finally {loading=false;$("refresh").disabled=false;}
}
for(const choice of ["metric","imperial"]) $(choice).addEventListener("click",()=>{unit=choice;try{localStorage.setItem("sb_beta_units",unit);}catch{}render();});
for(const id of ["dew-toggle","cloud-toggle","lcl-toggle"]) $(id).addEventListener("change",render);
$("refresh").addEventListener("click",refresh);
document.addEventListener("visibilitychange",()=>{if(!document.hidden) refresh();});
setInterval(()=>{if(!document.hidden) refresh();},300000);
setInterval(()=>{if(!document.hidden&&data) render();},60000);
refresh();
