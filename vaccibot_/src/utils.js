// src/utils.js

export const DAYS_FR = ["Dimanche","Lundi","Mardi","Mercredi","Jeudi","Vendredi","Samedi"];
export function todayLabel(){ return DAYS_FR[new Date().getDay()]; }

export function parseRanges(str){
  if(!str) return [];
  return String(str)
    .replace(/h/gi,":")
    .split(/[;,/|]+/)
    .map(s=>s.trim()).filter(Boolean)
    .map(s=>{
      const m=s.match(/(\d{1,2})[:h]?(\d{0,2})\s*[-–]\s*(\d{1,2})[:h]?(\d{0,2})/);
      if(!m) return null;
      const[,sh,sm="",eh,em=""]=m;
      return [Number(sh),Number(sm||0),Number(eh),Number(em||0)];
    }).filter(Boolean);
}

export function minsNow(){ const d=new Date(); return d.getHours()*60+d.getMinutes(); }
export function isOpenNow(ranges){
  const now=minsNow();
  for(const[sh,sm,eh,em] of ranges){
    const start=sh*60+sm, end=eh*60+em;
    if(now>=start && now<=end) return true;
  }
  return false;
}
export function prettyRanges(ranges){
  if(!ranges.length) return "—";
  return ranges.map(([sh,sm,eh,em]) =>
    `${String(sh).padStart(2,"0")}:${String(sm).padStart(2,"0")}–${String(eh).padStart(2,"0")}:${String(em).padStart(2,"0")}`
  ).join(" ; ");
}

export function pick(row, aliases){
  for(const key of aliases){
    if(row[key]!=null && String(row[key]).trim()!=="") return row[key];
  }
  return "";
}

export function distanceKm(lat1, lon1, lat2, lon2){
  const R=6371;
  const dLat=((lat2-lat1)*Math.PI)/180;
  const dLon=((lon2-lon1)*Math.PI)/180;
  const a=Math.sin(dLat/2)**2 + Math.cos(lat1*Math.PI/180)*Math.cos(lat2*Math.PI/180)*Math.sin(dLon/2)**2;
  return 2*R*Math.atan2(Math.sqrt(a),Math.sqrt(1-a));
}
