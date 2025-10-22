// src/pharmacy.js
import { state } from "./state.js";
import { todayLabel, parseRanges, isOpenNow, prettyRanges, pick, distanceKm } from "./utils.js";

const DAY_NAMES = ["Lundi","Mardi","Mercredi","Jeudi","Vendredi","Samedi","Dimanche"];

function extractOpeningFromHtmlCell(row, parseRanges, todayLabel) {
  let html = "";
  for (const k in row) {
    const v = row[k];
    if (typeof v === "string" && /<li>.*<\/li>/i.test(v)) {
      html = v;
      break;
    }
  }
  if (!html) return { perDay: {}, todayRanges: [] };

  const perDay = {};
  const liRegex = /<li>\s*(Lundi|Mardi|Mercredi|Jeudi|Vendredi|Samedi|Dimanche)\s*:\s*([^<]+?)\s*<\/li>/gi;
  let m;
  while ((m = liRegex.exec(html))) {
    const day = m[1];
    const times = m[2].replace(/&nbsp;|&#160;/g, " ").trim();
    perDay[day] = times;
  }

  const today = todayLabel();
  const todayStr = perDay[today] || "";
  const todayRanges = parseRanges(todayStr);

  return { perDay, todayRanges };
}

export async function loadCSV(){
  return new Promise((resolve)=>{
    Papa.parse("pharmacies.csv",{
      header:true,
      delimiter:";",
      download:true,
      complete:(results)=>{
        const data = (results.data || []).map(r=>{
          const name = pick(r,["Titre","Nom","name"]) || "Pharmacie";
          const address = pick(r,["Adresse_voie 1","Adresse","address"]);
          const city = pick(r,["Adresse_ville","Ville","city"]);
          const lat = parseFloat(String(pick(r,["Adresse_latitude","lat","latitude"])).replace(",",".")); // peut être NaN
          const lon = parseFloat(String(pick(r,["Adresse_longitude","lon","longitude"])).replace(",",".")); // peut être NaN
          const globalHours = pick(r,["Horaires","Heures","Hours"]);

          
          const perDayCsv = {
            Lundi:    pick(r,["Lundi","Lun","Mon"]),
            Mardi:    pick(r,["Mardi","Mar","Tue"]),
            Mercredi: pick(r,["Mercredi","Mer","Wed"]),
            Jeudi:    pick(r,["Jeudi","Jeu","Thu"]),
            Vendredi: pick(r,["Vendredi","Ven","Fri"]),
            Samedi:   pick(r,["Samedi","Sam","Sat"]),
            Dimanche: pick(r,["Dimanche","Dim","Sun"]),
          };

          let todayRanges;
          let perDayFromCsv = perDayCsv;

          const hasPerDayFromCsv = Object.values(perDayFromCsv).some(v => v && String(v).trim() !== "");
          if (hasPerDayFromCsv) {
            const today = todayLabel();
            const todayStr = perDayFromCsv[today] || globalHours || "";
            todayRanges = parseRanges(todayStr);
          } else {
            const parsed = extractOpeningFromHtmlCell(r, parseRanges, todayLabel);
            perDayFromCsv = parsed.perDay;
            todayRanges = parsed.todayRanges;
          }

   
          return {
            name,
            address,
            city,
            lat,
            lon,
            hours: { global: globalHours, perDay: perDayFromCsv },
            todayRanges
          };
        })
        .filter(p => Number.isFinite(p.lat) && Number.isFinite(p.lon));

        resolve(data);
      },
      error:()=>resolve([])
    });
  });
}

export function ensurePharmaciesLoaded(){
  return state.pharmacies.length
    ? Promise.resolve(state.pharmacies)
    : loadCSV().then(d => (state.pharmacies = d));
}

export function findNearest(lat, lon, limit=3){
  return state.pharmacies
    .map(p=>({...p, distance:distanceKm(lat,lon,p.lat,p.lon)}))
    .sort((a,b)=>a.distance-b.distance)
    .slice(0,limit);
}

export function lineForPharmacy(p){
  const open = isOpenNow(p.todayRanges);
  const hoursPretty = prettyRanges(p.todayRanges);
  const q = encodeURIComponent(`${p.address} ${p.city}`);
  const badge = open ? "🟢 Ouvert" : "🔴 Fermé";
  const today = todayLabel();

  let line = `🔹 <b>${p.name}</b><br>${p.address}, ${p.city}<br>${badge}`;
  if (p.todayRanges && p.todayRanges.length) {
    line += ` — <i>${today} : ${hoursPretty}</i>`;
  }
  line += `<br><a href="https://www.google.com/maps?q=${q}" target="_blank">📍 Itinéraire</a><br><br>`;
  return line;
}
