// src/specialists.js

export function specialistFor(text){
  const t=text.toLowerCase();
  if(t.includes("dents")||t.includes("dent")) return "dentiste";
  if(t.includes("peau")||t.includes("eczéma")||t.includes("acné")) return "dermatologue";
  if(t.includes("yeux")||t.includes("vue")) return "ophtalmologue";
  if(t.includes("gorge")||t.includes("toux")||t.includes("fièvre")||t.includes("grippe")) return "médecin généraliste";
  return "médecin généraliste";
}
