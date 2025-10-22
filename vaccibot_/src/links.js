// src/links.js

export function openMapsQueryAroundLatLon(query,lat,lon){
  const url=`https://www.google.com/maps/search/${encodeURIComponent(query)}/@${lat},${lon},14z`;
  window.open(url,"_blank");
}
export function openMapsQueryInCity(query,city){
  const url=`https://www.google.com/maps/search/${encodeURIComponent(query+" "+city)}`;
  window.open(url,"_blank");
}
export function openDoctolibCity(specialty,city){
  const slug=specialty.toLowerCase().replace(/\s+/g,"-");
  const url=`https://www.doctolib.fr/${slug}/${encodeURIComponent(city)}`;
  window.open(url,"_blank");
}
