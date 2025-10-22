// src/actions.js
import { state } from "./state.js";
import { botReply } from "./ui.js";
import { ensurePharmaciesLoaded, findNearest, lineForPharmacy } from "./pharmacy.js";
import { specialistFor } from "./specialists.js";
import { openMapsQueryAroundLatLon, openDoctolibCity } from "./links.js";


export async function proposeNearbyPharmacies(){
  await ensurePharmaciesLoaded();

  const { userData } = state;

  if (userData.location) {
    const list = findNearest(userData.location.latitude, userData.location.longitude, 3);
    if (list.length){
      let text = "Voici des pharmacies proches où vous pouvez vous faire vacciner 💉 :<br>";
      list.forEach(p => text += lineForPharmacy(p));
      botReply(text, 800);
      return;
    }
  }

  if (userData.lastCity){
    const match = state.pharmacies
      .filter(p => p.city && p.city.toLowerCase().includes(userData.lastCity.toLowerCase()))
      .slice(0, 3);
    if (match.length){
      let text = `Voici des pharmacies à ${userData.lastCity} 💉 :<br>`;
      match.forEach(p => text += lineForPharmacy(p));
      botReply(text, 800);
      return;
    }
  }

  botReply("Je n’ai trouvé aucune pharmacie à proximité immédiate. Donnez-moi votre ville pour affiner la recherche.");
}

export async function proposeSpecialistLinks(){
  const { userData } = state;
  const specialty = specialistFor(userData.symptomes || "");

  const city = userData.lastCity || "votre ville";
  const specSlug = specialty.toLowerCase().replace(/\s+/g, "-");
  const doctolibUrl = `https://www.doctolib.fr/${specSlug}/${encodeURIComponent(city)}`;

  const mapsUrl = userData.location
    ? `https://www.google.com/maps/search/${encodeURIComponent(specialty)}/@${userData.location.latitude},${userData.location.longitude},14z`
    : `https://www.google.com/maps/search/${encodeURIComponent(specialty + " " + city)}`;

  const text =
    `Vous pouvez consulter un <b>${specialty}</b> 🩺 pour vos symptômes.<br>` +
    `👉 <a href="${doctolibUrl}" target="_blank">Prendre RDV sur Doctolib</a><br>` +
    `👉 <a href="${mapsUrl}" target="_blank">Voir sur Google Maps</a>`;

  botReply(text);
}

export function handleGeolocForPharmacies(){
  botReply("Je recherche votre position…");

  navigator.geolocation.getCurrentPosition(async (pos)=>{
    state.userData.location = { latitude: pos.coords.latitude, longitude: pos.coords.longitude };
    botReply("Localisation obtenue 📍");

    await proposeNearbyPharmacies();

    const specialty = specialistFor(state.userData.symptomes || "");
    botReply("Je vous redirige vers les médecins de votre secteur afin de prendre RDV pour vos symptômes…");

    const { latitude, longitude } = state.userData.location;

    openMapsQueryAroundLatLon(specialty, latitude, longitude);

    if (state.userData.lastCity) {
      openDoctolibCity(specialty, state.userData.lastCity);
    }

    state.currentQuestion = null;
    if (state.flow) state.flow.step = "END";
  }, ()=>{
    botReply("Impossible d'accéder à la géolocalisation. Donnez-moi votre ville :");
    state.currentQuestion = "askCityForVaccination";
  });
}

export function handleGeolocForSpecialist(){
  botReply("Je recherche votre position…");
  navigator.geolocation.getCurrentPosition(async (pos) => {
    state.userData.location = {
      latitude: pos.coords.latitude,
      longitude: pos.coords.longitude
    };
    botReply("Localisation obtenue 📍 J’ouvre les résultats…");

    const specialty = specialistFor(state.userData.symptomes || "");


    openMapsQueryAroundLatLon(
      specialty,
      state.userData.location.latitude,
      state.userData.location.longitude
    );

    await proposeSpecialistLinks();


    if (state.userData.lastCity) {
      openDoctolibCity(specialty, state.userData.lastCity);
    }


    state.currentQuestion = null;
    if (state.flow) state.flow.step = "END";
  }, () => {
    botReply("Impossible d'accéder à la géolocalisation. Donnez-moi votre ville :");
    state.currentQuestion = "askCityForSpecialist";
  });
}

