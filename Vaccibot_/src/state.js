// src/state.js

export const state = {
  step: 0,
  pharmacies: [],
  currentQuestion: null, 
  userData: {
    age: null,
    symptomes: "",
    details: "",
    duration: null,        
    vaccinated: null,      
    vaccinationAsked: false,
    lastCity: null,
    location: null    
  }
};

export const constants = {
  greetings: ["salut","bonjour","bonsoir","hello","yo"],
  thanks: ["merci","thx","thanks","super","cool","top"],
  symptomsList: ["fièvre","toux","rhume","fatigue","maux","tête","nausée","grippe","courbature","mal","dents","gorge","peau","yeux"],
  yesWords: ["oui","ouais","yes","yep"],
  noWords: ["non","nope","pas vacciné","pas vaccine","pas vacciner","non vacciné","non vaccine"],
  durationRe: /\b(\d{1,3})\s*(semaine|semaines|jour|jours|heure|heures)\b/i,
  ageExplicitRe: /\bj[' ]?ai\s*(\d{1,3})\s*(ans?|years?)\b/i,
  ageWithUnitRe: /\b(\d{1,3})\s*(ans?|years?)\b/i
};
