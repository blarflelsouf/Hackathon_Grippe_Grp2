// src/flow.js
import { state, constants } from "./state.js";
import { botReply } from "./ui.js";
import { detectIntent } from "./intents.js";
import { proposeNearbyPharmacies, proposeSpecialistLinks, handleGeolocForPharmacies, handleGeolocForSpecialist } from "./actions.js";

export const FLOW = {
  STEP: {
    ASK_AGE: "ASK_AGE",
    ASK_HAS_SYMPTOMS: "ASK_HAS_SYMPTOMS",
    ASK_SYMPTOMS_TEXT: "ASK_SYMPTOMS_TEXT",
    ASK_DURATION: "ASK_DURATION",
    ASK_VACCINATED: "ASK_VACCINATED",
    INFO_RISKS_IF_NOT_VACCINATED: "INFO_RISKS_IF_NOT_VACCINATED",
    ENSURE_LOCATION_FOR_VACCINATION: "ENSURE_LOCATION_FOR_VACCINATION",
    OFFER_VACCINATION_CENTER: "OFFER_VACCINATION_CENTER",
    SHOW_VACCINATION_CENTER: "SHOW_VACCINATION_CENTER",
    SUGGEST_SPECIALIST: "SUGGEST_SPECIALIST",
    END: "END"
  },

  start(){
    state.flow = { step: this.STEP.ASK_AGE };
    botReply("Quel âge avez-vous ?");
  },

  async handle(message){
    const { STEP } = this;
    const s = state.flow?.step || STEP.ASK_AGE;
    const lower = message.toLowerCase();
    const intent = detectIntent(message); // (gardé au cas où)
    const { userData } = state;

    switch(s){

      case STEP.ASK_AGE: {
        const m = lower.match(constants.ageExplicitRe) || lower.match(/^(\d{1,3})\b/);
        const age = m ? parseInt(m[1],10) : NaN;
        if(Number.isFinite(age) && age > 0 && age < 120){
          userData.age = age;
          state.flow.step = STEP.ASK_HAS_SYMPTOMS;
          botReply(`D'accord, vous avez ${age} ans. Avez-vous des symptômes ? (oui/non)`);
        } else {
          botReply("Je n’ai pas bien compris. Quel âge avez-vous ?");
        }
        break;
      }

      case STEP.ASK_HAS_SYMPTOMS: {
        if (constants.yesWords.some(w=>lower.includes(w))) {
          state.flow.step = STEP.ASK_SYMPTOMS_TEXT;
          botReply("Quels sont vos symptômes ?");
        } 
        else if (constants.noWords.some(w=>lower.includes(w)) || lower.includes("non")) {
          state.flow.step = STEP.END;
          botReply("Très bien 👍 Restez attentif à votre état et hydratez-vous. Si des symptômes apparaissent, consultez un professionnel de santé.");
          return; // stop ici
        } 
        else {
          botReply("Pouvons-nous dire que vous avez des symptômes ? (oui/non)");
        }
        break;
      }

      case STEP.ASK_SYMPTOMS_TEXT: {
        userData.symptomes = message.trim();
        state.flow.step = STEP.ASK_DURATION;
        botReply("Depuis quand ? (ex : 3 jours / 1 semaine)");
        break;
      }

      case STEP.ASK_DURATION: {
        const mm = lower.match(constants.durationRe);
        if(!mm){
          botReply("Petite précision : depuis combien de temps exactement ? (ex : 2 jours / 1 semaine)");
          return;
        }
        const [, valStr, unit] = mm;
        userData.duration = { value:Number(valStr), unit };
        state.flow.step = STEP.ASK_VACCINATED;
        botReply("Êtes-vous vacciné contre la grippe cette année ? (oui/non)");
        break;
      }

      case STEP.ASK_VACCINATED: {
        if(constants.yesWords.some(w=>lower.includes(w))){
          userData.vaccinated = true;
          botReply("Génial! Rester à jour améliore votre protection.");
          state.flow.step = STEP.SUGGEST_SPECIALIST;
          await this.suggestSpecialist();
        } else if(constants.noWords.some(w=>lower.includes(w)) || lower.includes("non")){
          userData.vaccinated = false;
          state.flow.step = STEP.INFO_RISKS_IF_NOT_VACCINATED;
          botReply("Être non vacciné augmente le risque de formes sévères et la transmission à votre entourage. La vaccination reste recommandée, surtout en cas de facteurs de risque.");
          // Ensuite on s'assure de la localisation avant de proposer un centre
          state.flow.step = STEP.ENSURE_LOCATION_FOR_VACCINATION;
          await this.ensureLocationForVaccinationPreface();
        } else {
          botReply("Je n’ai pas saisi. Êtes-vous vacciné contre la grippe cette année ? (oui/non)");
        }
        break;
      }

      case STEP.ENSURE_LOCATION_FOR_VACCINATION: {
        if(state.currentQuestion === "geolocForVaccination"){
          if(constants.yesWords.some(w=>lower.includes(w))){
            return handleGeolocForPharmacies();
          } else if(constants.noWords.some(w=>lower.includes(w)) || lower.includes("non")){
            botReply("Très bien. Donnez-moi votre ville pour chercher près de chez vous :");
            state.currentQuestion = "askCityForVaccinationFlow";
            return;
          }
        }
        if(state.currentQuestion === "askCityForVaccinationFlow"){
          state.userData.lastCity = message.trim();
          state.currentQuestion = null;
          state.flow.step = STEP.OFFER_VACCINATION_CENTER;
          botReply("Souhaitez-vous un centre de vaccination (pharmacie) près de chez vous ? (oui/non)");
          return;
        }
        // Si on n'est pas dans une sous-question, on (re)demande
        await this.ensureLocationForVaccinationPreface();
        break;
      }

      case STEP.OFFER_VACCINATION_CENTER: {
        if(constants.yesWords.some(w=>lower.includes(w))){
          state.flow.step = STEP.SHOW_VACCINATION_CENTER;
          await proposeNearbyPharmacies();
          state.flow.step = STEP.SUGGEST_SPECIALIST;
          await this.suggestSpecialist();
        } else if(constants.noWords.some(w=>lower.includes(w)) || lower.includes("non")){
          state.flow.step = STEP.SUGGEST_SPECIALIST;
          await this.suggestSpecialist();
        } else {
          botReply("Souhaitez-vous un centre de vaccination près de chez vous ? (oui/non)");
        }
        break;
      }

      case STEP.SUGGEST_SPECIALIST: {
        if (state.currentQuestion === "askCityForDoctor") {
          if (constants.yesWords.some(w => lower.includes(w))) {
            handleGeolocForSpecialist();
            setTimeout(async () => {
              if (state.userData.location || state.userData.lastCity) {
                await proposeSpecialistLinks(); 
                state.flow.step = STEP.END;
              }
            }, 1200);
            return;
          } else if (constants.noWords.some(w => lower.includes(w)) || lower.includes("non")) {
            botReply("Très bien. Indiquez simplement votre ville :");
            state.currentQuestion = "askCityForDoctorCity";
            return;
          }
          botReply("Souhaitez-vous partager votre localisation ou me donner votre ville pour vous proposez un medecin le plus proche de votre secteur ? (oui/non)");
          return;
        }

        if (state.currentQuestion === "askCityForDoctorCity") {
          state.userData.lastCity = message.trim();
          state.currentQuestion = null;
          await proposeSpecialistLinks();
          state.flow.step = STEP.END;
          return;
        }

        await this.suggestSpecialist();
        break;
      }

      case STEP.END:
      default:
        botReply("Je reste disponible si besoin 😊");
        break;
    }
  },

  async ensureLocationForVaccinationPreface(){
    const { userData } = state;
    if(userData.location || userData.lastCity){
      state.flow.step = this.STEP.OFFER_VACCINATION_CENTER;
      botReply("Souhaitez-vous un centre de vaccination (pharmacie) près de chez vous ? (oui/non)");
      return;
    }
    botReply("Souhaitez-vous que j’utilise votre localisation pour chercher un centre de vaccination près de chez vous ? (oui/non)");
    state.currentQuestion = "geolocForVaccination";
  },

  async suggestSpecialist(){
    const { userData } = state;

    if (!userData.location && !userData.lastCity) {
      botReply("Souhaitez-vous partager votre localisation ou me donner votre ville pour vous proposez des spécialistes plus proches de votre secteur ? (oui/non)");
      state.currentQuestion = "askCityForDoctor";
      state.flow.step = this.STEP.SUGGEST_SPECIALIST; 
      return;
    }

    await proposeSpecialistLinks();
    state.flow.step = this.STEP.END;
    return;
  }
};
