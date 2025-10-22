// src/intents.js
import { state, constants } from "./state.js";

export function detectIntent(message){
  const { currentQuestion } = state;
  const { greetings, thanks, symptomsList, durationRe, ageExplicitRe, ageWithUnitRe, yesWords, noWords } = constants;
  const lower = message.toLowerCase();

  if (currentQuestion === "vaccinated") {
    if (yesWords.some(w => lower.includes(w))) return "vaccinated_yes";
    if (noWords.some(w => lower.includes(w)) || lower.includes("non")) return "vaccinated_no";
  }

  if (greetings.some(w => lower.includes(w))) return "greeting";
  if (thanks.some(w => lower.includes(w))) return "thanks";
  if (durationRe.test(lower)) return "duration";
  if (symptomsList.some(w => lower.includes(w))) return "symptom";
  if (ageExplicitRe.test(lower) || ageWithUnitRe.test(lower)) return "age";
  if (state.step === 0 && /^\s*\d{1,3}\s*$/.test(lower)) return "age";

  if (lower.includes("j'habite") || lower.includes("je suis à") || lower.includes("ma ville") || lower.includes("ville"))
    return "location";

  if (lower.includes("vaccin")) {
    if (lower.includes("pas") || lower.includes("non")) return "vaccinated_no";
    if (lower.includes("oui") || lower.includes("fait")) return "vaccinated_yes";
    return "vaccination_status";
  }
  return "unknown";
}
