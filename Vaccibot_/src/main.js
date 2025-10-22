// src/main.js
import { onSubmit, addUserMessage, botReply } from "./ui.js";
import { state } from "./state.js";
import { FLOW } from "./flow.js";

// Démarrage du flow au chargement
window.addEventListener("DOMContentLoaded", ()=>{
  FLOW.start();
});

onSubmit(async (message)=>{
  addUserMessage(message);
  await FLOW.handle(message);
});
