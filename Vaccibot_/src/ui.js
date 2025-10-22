// src/ui.js

const form = document.getElementById("chat-form");
const input = document.getElementById("user-input");
const chatBody = document.getElementById("chat-body");

export function onSubmit(handler){
  form.addEventListener("submit", (e)=>{
    e.preventDefault();
    const message = input.value.trim();
    if(!message) return;
    handler(message);
    input.value = "";
  });
}

export function addUserMessage(text){
  const div = document.createElement("div");
  div.className = "message user";
  div.textContent = text;
  chatBody.appendChild(div);
  chatBody.scrollTop = chatBody.scrollHeight;
}

export function botReply(html, delay=600){
  const typing = document.createElement("div");
  typing.className = "message bot typing";
  typing.innerHTML = "<span>•</span><span>•</span><span>•</span>";
  chatBody.appendChild(typing);
  chatBody.scrollTop = chatBody.scrollHeight;

  setTimeout(()=>{
    typing.remove();
    const div = document.createElement("div");
    div.className = "message bot";
    div.innerHTML = html.replace(/\n/g, "<br>");
    chatBody.appendChild(div);
    chatBody.scrollTop = chatBody.scrollHeight;
  }, delay);
}
