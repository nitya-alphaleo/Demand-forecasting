function toggleChat() {
    const bot = document.getElementById("chatbot");
    bot.style.display = bot.style.display === "flex" ? "none" : "flex";
}

function handleKeyPress(e) {
    if (e.key === "Enter") sendMessage();
}

function clearChat() {
    document.getElementById("chatMessages").innerHTML = "";
}

async function sendMessage() {
    const input = document.getElementById("chatInput");
    const chatBox = document.getElementById("chatMessages");
    const question = input.value.trim();
    if (!question) return;

    // Show user message
    chatBox.innerHTML += `<div class="user-message message"><b>You:</b> ${question}</div>`;
    input.value = "";

    // 🧠 SHOW THINKING MESSAGE
    const thinkingDiv = document.createElement("div");
    thinkingDiv.className = "bot-message message thinking";
    thinkingDiv.innerHTML = "<b>Genie:</b> 🤔 Thinking...";
    chatBox.appendChild(thinkingDiv);
    chatBox.scrollTop = chatBox.scrollHeight;

    try {
        const response = await fetch("/api/chat", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ question })
        });

        const data = await response.json();

        // Replace thinking with actual answer
        thinkingDiv.classList.remove("thinking");
        thinkingDiv.innerHTML = `<b>Genie:</b> ${data.answer.replace(/\n/g, "<br>")}`;

    } catch (err) {
        thinkingDiv.innerHTML = "<b>Genie:</b> ❌ Server error. Try again.";
    }

    chatBox.scrollTop = chatBox.scrollHeight;
}
