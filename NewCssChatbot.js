const Chatbot = (function () {
    // Environment-based API URL configuration
    const isLocalDev = window.location.hostname === "localhost" || window.location.hostname === "127.0.0.1";
    const baseApiUrl = isLocalDev
      ? "http://localhost:8080"
      : "https://gtf72jhlxpxnrbdoepfcldi7rm.apigateway.uk-london-1.oci.customer-oci.com/v1";
  
    const config = {
      buttonColor: "#001689",
      defaultTitle: "Fusion Assisto", // Base title without agent
      welcomeMessage: "I am your Fusion Assistant, how may I help you?",
      apiUrls: {
        // scm: `https://gtf72jhlxpxnrbdoepfcldi7rm.apigateway.uk-london-1.oci.customer-oci.com/v1/query`,
        scm:`https://gtf72jhlxpxnrbdoepfcldi7rm.apigateway.uk-london-1.oci.customer-oci.com/v2/scm`,
        hcm: `https://gtf72jhlxpxnrbdoepfcldi7rm.apigateway.uk-london-1.oci.customer-oci.com/v2/hcm`
      },
      titles: {
        scm: "SCM Agent",
        hcm: "HCM Agent"
      },
      username: null,
      proxyUrl: null,
      threadId: "",
      agentType: null,
      maxWidth: 550,
      maxHeight: 650,
      formatPreference: "natural_language"
    };
  
    let messagesContainer, chatWindow, formatToggle, selectionScreen;
    let isExpanded = false;
  
    function loadStylesheet() {
      const style = document.createElement("style");
      style.textContent = `
        @keyframes fadeIn {
          from { opacity: 0; transform: translateY(10px); }
          to { opacity: 1; transform: translateY(0); }
        }
        @keyframes thinking {
          0% { opacity: 0.3; }
          50% { opacity: 1; }
          100% { opacity: 0.3; }
        }
        @keyframes buttonHover {
          from { transform: scale(1); }
          to { transform: scale(1.05); }
        }
        .chatbot-container {
          position: fixed;
          bottom: 20px;
          right: 20px;
          z-index: 1000;
          font-family: 'Oracle Sans', sans-serif;
        }
        .chatbot-button {
          width: 50px;
          height: 50px;
          border-radius: 50%;
          background-color: #001689;
          color: white;
          border: none;
          font-size: 24px;
          cursor: pointer;
          box-shadow: 0 4px 8px rgba(0, 0, 0, 0.2);
          transition: transform 0.3s ease;
        }
        .chatbot-button:hover {
          transform: scale(1.1);
        }
        .selection-screen {
          position: fixed;
          bottom: 80px;
          right: 20px;
          width: 300px;
          background: #001689;
          border-radius: 12px;
          padding: 20px;
          display: none;
          flex-direction: column;
          gap: 10px;
          box-shadow: 0 5px 15px rgba(0, 0, 0, 0.2);
          z-index: 1000;
          animation: fadeIn 0.3s ease-out;
        }
        .selection-screen.open {
          display: flex;
        }
        .agent-button {
          padding: 12px;
          background: #001265;
          color: white;
          border: none;
          border-radius: 8px;
          cursor: pointer;
          font-size: 16px;
          text-align: center;
          transition: transform 0.2s, background 0.2s;
        }
        .agent-button:hover {
          background: #001377;
          animation: buttonHover 0.2s forwards;
        }
        .chatbot-window {
          position: fixed;
          bottom: 80px;
          right: 20px;
          width: 400px;
          min-width: 400px;
          height: 550px;
          min-height: 550px;
          background: #001689;
          display: none;
          flex-direction: column;
          overflow: hidden;
          z-index: 1000;
          font-family: 'Oracle Sans', sans-serif;
          font-size: 15px;
          border-radius: 12px;
          box-shadow: 0 5px 15px rgba(0, 0, 0, 0.2);
          transition: height 0.3s ease;
          max-width: 80vw;
          max-height: 80vh;
        }
        .chatbot-window.open {
          display: flex;
        }
        .chatbot-header {
          display: flex;
          justify-content: space-between;
          align-items: center;
          padding: 15px;
          background: #001689;
          border-bottom: 1px solid rgba(255, 255, 255, 0.1);
          border-radius: 12px 12px 0 0;
          position: relative;
          min-height: 50px;
        }
        .chatbot-logo {
          height: 46px;
          width: auto;
          margin-right: 10px;
          vertical-align: middle;
        }
        .header-title-container {
          display: flex;
          align-items: center;
          margin: 0 auto;
          flex: 1;
          justify-content: center;
        }
        .format-toggle-container {
          position: relative;
          display: flex;
          background: #001265;
          border-radius: 8px;
          overflow: hidden;
          margin: 0 auto 8px;
          box-shadow: 0 2px 5px rgba(0, 0, 0, 0.2);
          width: 80%;
          height: 30px;
          z-index: 900;
        }
        .format-toggle-option {
          flex: 1;
          padding: 6px 12px;
          color: white;
          cursor: pointer;
          font-size: 13px;
          transition: background-color 0.2s;
          user-select: none;
          text-align: center;
          display: flex;
          align-items: center;
          justify-content: center;
        }
        .format-toggle-option.active {
          background-color: #001689;
          font-weight: bold;
        }
        .format-toggle-option:hover:not(.active) {
          background-color: #001377;
        }
        .expand-btn {
          position: absolute;
          left: 12px;
          top: 50%;
          transform: translateY(-50%);
          background: rgba(255, 255, 255, 0.2);
          border: none;
          color: white;
          width: 24px;
          height: 24px;
          border-radius: 4px;
          font-size: 14px;
          cursor: w-resize;
          display: flex;
          align-items: center;
          justify-content: center;
          touch-action: none;
        }
        .expand-btn:hover {
          background: rgba(255, 255, 255, 0.3);
        }
        .expand-btn.dragging {
          background: rgba(255, 255, 255, 0.5);
        }
        .refresh-btn, .close-btn {
          background: none;
          border: none;
          font-size: 20px;
          cursor: pointer;
          color: white;
          margin-left: 10px;
        }
        .chatbot-title {
          color: white;
          font-weight: 500;
          font-size: 18px;
        }
        .chatbot-messages {
          flex: 1;
          padding: 15px;
          overflow-y: auto;
          background: #f9f9f9;
          color: #333333;
        }
        .message {
          margin-bottom: 10px;
          padding: 12px;
          border-radius: 12px;
          max-width: 80%;
          word-wrap: break-word;
          animation: fadeIn 0.3s ease-out forwards;
          font-size: 15px;
          line-height: 1.4;
        }
        .user-message {
          background: #e6e6e6;
          margin-left: auto;
          color: #333333;
          border-radius: 12px 12px 0 12px;
        }
        .bot-message {
          background: #d9d9d9;
          color: #333333;
          border-radius: 0 12px 12px 12px;
        }
        .thinking-indicator {
          display: flex;
          padding: 12px;
          background: #d9d9d9;
          border-radius: 12px;
          margin-bottom: 10px;
          width: 60px;
          animation: fadeIn 0.3s ease-out forwards;
        }
        .thinking-dot {
          height: 8px;
          width: 8px;
          border-radius: 50%;
          background: #666666;
          margin: 0 3px;
          animation: thinking 1.4s infinite;
        }
        .thinking-dot:nth-child(2) {
          animation-delay: 0.2s;
        }
        .thinking-dot:nth-child(3) {
          animation-delay: 0.4s;
        }
        .chatbot-input {
          display: flex;
          padding: 12px;
          border-top: 1px solid #cccccc;
          background: #e6e6e6;
          align-items: center;
          border-radius: 0 0 12px 12px;
        }
        .chatbot-input input {
          flex: 1;
          padding: 10px 12px;
          border: 1px solid #cccccc;
          border-radius: 12px;
          background: #f9f9f9;
          color: #333333;
          font-size: 15px;
        }
        .chatbot-input button {
          padding: 10px 18px;
          background: #999999;
          color: white;
          border: none;
          border-radius: 12px;
          cursor: pointer;
          font-size: 15px;
          margin-left: 10px;
        }
        .chatbot-input button:hover {
          background: #777777;
        }
        .formatted-response p {
          margin: 8px 0;
        }
        .table-container {
          overflow-x: auto;
          margin: 10px 0;
        }
        .markdown-table {
          border-collapse: collapse;
          width: 100%;
          margin: 10px 0;
          font-size: 14px;
        }
        .markdown-table th, .markdown-table td {
          padding: 8px;
          text-align: left;
          border: 1px solid #ddd;
        }
        .markdown-table th {
          background-color: #f2f2f2;
          font-weight: bold;
        }
        .markdown-table tr:nth-child(even) {
          background-color: #f9f9f9;
        }
        .markdown-list {
          padding-left: 20px;
          margin: 8px 0;
          list-style-type: disc;
        }
        .markdown-list li {
          margin-bottom: 6px;
          line-height: 1.4;
        }
        .markdown-list .markdown-list {
          list-style-type: circle;
          margin-top: 6px;
        }
        .markdown-list li a {
          color: #0066cc;
          text-decoration: none;
        }
        .markdown-list li a:hover {
          text-decoration: underline;
        }
        @media (max-width: 480px) {
          .chatbot-window, .selection-screen {
            width: 90%;
            right: 5%;
            bottom: 70px;
          }
        }
      `;
      document.head.appendChild(style);
    }
  
    const container = document.createElement("div");
    container.className = "chatbot-container";
  
    const button = document.createElement("button");
    button.className = "chatbot-button";
    button.innerHTML = "🗨️";
    button.title = "Open Chat";
  
    selectionScreen = document.createElement("div");
    selectionScreen.className = "selection-screen";
  
    const scmButton = document.createElement("button");
    scmButton.className = "agent-button";
    scmButton.textContent = "SCM Agent";
    scmButton.dataset.agent = "scm";
  
    const hcmButton = document.createElement("button");
    hcmButton.className = "agent-button";
    hcmButton.textContent = "HCM Agent";
    hcmButton.dataset.agent = "hcm";
  
    selectionScreen.appendChild(scmButton);
    selectionScreen.appendChild(hcmButton);
  
    chatWindow = document.createElement("div");
    chatWindow.className = "chatbot-window";
  
    const header = document.createElement("div");
    header.className = "chatbot-header";
  
    const expandBtn = document.createElement("button");
    expandBtn.className = "expand-btn";
    expandBtn.innerHTML = "⟷";
    expandBtn.title = "Drag to resize width";
  
    const headerTitleContainer = document.createElement("div");
    headerTitleContainer.className = "header-title-container";
  
    const titleElement = document.createElement("div");
    titleElement.className = "chatbot-title";
    titleElement.textContent = config.defaultTitle;
  
    const refreshBtn = document.createElement("button");
    refreshBtn.className = "refresh-btn";
    refreshBtn.innerHTML = "↻";
    refreshBtn.title = "Refresh chat";
  
    const closeBtn = document.createElement("button");
    closeBtn.className = "close-btn";
    closeBtn.innerHTML = "✖️";
  
    const formatToggleContainer = document.createElement("div");
    formatToggleContainer.className = "format-toggle-container";
  
    const nlOption = document.createElement("div");
    nlOption.className = "format-toggle-option active";
    nlOption.textContent = "Natural Language";
    nlOption.dataset.format = "natural_language";
  
    const tableOption = document.createElement("div");
    tableOption.className = "format-toggle-option";
    tableOption.textContent = "Table Format";
    tableOption.dataset.format = "table";
  
    formatToggleContainer.appendChild(nlOption);
    formatToggleContainer.appendChild(tableOption);
  
    const messages = document.createElement("div");
    messages.className = "chatbot-messages";
    messagesContainer = messages;
  
    const inputArea = document.createElement("div");
    inputArea.className = "chatbot-input";
  
    const input = document.createElement("input");
    input.type = "text";
    input.placeholder = "Type your message...";
  
    const sendButton = document.createElement("button");
    sendButton.textContent = "Send";
  
    inputArea.appendChild(input);
    inputArea.appendChild(sendButton);
    header.appendChild(expandBtn);
    header.appendChild(headerTitleContainer);
    header.appendChild(refreshBtn);
    header.appendChild(closeBtn);
    headerTitleContainer.appendChild(titleElement);
  
    chatWindow.appendChild(header);
    chatWindow.appendChild(formatToggleContainer);
    chatWindow.appendChild(messages);
    chatWindow.appendChild(inputArea);
  
    container.appendChild(button);
    container.appendChild(selectionScreen);
    container.appendChild(chatWindow);
    document.body.appendChild(container);
  
    let isDragging = false;
    let startX = 0;
    let startWidth = 0;
    const minWidth = 400;
  
    button.addEventListener("click", toggleSelectionScreen);
    scmButton.addEventListener("click", () => selectAgent("scm"));
    hcmButton.addEventListener("click", () => selectAgent("hcm"));
    closeBtn.addEventListener("click", toggleChat);
    refreshBtn.addEventListener("click", refreshChat);
    sendButton.addEventListener("click", sendMessage);
    input.addEventListener("keypress", (e) => {
      if (e.key === "Enter") sendMessage();
    });
  
    nlOption.addEventListener("click", () => setFormatPreference("natural_language"));
    tableOption.addEventListener("click", () => setFormatPreference("table"));
  
    expandBtn.addEventListener("mousedown", startDrag);
    expandBtn.addEventListener("touchstart", startDrag, { passive: false });
    document.addEventListener("mousemove", onDrag);
    document.addEventListener("touchmove", onDrag, { passive: false });
    document.addEventListener("mouseup", endDrag);
    document.addEventListener("touchend", endDrag);
  
    const resizeObserver = new ResizeObserver(() => adjustChatWindowSize());
    resizeObserver.observe(messagesContainer);
  
    function toggleSelectionScreen() {
      // Reset chat and close chat window before opening selection screen
      refreshChat();
      chatWindow.classList.remove("open");
      selectionScreen.classList.toggle("open");
      // Reset title to default
      titleElement.textContent = config.defaultTitle;
      config.agentType = null; // Clear agent selection
    }
  
    function selectAgent(agentType) {
      config.agentType = agentType;
      // Set dynamic title: Fusion Assisto-[HCM Agent] or Fusion Assisto-[SCM Agent]
      titleElement.textContent = `${config.defaultTitle}-${config.titles[agentType]}`;
      selectionScreen.classList.remove("open");
      toggleChat();
    }
  
    function toggleChat() {
      chatWindow.classList.toggle("open");
      if (chatWindow.classList.contains("open") && !messagesContainer.children.length) {
        addMessage(config.welcomeMessage, "bot-message");
      }
    }
  
    function startDrag(e) {
      e.preventDefault();
      isDragging = true;
      startX = e.type === "touchstart" ? e.touches[0].clientX : e.clientX;
      startWidth = parseInt(window.getComputedStyle(chatWindow).width);
      expandBtn.classList.add("dragging");
      document.body.style.userSelect = "none";
    }
  
    function onDrag(e) {
      if (!isDragging) return;
      let currentX = e.type === "touchmove" ? e.touches[0].clientX : e.clientX;
      e.preventDefault();
      const movedDistance = startX - currentX;
      const newWidth = Math.max(minWidth, startWidth + movedDistance);
      chatWindow.style.width = `${newWidth}px`;
      chatWindow.style.right = "20px";
      chatWindow.style.left = "auto";
    }
  
    function endDrag() {
      if (isDragging) {
        isDragging = false;
        expandBtn.classList.remove("dragging");
        document.body.style.userSelect = "";
      }
    }
  
    function adjustChatWindowSize() {
      const contentHeight = messagesContainer.scrollHeight;
      const headerHeight = header.offsetHeight;
      const toggleHeight = formatToggleContainer.offsetHeight;
      const inputHeight = inputArea.offsetHeight;
      const totalContentHeight = contentHeight + headerHeight + toggleHeight + inputHeight;
      if (totalContentHeight > chatWindow.offsetHeight && totalContentHeight < config.maxHeight) {
        chatWindow.style.height = `${totalContentHeight}px`;
      }
      const messageElements = messagesContainer.querySelectorAll(".message");
      if (messageElements.length > 0) {
        let maxMessageWidth = 0;
        messageElements.forEach((msg) => {
          const msgWidth = msg.scrollWidth;
          if (msgWidth > maxMessageWidth) maxMessageWidth = msgWidth;
        });
        const containerWidth = messagesContainer.offsetWidth;
        if (maxMessageWidth > containerWidth * 0.9 && chatWindow.offsetWidth < config.maxWidth) {
          const newWidth = Math.min(config.maxWidth, chatWindow.offsetWidth + 20);
          chatWindow.style.width = `${newWidth}px`;
        }
      }
    }
  
    function addMessage(text, className) {
      const message = document.createElement("div");
      message.className = `message ${className}`;
      if (className === "bot-message") {
        const formatted = formatLLMResponse(text);
        message.appendChild(formatted);
      } else {
        message.textContent = text;
      }
      messages.appendChild(message);
      messages.scrollTop = messages.scrollHeight;
      setTimeout(adjustChatWindowSize, 100);
    }
  
    function showThinkingIndicator() {
      const thinking = document.createElement("div");
      thinking.className = "thinking-indicator";
      thinking.id = "thinking-indicator";
      for (let i = 0; i < 3; i++) {
        const dot = document.createElement("div");
        dot.className = "thinking-dot";
        thinking.appendChild(dot);
      }
      messagesContainer.appendChild(thinking);
      messagesContainer.scrollTop = messagesContainer.scrollHeight;
    }
  
    function removeThinkingIndicator() {
      const indicator = document.getElementById("thinking-indicator");
      if (indicator) messagesContainer.removeChild(indicator);
    }
  
    async function sendMessage() {
      const text = input.value.trim();
      if (!text || !config.agentType) return;
      addMessage(text, "user-message");
      input.value = "";
      showThinkingIndicator();
      try {
        const responseData = await askAgent(text);
        removeThinkingIndicator();
        addMessage(responseData.response, "bot-message");
      } catch (error) {
        removeThinkingIndicator();
        addMessage("Sorry, I couldn't process your request. " + error.message, "bot-message");
        console.error("Error:", error);
      }
    }
  
    async function askAgent(question) {
      try {
        const payload = {
          question,
          thread_id: config.threadId,
          format_preference: config.formatPreference,
          agent_type: config.agentType
        };
        const apiUrl = config.apiUrls[config.agentType];
        console.log("Request payload:", JSON.stringify(payload));
        console.log("Using API URL:", apiUrl);
        const response = await fetch(apiUrl, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          credentials: "include",
          body: JSON.stringify(payload)
        });
        if (!response.ok) {
          const errorData = await response.json().catch(() => ({}));
          throw new Error(`Server error: ${response.status} - ${errorData.detail || "Unknown error"}`);
        }
        const data = await response.json();
        console.log("API response:", data);
        if (data.thread_id) {
          config.threadId = data.thread_id;
          console.log("Updated thread_id:", config.threadId);
        }
        return data;
      } catch (error) {
        console.error("Error calling API:", error);
        throw error;
      }
    }
  
    function refreshChat() {
      while (messagesContainer.firstChild) {
        messagesContainer.removeChild(messagesContainer.firstChild);
      }
      config.threadId = "";
      console.log("Chat refreshed, thread_id reset to null");
    }
  
    function setFormatPreference(format) {
      config.formatPreference = format;
      const options = formatToggleContainer.querySelectorAll(".format-toggle-option");
      options.forEach((option) => {
        option.classList.toggle("active", option.dataset.format === format);
      });
      console.log(`Format preference set to: ${format}`);
    }
  
    function formatLLMResponse(text) {
      const container = document.createElement("div");
      container.className = "formatted-response";
      if (text.includes("|")) {
        const formattedText = formatMarkdownTable(text);
        container.innerHTML = formattedText;
      } else if (text.match(/^\s*[\*\-]\s+/m)) {
        const formattedText = formatMarkdownBullets(text);
        container.innerHTML = formattedText;
      } else {
        const linkedText = convertMarkdownLinks(text);
        container.innerHTML = `<p>${linkedText}</p>`;
      }
      return container;
    }
  
    function formatMarkdownBullets(text) {
      const lines = text.split("\n");
      let inList = false;
      let result = "";
      for (let i = 0; i < lines.length; i++) {
        const line = lines[i].trim();
        if (line.match(/^\s*[\*\-]\s+/)) {
          if (!inList) {
            result += '<ul class="markdown-list">';
            inList = true;
          }
          const bulletContent = line.replace(/^\s*[\*\-]\s+/, "");
          const linkedBulletContent = convertMarkdownLinks(bulletContent);
          result += `<li>${linkedBulletContent}</li>`;
        } else {
          if (inList) {
            result += "</ul>";
            inList = false;
          }
          if (line) {
            const linkedText = convertMarkdownLinks(line);
            result += `<p>${linkedText}</p>`;
          }
        }
      }
      if (inList) result += "</ul>";
      return result;
    }
  
    function convertMarkdownLinks(text) {
      return text.replace(/\[([^\]]+)\]\(([^)]+)\)/g, '<a href="$2" target="_blank">$1</a>');
    }
  
    function formatMarkdownTable(text) {
      const lines = text.split("\n");
      let tableLines = [];
      let afterTable = [];
      let inTable = false;
      for (const line of lines) {
        if (line.trim().startsWith("|")) {
          inTable = true;
          tableLines.push(line);
        } else if (inTable) {
          afterTable.push(line);
        }
      }
      let result = '<div class="table-container"><table class="markdown-table">';
      if (tableLines.length >= 2) {
        const headers = tableLines[0].split("|").filter((cell) => cell.trim() !== "");
        result += "<thead><tr>";
        headers.forEach((header) => {
          result += `<th>${header.trim()}</th>`;
        });
        result += "</tr></thead><tbody>";
        for (let i = 2; i < tableLines.length; i++) {
          const cells = tableLines[i].split("|").filter((cell) => cell.trim() !== "");
          result += "<tr>";
          cells.forEach((cell) => {
            result += `<td>${cell.trim()}</td>`;
          });
          result += "</tr>";
        }
        result += "</tbody></table></div>";
      }
      if (afterTable.length > 0) {
        const afterTableText = afterTable.join("<br>");
        const linkedText = convertMarkdownLinks(afterTableText);
        result += linkedText;
      }
      return result;
    }
  
    return {
      init: (options) => {
        loadStylesheet();
        Object.assign(config, options);
        console.log("Initialized with API URLs:", config.apiUrls);
        titleElement.textContent = config.defaultTitle;
        button.style.backgroundColor = config.buttonColor;
      },
      destroy: () => {
        resizeObserver.disconnect();
        container.remove();
      }
    };
  })();
  
  export default Chatbot;