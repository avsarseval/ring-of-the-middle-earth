let eventSource = null;

const sideSelect = document.getElementById("sideSelect");
const apiOutput = document.getElementById("apiOutput");
const eventLog = document.getElementById("eventLog");
const stateSummary = document.getElementById("stateSummary");
const orderInput = document.getElementById("orderInput");

function pretty(data) {
  return JSON.stringify(data, null, 2);
}

function writeOutput(data) {
  if (typeof data === "string") {
    apiOutput.textContent = data;
  } else {
    apiOutput.textContent = pretty(data);
  }
}

function appendEvent(text) {
  const time = new Date().toLocaleTimeString();
  eventLog.textContent = `[${time}] ${text}\n\n` + eventLog.textContent;
}

async function requestJSON(url, options = {}) {
  const res = await fetch(url, options);
  const text = await res.text();

  let data;
  try {
    data = JSON.parse(text);
  } catch {
    data = text;
  }

  if (!res.ok) {
    throw new Error(typeof data === "string" ? data : pretty(data));
  }

  return data;
}

function currentSide() {
  return sideSelect.value;
}

async function refreshState() {
  const side = currentSide();
  const data = await requestJSON(`/game/state?side=${side}`);
  writeOutput(data);
  renderStateSummary(data, side);
}

function renderStateSummary(state, side) {
  const rb = state.units?.["ring-bearer"];
  const turn = state.turn;
  const lightView = state.lightView;
  const darkView = state.darkView;

  stateSummary.innerHTML = `
    <strong>Side:</strong> ${side}<br/>
    <strong>Turn:</strong> ${turn}<br/>
    <strong>Ring Bearer region from units:</strong> ${rb?.region || "(hidden)"}<br/>
    <strong>LightView Ring Bearer:</strong> ${lightView?.ringBearerRegion || "(hidden)"}<br/>
    <strong>DarkView Last Detected:</strong> ${darkView?.lastDetectedRegion || "-"}<br/>
    <strong>DarkView Last Detected Turn:</strong> ${darkView?.lastDetectedTurn || "-"}<br/>
    <strong>Total Units:</strong> ${Object.keys(state.units || {}).length}<br/>
    <strong>Total Regions:</strong> ${Object.keys(state.regions || {}).length}<br/>
    <strong>Total Paths:</strong> ${Object.keys(state.paths || {}).length}
  `;
}

function connectEvents() {
  if (eventSource) {
    eventSource.close();
  }

  const side = currentSide();
  eventSource = new EventSource(`/events?side=${side}`);

  appendEvent(`Connected to SSE as ${side}`);

  eventSource.onopen = () => {
    appendEvent(`SSE connection opened (${side})`);
  };

  eventSource.onerror = () => {
    appendEvent(`SSE connection error (${side})`);
  };

  eventSource.addEventListener("game.broadcast", (event) => {
    appendEvent(`game.broadcast\n${event.data}`);
    try {
      const parsed = JSON.parse(event.data);
      if (parsed.state) {
        renderStateSummary(parsed.state, side);
      }
    } catch {}
  });

  eventSource.addEventListener("game.ring.detection", (event) => {
    appendEvent(`game.ring.detection\n${event.data}`);
  });

  eventSource.onmessage = (event) => {
    appendEvent(`message\n${event.data}`);
  };
}

function loadTemplate() {
  const turn = 1;
  const value = document.getElementById("orderTemplate").value;

  const templates = {
    ringMove: {
      orderType: "ASSIGN_ROUTE",
      playerId: "light",
      unitId: "ring-bearer",
      turn,
      pathIds: ["shire-to-bree"]
    },
    aragornMove: {
      orderType: "ASSIGN_ROUTE",
      playerId: "light",
      unitId: "aragorn",
      turn,
      pathIds: ["bree-to-weathertop"]
    },
    invalidWrongTurn: {
      orderType: "ASSIGN_ROUTE",
      playerId: "light",
      unitId: "aragorn",
      turn: 99,
      pathIds: ["bree-to-weathertop"]
    },
    invalidNotYourUnit: {
      orderType: "ASSIGN_ROUTE",
      playerId: "light",
      unitId: "witch-king",
      turn,
      pathIds: ["minas-morgul-to-osgiliath"]
    }
  };

  orderInput.value = pretty(templates[value]);
}

async function sendOrder() {
  let body;

  try {
    body = JSON.parse(orderInput.value);
  } catch {
    alert("Order JSON geçersiz.");
    return;
  }

  const data = await requestJSON("/order", {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify(body)
  });

  writeOutput(data);
}

document.getElementById("healthBtn").addEventListener("click", async () => {
  writeOutput(await requestJSON("/health"));
});

document.getElementById("startBtn").addEventListener("click", async () => {
  const result = await requestJSON("/game/start", {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ mode: "HVH" })
  });

  writeOutput(result);

  const side = currentSide();
  const state = await requestJSON(`/game/state?side=${side}`);
  renderStateSummary(state, side);
});

document.getElementById("stateBtn").addEventListener("click", refreshState);

document.getElementById("turnBtn").addEventListener("click", async () => {
  writeOutput(await requestJSON("/turn/end", {
    method: "POST"
  }));

  await refreshState();
});

document.getElementById("routesBtn").addEventListener("click", async () => {
  writeOutput(await requestJSON("/analysis/routes?playerId=light"));
});

document.getElementById("interceptBtn").addEventListener("click", async () => {
  writeOutput(await requestJSON("/analysis/intercept?playerId=shadow"));
});

document.getElementById("ordersBtn").addEventListener("click", async () => {
  writeOutput(await requestJSON(`/orders/available?playerId=${currentSide()}&unitId=ring-bearer`));
});

document.getElementById("connectEventsBtn").addEventListener("click", connectEvents);
document.getElementById("loadTemplateBtn").addEventListener("click", loadTemplate);
document.getElementById("sendOrderBtn").addEventListener("click", sendOrder);

loadTemplate();
refreshState().catch(() => {});