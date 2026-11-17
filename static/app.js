const PORTO_CENTER = [41.1579, -8.6291];

const map = L.map("map").setView(PORTO_CENTER, 13);
L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
  attribution: "&copy; OpenStreetMap contributors",
}).addTo(map);
setTimeout(() => map.invalidateSize(), 0);
window.addEventListener("resize", () => map.invalidateSize());

let origin = null, destination = null;
let originMarker = null, destMarker = null;
let congestionMarkers = [];

const goBtn = document.getElementById("go");
const hint = document.getElementById("hint");
const err = document.getElementById("err");
const resultEl = document.getElementById("result");
const schematicEl = document.getElementById("schematic");
const schematicEmptyEl = document.getElementById("schematic-empty");

function setDefaultDeparture() {
  const now = new Date(Date.now() - new Date().getTimezoneOffset() * 60000);
  document.getElementById("depart").value = now.toISOString().slice(0, 16);
}
setDefaultDeparture();

function clearLayer(layer) {
  if (layer) map.removeLayer(layer);
}

function updateHint() {
  if (!origin) hint.textContent = "Click the map to set the origin.";
  else if (!destination) hint.textContent = "Now click the map to set the destination.";
  else hint.textContent = "Ready — press Estimate, or click the map again to restart.";
  goBtn.disabled = !(origin && destination);
}

map.on("click", (e) => {
  const { lat, lng } = e.latlng;
  if (!origin || (origin && destination)) {
    // start a fresh pick
    clearLayer(originMarker); clearLayer(destMarker);
    congestionMarkers.forEach(clearLayer); congestionMarkers = [];
    resultEl.innerHTML = ""; err.textContent = "";
    showEmptySchematic();
    origin = { lat, lon: lng };
    destination = null;
    originMarker = L.circleMarker([lat, lng], { radius: 7, color: "#22c55e" }).addTo(map).bindTooltip("Origin");
    document.getElementById("origin-pt").textContent = `${lat.toFixed(5)}, ${lng.toFixed(5)}`;
    document.getElementById("dest-pt").textContent = "click map to set";
  } else {
    destination = { lat, lon: lng };
    destMarker = L.circleMarker([lat, lng], { radius: 7, color: "#ef4444" }).addTo(map).bindTooltip("Destination");
    document.getElementById("dest-pt").textContent = `${lat.toFixed(5)}, ${lng.toFixed(5)}`;
  }
  updateHint();
});

goBtn.addEventListener("click", async () => {
  err.textContent = "";
  resultEl.innerHTML = "";
  goBtn.disabled = true;
  goBtn.textContent = "Estimating…";
  try {
    const departInput = document.getElementById("depart").value;
    const departure_time = new Date(departInput).toISOString();
    const resp = await fetch("/estimate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ origin, destination, departure_time }),
    });
    if (!resp.ok) {
      const body = await resp.json().catch(() => ({}));
      throw new Error(body.detail || `request failed (${resp.status})`);
    }
    const data = await resp.json();
    renderResult(data);
  } catch (e) {
    err.textContent = e.message;
  } finally {
    goBtn.disabled = false;
    goBtn.textContent = "Estimate travel time & route";
  }
});

function showEmptySchematic() {
  schematicEl.innerHTML = "";
  schematicEl.appendChild(schematicEmptyEl);
}

function renderResult(data) {
  congestionMarkers.forEach(clearLayer);
  congestionMarkers = [];

  // Real map: only congestion markers are real-world facts (lat/lon). The route itself is
  // drawn schematically below — a straight line between zone centroids on the real street
  // map is misleading (it cuts through blocks taxis never actually drove through).
  data.congested_zones.forEach((z) => {
    const m = L.circleMarker([z.lat, z.lon], {
      radius: 8, color: "#ef4444", fillColor: "#ef4444", fillOpacity: 0.6,
    }).addTo(map).bindTooltip("Likely congested at this hour");
    congestionMarkers.push(m);
  });
  if (data.congested_zones.length) {
    const group = L.featureGroup(congestionMarkers.concat([originMarker, destMarker].filter(Boolean)));
    map.fitBounds(group.getBounds().pad(0.3));
  }

  renderSchematic(data.route, new Set((data.live && data.live.slower_than_usual) || []));

  const weekendBadge = data.is_weekend ? '<span class="badge weekend">weekend</span>' : "";
  const live = data.live || {};
  let liveHtml = `<div class="stat"><span>Live layer</span><b>${escapeHtml(live.note || "not available")}</b></div>`;
  if (live.available) {
    const [ws, we] = live.window_local.map((t) => new Date(t));
    const fmt = (d) => d.toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit", timeZone: "Europe/Lisbon" });
    const day = ws.toLocaleDateString("en-GB", { timeZone: "Europe/Lisbon" });
    liveHtml = `
      <div class="live-box">
        <div class="live-title">Live layer · Kafka replay</div>
        <div class="stat"><span>Event-time window</span><b>${day} ${fmt(ws)}–${fmt(we)}</b></div>
        <div class="stat"><span>Route zones with live data</span><b>${live.zones_on_route_with_data} / ${data.route.length}</b></div>
        <div class="stat"><span>Slower than usual (&lt;${Math.round(live.slower_than_usual_ratio * 100)}% of normal)</span><b>${live.slower_than_usual.length ? `<span class="badge congest">${live.slower_than_usual.length}</span>` : "0"}</b></div>
        <div class="live-note">Historical replay, not today's traffic. Flags zones only — the ETA is unchanged.</div>
      </div>`;
  }
  resultEl.innerHTML = `
    <div class="stat"><span>Predicted travel time</span><b>${data.predicted_duration_min.toFixed(1)} min</b></div>
    <div class="stat"><span>Straight-line distance</span><b>${data.straight_line_km.toFixed(2)} km</b></div>
    <div class="stat"><span>Departure (local hour)</span><b>${data.departure_local_hour}:00 ${weekendBadge}</b></div>
    <div class="stat"><span>Route zones</span><b>${data.route.length}</b></div>
    <div class="stat"><span>Congested zones on route</span><b>${data.congested_zones.length ? `<span class="badge congest">${data.congested_zones.length}</span>` : "0"}</b></div>
    ${liveHtml}
  `;
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

// Color ramp for a zone's congestion_score (0 = free-flowing, 1 = slowest observed).
function congestionColor(score) {
  if (score >= 0.8) return "#b1540c"; // congested
  if (score >= 0.55) return "#d08a4f"; // busy
  return "#334155"; // normal
}

// Schematic "subway map" view of the route: a staircase path through the grid zones the
// trip actually crosses, laid out on an abstract grid (gx, gy) rather than real
// lat/lon — this is what makes zig-zag zone-to-zone routes read as a sensible path instead
// of a straight line slicing across a real street map.
function renderSchematic(route, slowerIds = new Set()) {
  if (!route || route.length < 2) { showEmptySchematic(); return; }

  const gxs = route.map((z) => z.gx), gys = route.map((z) => z.gy);
  const pad = 1;
  const minGx = Math.min(...gxs) - pad, maxGx = Math.max(...gxs) + pad;
  const minGy = Math.min(...gys) - pad, maxGy = Math.max(...gys) + pad;
  const cols = maxGx - minGx + 1, rows = maxGy - minGy + 1;

  const cell = 52;
  const w = cols * cell, h = rows * cell;
  // gy grows northward (increasing lat); screen y grows downward, so flip.
  const toX = (gx) => (gx - minGx) * cell;
  const toY = (gy) => (maxGy - gy) * cell;

  const routeKeys = new Set(route.map((z) => `${z.gx}_${z.gy}`));
  const scoreByKey = new Map(route.map((z) => [`${z.gx}_${z.gy}`, z.congestion_score]));

  let svg = `<svg viewBox="0 0 ${w} ${h}" preserveAspectRatio="xMidYMid meet">`;

  // background grid: every cell in the bounding box, route cells highlighted, context cells faint
  for (let gx = minGx; gx <= maxGx; gx++) {
    for (let gy = minGy; gy <= maxGy; gy++) {
      const key = `${gx}_${gy}`;
      const x = toX(gx), y = toY(gy);
      const onRoute = routeKeys.has(key);
      const fill = onRoute ? congestionColor(scoreByKey.get(key)) : "#1e293b";
      const opacity = onRoute ? 1 : 0.45;
      svg += `<rect x="${x + 2}" y="${y + 2}" width="${cell - 4}" height="${cell - 4}" rx="6"
                fill="${fill}" opacity="${opacity}" stroke="#0b1220" stroke-width="2"/>`;
    }
  }

  // live layer: zones currently slower than their historical speed for this hour
  route.filter((z) => slowerIds.has(z.zone_id)).forEach((z) => {
    svg += `<rect x="${toX(z.gx) + 1}" y="${toY(z.gy) + 1}" width="${cell - 2}" height="${cell - 2}" rx="7"
              fill="none" stroke="#ef4444" stroke-width="3" stroke-dasharray="5 3">
              <title>Live ${z.live_speed_kmh} km/h vs usual ${z.hist_speed_kmh} km/h</title></rect>`;
  });

  // staircase path connecting consecutive route-zone centers
  const points = route.map((z) => `${toX(z.gx) + cell / 2},${toY(z.gy) + cell / 2}`).join(" ");
  svg += `<polyline points="${points}" fill="none" stroke="#e5e7eb" stroke-width="3"
            stroke-linejoin="round" stroke-linecap="round" opacity="0.9"/>`;

  // A / B badges
  const first = route[0], last = route[route.length - 1];
  const badge = (z, label, color, textColor) => {
    const cx = toX(z.gx) + cell / 2, cy = toY(z.gy) + cell / 2;
    return `<circle cx="${cx}" cy="${cy}" r="13" fill="${color}" stroke="#0b1220" stroke-width="2"/>
            <text x="${cx}" y="${cy}" text-anchor="middle" dominant-baseline="central"
                  font-size="13" font-weight="700" fill="${textColor}">${label}</text>`;
  };
  svg += badge(first, "A", "#22c55e", "#06240f");
  svg += badge(last, "B", "#ef4444", "#2b0000");

  svg += "</svg>";
  schematicEl.innerHTML = svg;
}
