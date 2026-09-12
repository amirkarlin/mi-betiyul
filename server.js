// מי בטיול? - שרת בסיס: מנהל בזיכרון את רשימת המטיילים הפעילים ומשדר עדכונים
// בזמן אמת דרך Socket.IO. מיועד כ-MVP: לפני הרחבה לייצור אמיתי כדאי להעביר את
// האחסון ל-DB אמיתי (Postgres/Redis) כדי לשרוד ריסטארטים ולתמוך בכמה מופעים.

const fs = require("fs");
const path = require("path");
const http = require("http");
const express = require("express");
const { Server } = require("socket.io");
const webpush = require("web-push");

const PORT = process.env.PORT || 3000;
const STALE_MS = 20 * 60 * 1000;      // מטייל שלא עדכן מיקום 20 דק' נחשב לא פעיל
const CLEANUP_INTERVAL_MS = 60 * 1000; // תדירות בדיקת "ניקוי" מטיילים ישנים
const MAX_STRING_LEN = 40;
const DOG_TYPES = ["floppy", "pointy", "curly", "small", "gallery"];
const DOG_COLOR_RE = /^#[0-9a-fA-F]{6}$/;
const DEFAULT_DOG_COLOR = "#8B5A2B";
const DOG_ICON_RE = /^g([1-9]|1[0-9]|2[0-6])$/; // תמונות הגלריה: g1..g26 (public/dog-icons/g*.png)
const ID_RE = /^[A-Za-z0-9-]{1,64}$/; // תואם למזהים שנוצרים ב-localStorage בצד הלקוח
const INVITE_COOLDOWN_MS = 10 * 60 * 1000; // מגבלת קצב: הזמנה אחת לכל זוג כל 10 דק'

// ===== התראות Push: מפתחות VAPID (נוצרים פעם אחת ונשמרים לקובץ כדי שהמנויים הקיימים
// של המשתמשים לא יתבטלו בכל הפעלה מחדש של השרת) =====
const VAPID_FILE = path.join(__dirname, "vapid.json");
function loadOrCreateVapidKeys() {
  try {
    const parsed = JSON.parse(fs.readFileSync(VAPID_FILE, "utf8"));
    if (parsed && parsed.publicKey && parsed.privateKey) return parsed;
  } catch (e) { /* אין קובץ עדיין - ניצור מפתחות חדשים */ }
  const keys = webpush.generateVAPIDKeys();
  try {
    fs.writeFileSync(VAPID_FILE, JSON.stringify(keys, null, 2));
  } catch (e) {
    console.error("לא הצלחנו לשמור את מפתחות ה-VAPID לקובץ:", e);
  }
  return keys;
}
const VAPID_KEYS = loadOrCreateVapidKeys();
webpush.setVapidDetails("mailto:mi-betiyul@example.com", VAPID_KEYS.publicKey, VAPID_KEYS.privateKey);

// ===== אחסון פשוט מבוסס קובץ למועדפים ולמנויי Push - צריך לשרוד ריסטארט של השרת,
// בניגוד לרשימת המטיילים הפעילה (walkers) שמותר לה להתאפס =====
const DATA_FILE = path.join(__dirname, "data.json");
function loadStore() {
  try {
    const parsed = JSON.parse(fs.readFileSync(DATA_FILE, "utf8"));
    return {
      favorites: (parsed && typeof parsed.favorites === "object" && parsed.favorites) || {},
      subscriptions: (parsed && typeof parsed.subscriptions === "object" && parsed.subscriptions) || {}
    };
  } catch (e) {
    return { favorites: {}, subscriptions: {} };
  }
}
const store = loadStore();
function saveStore() {
  try {
    fs.writeFileSync(DATA_FILE, JSON.stringify(store));
  } catch (e) {
    console.error("שגיאה בשמירת data.json:", e);
  }
}

function isValidId(v) {
  return typeof v === "string" && ID_RE.test(v);
}
function getOutgoingFavorites(id) {
  return Array.isArray(store.favorites[id]) ? store.favorites[id] : [];
}
function setFavorite(id, targetId, on) {
  const list = getOutgoingFavorites(id).slice();
  const idx = list.indexOf(targetId);
  if (on && idx === -1) list.push(targetId);
  if (!on && idx !== -1) list.splice(idx, 1);
  store.favorites[id] = list;
  saveStore();
}
function isMutualFavorite(a, b) {
  return getOutgoingFavorites(a).indexOf(b) !== -1 && getOutgoingFavorites(b).indexOf(a) !== -1;
}
function getIncomingFavorites(id) {
  const result = [];
  Object.keys(store.favorites).forEach((otherId) => {
    if (otherId !== id && getOutgoingFavorites(otherId).indexOf(id) !== -1) result.push(otherId);
  });
  return result;
}
function sanitizePushSubscription(sub) {
  if (!sub || typeof sub !== "object") return null;
  if (typeof sub.endpoint !== "string" || sub.endpoint.length < 10 || sub.endpoint.length > 600) return null;
  if (!sub.keys || typeof sub.keys.p256dh !== "string" || typeof sub.keys.auth !== "string") return null;
  if (sub.keys.p256dh.length > 300 || sub.keys.auth.length > 300) return null;
  return { endpoint: sub.endpoint, keys: { p256dh: sub.keys.p256dh, auth: sub.keys.auth } };
}

const lastInviteAt = new Map(); // "fromId>toId" -> timestamp, לצורך הגבלת קצב (לא חייב לשרוד ריסטארט)

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: false } // אותו מקור בלבד - השרת מגיש גם את הלקוח
});

app.use(express.json({ limit: "20kb" }));
app.use(express.static(path.join(__dirname, "public")));
app.get("/healthz", (req, res) => res.json({ ok: true, walkers: walkers.size }));

app.get("/api/vapid-public-key", (req, res) => {
  res.json({ key: VAPID_KEYS.publicKey });
});

app.post("/api/subscribe", (req, res) => {
  const id = req.body && req.body.id;
  const sub = sanitizePushSubscription(req.body && req.body.subscription);
  if (!isValidId(id) || !sub) return res.status(400).json({ ok: false });
  store.subscriptions[id] = sub;
  saveStore();
  res.json({ ok: true });
});

app.post("/api/unsubscribe", (req, res) => {
  const id = req.body && req.body.id;
  if (isValidId(id) && store.subscriptions[id]) {
    delete store.subscriptions[id];
    saveStore();
  }
  res.json({ ok: true });
});

/** @type {Map<string, {dogName:string, dogBreed:string, dogType:string, dogColor:string, dogIcon:(string|null), lat:number, lng:number, startedAt:string, lastPing:string, socketId:string}>} */
const walkers = new Map();

function sanitizeStr(v, fallback) {
  if (typeof v !== "string") return fallback;
  const trimmed = v.trim().slice(0, MAX_STRING_LEN);
  return trimmed || fallback;
}

function isValidCoord(lat, lng) {
  return typeof lat === "number" && typeof lng === "number" &&
    isFinite(lat) && isFinite(lng) &&
    lat >= -90 && lat <= 90 && lng >= -180 && lng <= 180;
}

function sanitizeDogType(v) {
  return DOG_TYPES.includes(v) ? v : "floppy";
}

function sanitizeDogColor(v) {
  return typeof v === "string" && DOG_COLOR_RE.test(v) ? v : DEFAULT_DOG_COLOR;
}

function sanitizeDogIcon(v) {
  return typeof v === "string" && DOG_ICON_RE.test(v) ? v : null;
}

function publicList() {
  // לא חושפים socketId החוצה
  return Array.from(walkers.entries()).map(([id, w]) => ({
    id,
    dogName: w.dogName,
    dogBreed: w.dogBreed || "",
    dogType: w.dogType,
    dogColor: w.dogColor,
    dogIcon: w.dogIcon || null,
    lat: w.lat,
    lng: w.lng,
    startedAt: w.startedAt,
    lastPing: w.lastPing
  }));
}

function broadcastWalkers() {
  io.emit("walkers:update", publicList());
}

function pruneStale() {
  const now = Date.now();
  let changed = false;
  for (const [id, w] of walkers) {
    if (now - new Date(w.lastPing).getTime() > STALE_MS) {
      walkers.delete(id);
      changed = true;
    }
  }
  if (changed) broadcastWalkers();
}
setInterval(pruneStale, CLEANUP_INTERVAL_MS);

io.on("connection", (socket) => {
  // שולחים ללקוח החדש את המצב הנוכחי מיד עם ההתחברות
  socket.emit("walkers:update", publicList());

  let myWalkerId = null;

  socket.on("checkin", (data) => {
    if (!data || typeof data.id !== "string" || !data.id) return;
    if (!isValidCoord(data.lat, data.lng)) return;

    myWalkerId = data.id;
    const now = new Date().toISOString();
    const existing = walkers.get(data.id);

    walkers.set(data.id, {
      dogName: sanitizeStr(data.dogName, "כלב/ה"),
      dogBreed: sanitizeStr(data.dogBreed, ""),
      dogType: sanitizeDogType(data.dogType),
      dogColor: sanitizeDogColor(data.dogColor),
      dogIcon: sanitizeDogIcon(data.dogIcon),
      lat: data.lat,
      lng: data.lng,
      startedAt: (existing && existing.startedAt) || sanitizeStr(data.startedAt, now) || now,
      lastPing: now,
      socketId: socket.id
    });
    broadcastWalkers();
  });

  socket.on("ping", (data) => {
    if (!data || typeof data.id !== "string") return;
    const w = walkers.get(data.id);
    if (!w) return; // אם אין check-in פעיל, מתעלמים
    if (isValidCoord(data.lat, data.lng)) {
      w.lat = data.lat;
      w.lng = data.lng;
    }
    w.lastPing = new Date().toISOString();
    w.socketId = socket.id;
    broadcastWalkers();
  });

  socket.on("checkout", (data) => {
    const id = data && typeof data.id === "string" ? data.id : myWalkerId;
    if (id && walkers.has(id)) {
      walkers.delete(id);
      broadcastWalkers();
    }
  });

  // ===== מועדפים (כוכב) - נשמרים לצמיתות, לא תלויים בהיותך "בטיול" =====
  socket.on("favorites:sync", (data) => {
    const id = data && data.id;
    if (!isValidId(id)) return;
    socket.emit("favorites:state", { outgoing: getOutgoingFavorites(id), incoming: getIncomingFavorites(id) });
  });

  socket.on("favorite:toggle", (data) => {
    if (!data || !isValidId(data.id) || !isValidId(data.targetId) || data.id === data.targetId) return;
    setFavorite(data.id, data.targetId, !!data.on);
    socket.emit("favorites:state", { outgoing: getOutgoingFavorites(data.id), incoming: getIncomingFavorites(data.id) });
  });

  // ===== הזמנת "בוא/י לגינה" - דורשת כוכב הדדי, נשלחת דרך Push גם אם היעד/ת מנותק/ת =====
  socket.on("invite:send", (data) => {
    if (!data || !isValidId(data.id) || !isValidId(data.targetId) || data.id === data.targetId) return;
    const fromId = data.id, toId = data.targetId;

    if (!isMutualFavorite(fromId, toId)) {
      socket.emit("invite:result", { ok: false, targetId: toId, reason: "not_mutual" });
      return;
    }
    const fromWalker = walkers.get(fromId);
    if (!fromWalker) {
      socket.emit("invite:result", { ok: false, targetId: toId, reason: "not_walking" });
      return;
    }
    const sub = store.subscriptions[toId];
    if (!sub) {
      socket.emit("invite:result", { ok: false, targetId: toId, reason: "no_subscription" });
      return;
    }
    const cooldownKey = fromId + ">" + toId;
    const now = Date.now();
    const last = lastInviteAt.get(cooldownKey) || 0;
    if (now - last < INVITE_COOLDOWN_MS) {
      socket.emit("invite:result", { ok: false, targetId: toId, reason: "cooldown" });
      return;
    }
    lastInviteAt.set(cooldownKey, now);

    const payload = JSON.stringify({
      title: "🐾 " + fromWalker.dogName + " מזמינ/ה אותך לגינה!",
      body: "לחצו כדי לפתוח את מי בטיול ולראות איפה."
    });
    webpush.sendNotification(sub, payload)
      .then(() => {
        socket.emit("invite:result", { ok: true, targetId: toId });
      })
      .catch((err) => {
        if (err && (err.statusCode === 404 || err.statusCode === 410)) {
          delete store.subscriptions[toId];
          saveStore();
        }
        socket.emit("invite:result", { ok: false, targetId: toId, reason: "send_failed" });
      });
  });

  socket.on("disconnect", () => {
    // לא מוחקים מיד בניתוק - ייתכן ריענון דף/רשת רעועה. הניקוי התקופתי (pruneStale)
    // יטפל במטיילים שבאמת לא חוזרים תוך STALE_MS.
  });
});

server.listen(PORT, () => {
  console.log("מי בטיול? מאזין על פורט " + PORT);
});
