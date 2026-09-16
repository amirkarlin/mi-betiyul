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
      subscriptions: (parsed && typeof parsed.subscriptions === "object" && parsed.subscriptions) || {},
      profiles: (parsed && typeof parsed.profiles === "object" && parsed.profiles) || {}
    };
  } catch (e) {
    return { favorites: {}, subscriptions: {}, profiles: {} };
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

// ===== קוד אישי: מאפשר "להיזכר" גם ממכשיר/דפדפן אחר בלי הרשמה אמיתית (בלי סיסמה/אימייל) -
// המשתמש/ת שומר/ת קוד קצר וקריא, ומזין/ה אותו במכשיר החדש כדי לקבל בחזרה את הפרופיל,
// המועדפים וההתראות שלו/ה. פשוט בכוונה, מתאים ל-MVP קהילתי ולא לאבטחה ברמת בנק. =====
const CODE_ALPHABET = "23456789ABCDEFGHJKMNPQRSTUVWXYZ"; // בלי 0/O ו-1/I/L כדי למנוע בלבול חזותי
function generateCodePart() {
  let s = "";
  for (let i = 0; i < 4; i++) s += CODE_ALPHABET[Math.floor(Math.random() * CODE_ALPHABET.length)];
  return s;
}
function generateUniqueCode() {
  let code;
  do {
    code = generateCodePart() + "-" + generateCodePart();
  } while (Object.values(store.profiles).some((p) => p.code === code));
  return code;
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
// ===== מקטע "החברים שלי" בלקוח צריך שם/תמונה/צבע גם לכלבים שאינם בטיול כרגע -
// המידע הזה נלקח מהפרופיל השמור (מסונכרן אוטומטית בכל פעם שמשתמש/ת שומר/ת פרופיל) =====
function getProfileSummaries(ids) {
  const result = {};
  ids.forEach((id) => {
    const p = store.profiles[id];
    if (p) {
      result[id] = { dogName: p.dogName, dogBreed: p.dogBreed || "", dogIcon: p.dogIcon || null, dogColor: p.dogColor || DEFAULT_DOG_COLOR };
    }
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

// ===== תגובת "יאללה" ללחיצה על כפתור בתוך התראת ה-Push עצמה (גם כשהאפליקציה סגורה לגמרי) -
// שולחת התראת Push חוזרת למי ששלח/ה את ההזמנה המקורית =====
app.post("/api/invite-reply", (req, res) => {
  const fromId = req.body && req.body.fromId; // מי ששלח/ה את ההזמנה המקורית - מקבל/ת את ה"יאללה"
  const toId = req.body && req.body.toId;     // מי שהגיב/ה "יאללה" - היה/הייתה יעד ההזמנה
  if (!isValidId(fromId) || !isValidId(toId) || fromId === toId) return res.status(400).json({ ok: false });
  if (!isMutualFavorite(fromId, toId)) return res.status(403).json({ ok: false });

  const toProfile = store.profiles[toId];
  const toName = (toProfile && toProfile.dogName) || "חבר/ה";
  const sub = store.subscriptions[fromId];
  if (sub) {
    const payload = JSON.stringify({
      title: "🎉 " + toName + " ענה/תה יאללה!",
      body: "מתכוננים לצאת לגינה 🐾",
      kind: "yalla"
    });
    webpush.sendNotification(sub, payload).catch((err) => {
      if (err && (err.statusCode === 404 || err.statusCode === 410)) {
        delete store.subscriptions[fromId];
        saveStore();
      }
    });
  }
  // אם המזמין/ה מחובר/ת כרגע (בטיול פעיל) - גם הודעה מיידית בתוך האפליקציה עצמה
  const fromWalker = walkers.get(fromId);
  if (fromWalker && fromWalker.socketId) {
    io.to(fromWalker.socketId).emit("invite:reply", { dogName: toName });
  }
  res.json({ ok: true });
});

// ===== קוד אישי: שמירת/עדכון הפרופיל בשרת (מחזיר קוד קבוע), והתחברות ממכשיר אחר לפי קוד =====
app.post("/api/profile", (req, res) => {
  const id = req.body && req.body.id;
  if (!isValidId(id)) return res.status(400).json({ ok: false });
  const clean = sanitizeProfileInput(req.body);
  if (!clean) return res.status(400).json({ ok: false });
  const existing = store.profiles[id];
  const code = (existing && existing.code) || generateUniqueCode();
  store.profiles[id] = Object.assign({}, clean, { code, updatedAt: new Date().toISOString() });
  saveStore();
  res.json({ ok: true, code });
});

app.post("/api/login-with-code", (req, res) => {
  const raw = req.body && req.body.code;
  const code = typeof raw === "string" ? raw.trim().toUpperCase() : "";
  if (!code) return res.status(400).json({ ok: false, error: "invalid_code" });
  const entry = Object.entries(store.profiles).find(([, p]) => p.code === code);
  if (!entry) return res.status(404).json({ ok: false, error: "not_found" });
  const [id, profile] = entry;
  res.json({
    ok: true,
    id,
    dogName: profile.dogName,
    dogBreed: profile.dogBreed,
    dogIcon: profile.dogIcon,
    dogColor: profile.dogColor,
    code: profile.code
  });
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

function sanitizeProfileInput(body) {
  if (!body || typeof body.dogName !== "string" || !body.dogName.trim()) return null;
  return {
    dogName: sanitizeStr(body.dogName, "כלב/ה"),
    dogBreed: sanitizeStr(body.dogBreed, ""),
    dogIcon: sanitizeDogIcon(body.dogIcon),
    dogColor: sanitizeDogColor(body.dogColor)
  };
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
    const outgoing = getOutgoingFavorites(id);
    socket.emit("favorites:state", { outgoing, incoming: getIncomingFavorites(id), profiles: getProfileSummaries(outgoing) });
  });

  socket.on("favorite:toggle", (data) => {
    if (!data || !isValidId(data.id) || !isValidId(data.targetId) || data.id === data.targetId) return;
    setFavorite(data.id, data.targetId, !!data.on);
    const outgoing = getOutgoingFavorites(data.id);
    socket.emit("favorites:state", { outgoing, incoming: getIncomingFavorites(data.id), profiles: getProfileSummaries(outgoing) });
  });

  // ===== הזמנת "בוא/י לגינה" - דורשת כוכב הדדי, נשלחת דרך Push גם אם היעד/ת מנותק/ת.
  // אפשר לשלוח גם בלי להיות בטיול כרגע - השם לכותרת ההתראה נלקח מהפרופיל השמור =====
  socket.on("invite:send", (data) => {
    if (!data || !isValidId(data.id) || !isValidId(data.targetId) || data.id === data.targetId) return;
    const fromId = data.id, toId = data.targetId;

    if (!isMutualFavorite(fromId, toId)) {
      socket.emit("invite:result", { ok: false, targetId: toId, reason: "not_mutual" });
      return;
    }
    const fromWalker = walkers.get(fromId);
    const fromProfile = store.profiles[fromId];
    const fromName = (fromWalker && fromWalker.dogName) || (fromProfile && fromProfile.dogName) || "חבר/ה מהאפליקציה";
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
      title: "🐾 " + fromName + " מזמינ/ה אותך לגינה!",
      body: "לחצו כדי לפתוח את מי בטיול ולראות איפה, או הגיבו ישר מההתראה.",
      kind: "invite",
      fromId: fromId,
      toId: toId
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
