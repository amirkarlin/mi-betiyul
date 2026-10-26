// מי בטיול? - שרת בסיס: מנהל בזיכרון את רשימת המטיילים הפעילים ומשדר עדכונים
// בזמן אמת דרך Socket.IO. מיועד כ-MVP: לפני הרחבה לייצור אמיתי כדאי להעביר את
// האחסון ל-DB אמיתי (Postgres/Redis) כדי לשרוד ריסטארטים ולתמוך בכמה מופעים.

const fs = require("fs");
const path = require("path");
const http = require("http");
const express = require("express");
const { Server } = require("socket.io");
const webpush = require("web-push");
const crypto = require("crypto");
const { createClient } = require("@supabase/supabase-js");

const PORT = process.env.PORT || 3000;
const STALE_MS = 20 * 60 * 1000;      // מטייל שלא עדכן מיקום 20 דק' נחשב לא פעיל
const CLEANUP_INTERVAL_MS = 60 * 1000; // תדירות בדיקת "ניקוי" מטיילים ישנים
const MAX_STRING_LEN = 40;
const DOG_TYPES = ["floppy", "pointy", "curly", "small", "gallery"];
const DOG_COLOR_RE = /^#[0-9a-fA-F]{6}$/;
const DEFAULT_DOG_COLOR = "#8B5A2B";
const DEFAULT_DOG_NAME = "כלב/ה"; // פלייסהולדר בלבד (למשל בתוויות ריקות) - אף פעם לא מוצג כאילו הוא שם אמיתי בהודעות בין משתמשים
// ממיר שם כלב ל"שם תצוגה" בטוח להצגה בהודעה בין משתמשים (הזמנה/יאללה/התראה) - אם השם
// חסר, או שהוא בפועל רק הפלייסהולדר הגנרי (למשל פרופיל שנשמר פעם בלי שם אמיתי), מחזירים
// null כדי שהקורא/ת יציבו fallback ניטרלי ("חבר/ה") במקום להציג את המילה "כלב/ה" כשם
function displayDogName(name) {
  if (typeof name !== "string") return null;
  const trimmed = name.trim();
  if (!trimmed || trimmed === DEFAULT_DOG_NAME) return null;
  return trimmed;
}
// מספר תמונות הגלריה הקיימות כרגע (public/dog-icons/g1.png ... g<GALLERY_ICON_COUNT>.png).
// *** צריך להתעדכן ל-GALLERY_COUNT ב-public/index.html בכל פעם שמוסיפים תמונות לגלריה! ***
// אם המספר כאן קטן מדי, השרת "ינקה" (sanitizeDogIcon יחזיר null) כל dogIcon עם מספר
// גבוה מהטווח - והתוצאה היא שמשתמש/ת שבחר/ה תמונה חדשה יראה את האייקון שלו/שלה כראוי
// (כי זה מגיע מהפרופיל המקומי, בלי לעבור sanitize), אבל כל שאר המשתמשים יראו עבורו/ה
// רק אייקון כף-רגל גנרי במקום - בדיוק הבאג שתוקן ב-2026-10-16.
const GALLERY_ICON_COUNT = 86;
const ID_RE = /^[A-Za-z0-9-]{1,64}$/; // תואם למזהים שנוצרים ב-localStorage בצד הלקוח
const INVITE_COOLDOWN_MS = 10 * 60 * 1000; // מגבלת קצב: הזמנה אחת לכל זוג כל 10 דק'

// ===== אחסון קבוע חיצוני (Supabase) - חובה כדי לשרוד ריסטארטים של Render =====
// שירות Web Service בתוכנית החינמית של Render מוחק את כל הדיסק המקומי בכל
// הפעלה מחדש/שינה/דיפלוי (אין דיסק קבוע בתוכנית החינמית). לכן קבצי data.json/vapid.json
// המקומיים משמשים רק כגיבוי לזמן ריצה וכ-fallback כשאין Supabase מוגדר - מקור האמת
// האמיתי, שבאמת שורד ריסטארט, הוא טבלת kv_store ב-Supabase.
// יוצרים פרויקט חינמי ב-supabase.com, מריצים את ה-SQL הבא בעורך ה-SQL שלו:
//   create table if not exists kv_store (
//     key text primary key,
//     value jsonb not null,
//     updated_at timestamptz not null default now()
//   );
// ואז מגדירים ב-Render (Environment) את SUPABASE_URL ו-SUPABASE_SERVICE_KEY
// (המפתח מסוג service_role מתוך Project Settings > API בפרויקט ב-Supabase).
const SUPABASE_URL = process.env.SUPABASE_URL || "";
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY || "";
const supabase = (SUPABASE_URL && SUPABASE_SERVICE_KEY)
  ? createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY)
  : null;
if (!supabase) {
  console.warn("Supabase לא מוגדר (SUPABASE_URL/SUPABASE_SERVICE_KEY) - האחסון יתאפס בכל הפעלה מחדש של השרת.");
}

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
let VAPID_KEYS = loadOrCreateVapidKeys();
webpush.setVapidDetails("mailto:mi-betiyul@example.com", VAPID_KEYS.publicKey, VAPID_KEYS.privateKey);

// ===== אחסון פשוט מבוסס קובץ למועדפים ולמנויי Push - צריך לשרוד ריסטארט של השרת,
// בניגוד לרשימת המטיילים הפעילה (walkers) שמותר לה להתאפס =====
const DATA_FILE = path.join(__dirname, "data.json");
function normalizeStoreShape(parsed) {
  return {
    favorites: (parsed && typeof parsed.favorites === "object" && parsed.favorites) || {},
    subscriptions: (parsed && typeof parsed.subscriptions === "object" && parsed.subscriptions) || {},
    profiles: (parsed && typeof parsed.profiles === "object" && parsed.profiles) || {},
    walkHistory: (parsed && typeof parsed.walkHistory === "object" && parsed.walkHistory) || {},
    notifications: (parsed && typeof parsed.notifications === "object" && parsed.notifications) || {},
    // ===== הצעות נקודת מפגש פעילות/שהוכרעו - מערך שטוח (לא keyed לפי משתמש, כי לכל הצעה
    // יש שני צדדים - fromId ו-toId - וכל אחד מהם צריך לראות אותה) - ראו "הצעת נקודת מפגש
    // כסיכה חיה במפה" בהערות הטכניות למעלה =====
    meetups: Array.isArray(parsed && parsed.meetups) ? parsed.meetups : []
  };
}
function loadStore() {
  try {
    return normalizeStoreShape(JSON.parse(fs.readFileSync(DATA_FILE, "utf8")));
  } catch (e) {
    return { favorites: {}, subscriptions: {}, profiles: {}, walkHistory: {}, notifications: {}, meetups: [] };
  }
}
const store = loadStore();
function saveStore() {
  try {
    fs.writeFileSync(DATA_FILE, JSON.stringify(store));
  } catch (e) {
    console.error("שגיאה בשמירת data.json:", e);
  }
  if (supabase) {
    supabase.from("kv_store")
      .upsert({ key: "app_data", value: store, updated_at: new Date().toISOString() })
      .then(({ error }) => { if (error) console.error("שגיאה בשמירת הנתונים ל-Supabase:", error.message); })
      .catch((e) => console.error("שגיאה בשמירת הנתונים ל-Supabase:", e.message));
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
// מנקה/מקצר תווית מיקום חופשית (כתובת/שם מקום) שמגיעה מהצעת "נקודת מפגש" - מוצגת
// למשתמש/ת אחר/ת (בהתראת Push וב"התראות שקיבלתי"), לכן חייבת לעבור חיתוך אורך בטוח
function sanitizeMeetLabel(v) {
  if (typeof v !== "string") return "";
  return v.trim().slice(0, 160);
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
// ===== "הטיולים שלי" - יומן משכי טיולים, נשמר לצמיתות לפי מזהה (שרד גם ריסטארט
// של השרת, וגם מעבר בין מכשירים דרך הקוד האישי, בדיוק כמו מועדפים ופרופיל) =====
const MAX_HISTORY_PER_USER = 200; // מגבלה כדי שקובץ הנתונים לא יגדל בלי סוף
function addWalkHistoryEntry(id, startedAtIso, endedAtIso) {
  if (!isValidId(id)) return;
  const startedMs = new Date(startedAtIso).getTime();
  const endedMs = new Date(endedAtIso).getTime();
  if (!isFinite(startedMs) || !isFinite(endedMs) || endedMs <= startedMs) return;
  const durationMin = Math.max(1, Math.round((endedMs - startedMs) / 60000));
  const list = Array.isArray(store.walkHistory[id]) ? store.walkHistory[id] : [];
  list.push({ startedAt: startedAtIso, endedAt: endedAtIso, durationMin });
  store.walkHistory[id] = list.slice(-MAX_HISTORY_PER_USER); // שומרים רק את האחרונים
  saveStore();
}
function getWalkHistory(id) {
  return Array.isArray(store.walkHistory[id]) ? store.walkHistory[id].slice().reverse() : []; // חדש לישן
}

// ===== "התראות שקיבלתי" - יומן הודעות (הזמנות/תגובות יאללה) לפי מזהה, נשמר לצמיתות
// בדיוק כמו יומן הטיולים, כדי שיישאר גם אחרי ריסטארט או מעבר בין מכשירים =====
const MAX_NOTIFICATIONS_PER_USER = 100; // מגבלה כדי שקובץ הנתונים לא יגדל בלי סוף
// entry: { message, kind: "invite"|"yalla"|"info", fromId?, toId? } - fromId/toId חובה כש-kind==="invite",
// כדי שאפשר יהיה להציג כפתור "יאללה" ולשלוח תגובה ישירות מתוך רשימת "התראות שקיבלתי" עצמה
// (ולא רק דרך כפתור בהתראת ה-Push או קישור עומק, שלא תמיד עובדים - למשל באייפון)
function addNotification(id, entry) {
  if (!isValidId(id) || !entry || typeof entry.message !== "string" || !entry.message.trim()) return;
  const kind = (entry.kind === "invite" || entry.kind === "yalla") ? entry.kind : "info";
  const clean = {
    id: crypto.randomUUID(),
    kind,
    message: entry.message.trim().slice(0, 300),
    at: new Date().toISOString(),
    read: false
  };
  if (kind === "invite") {
    if (!isValidId(entry.fromId) || !isValidId(entry.toId)) return;
    clean.fromId = entry.fromId;
    clean.toId = entry.toId;
    clean.replied = false;
    // ===== "הצעת נקודת מפגש" (אופציונלי) - כתובת/נקודה במפה שצורפה להזמנה, כדי שאפשר
    // יהיה להציג אותה ב"התראות שקיבלתי" ולפתוח ניווט אליה. isValidCoord מוגדר בהמשך
    // הקובץ, אבל הצהרת function מורמת (hoisting) אז הקריאה כאן תקינה =====
    if (isValidCoord(entry.meetLat, entry.meetLng)) {
      clean.meetLat = entry.meetLat;
      clean.meetLng = entry.meetLng;
      clean.meetLabel = sanitizeMeetLabel(entry.meetLabel);
    }
  }
  const list = Array.isArray(store.notifications[id]) ? store.notifications[id] : [];
  list.push(clean);
  store.notifications[id] = list.slice(-MAX_NOTIFICATIONS_PER_USER); // שומרים רק את האחרונות
  saveStore();
  return clean; // מוחזר כדי שאפשר יהיה לקשר אליו הצעת מפגש (ראו createMeetup) לצורך ביטול מאוחר יותר
}
// כש"מבטלים" הצעת מפגש (ראו meetup:cancel) - מסמנים גם את רשומת ההתראה המקורית אצל מי
// שקיבל/ה אותה כמבוטלת, כדי שכפתור "יאללה" ייעלם משם וכדי שיוצג שם שההצעה בוטלה
function markNotificationCancelled(toId, notifId) {
  const list = store.notifications[toId];
  if (!Array.isArray(list) || !notifId) return;
  const n = list.find((x) => x.id === notifId);
  if (n && !n.cancelled) {
    n.cancelled = true;
    saveStore();
  }
}
function getNotifications(id) {
  return Array.isArray(store.notifications[id]) ? store.notifications[id].slice().reverse() : []; // חדש לישן
}
function markNotificationsRead(id) {
  const list = store.notifications[id];
  if (!Array.isArray(list) || !list.length) return;
  let changed = false;
  list.forEach((n) => { if (!n.read) { n.read = true; changed = true; } });
  if (changed) saveStore();
}
// כשעונים בפועל "יאללה" להזמנה (מכל ערוץ - כפתור בהתראה, קישור עומק, או ישירות מתוך
// רשימת "התראות שקיבלתי") - מסמנים את ההזמנה כ"נענתה" כדי שכפתור התגובה ייעלם משם
function markInviteNotificationsReplied(toId, fromId) {
  const list = store.notifications[toId];
  if (!Array.isArray(list) || !list.length) return;
  let changed = false;
  list.forEach((n) => {
    if (n.kind === "invite" && n.fromId === fromId && !n.replied) {
      n.replied = true;
      if (!n.read) n.read = true;
      changed = true;
    }
  });
  if (changed) saveStore();
}

// ===== הצעת נקודת מפגש כסיכה חיה על המפה =====
// בנוסף להתראה/לרשומה ב"התראות שקיבלתי" (שתלויות ב"invite:send" הקיים), הצעת מפגש עם
// מיקום מקבלת גם רשומה משלה כאן - כדי שאפשר יהיה להציג אותה כסיכה חיה על המפה גם אצל
// השולח/ת וגם אצל מי שקיבל/ה אותה (לא רק כהודעת טקסט), ולאפשר ביטול שלה ע"י השולח/ת.
// מערך שטוח (לא keyed לפי משתמש) כי לכל רשומה שני "בעלים" - fromId ו-toId.
const MAX_MEETUPS = 300; // מגבלת גודל כדי שקובץ הנתונים לא יגדל בלי סוף
const MEETUP_RESOLVED_TTL_MS = 24 * 60 * 60 * 1000; // הצעות שהוכרעו (התקבלו/בוטלו) נשמרות רק יממה, להיסטוריה קצרה בלבד
function pruneMeetups() {
  const cutoff = Date.now() - MEETUP_RESOLVED_TTL_MS;
  store.meetups = store.meetups.filter((m) => m.status === "pending" || new Date(m.createdAt).getTime() > cutoff);
  if (store.meetups.length > MAX_MEETUPS) store.meetups = store.meetups.slice(-MAX_MEETUPS);
}
function createMeetup(fromId, toId, fromDogName, lat, lng, label, notifId) {
  const m = {
    id: crypto.randomUUID(),
    fromId, toId,
    fromDogName: fromDogName || "",
    lat, lng,
    label: sanitizeMeetLabel(label) || "נקודה שסומנה במפה",
    status: "pending", // "pending" | "accepted" | "cancelled"
    createdAt: new Date().toISOString(),
    notifId: notifId || null
  };
  store.meetups.push(m);
  pruneMeetups();
  saveStore();
  return m;
}
// כש"יאללה" נענה בין שני צדדים - כל הצעת מפגש ממתינה ביניהם נחשבת "התקבלה" ומוסרת מהמפה
function acceptMeetupsBetween(fromId, toId) {
  let changed = false;
  store.meetups.forEach((m) => {
    if (m.status === "pending" && m.fromId === fromId && m.toId === toId) {
      m.status = "accepted";
      changed = true;
    }
  });
  if (changed) saveStore();
}
// רק השולח/ת של הצעה יכול/ה לבטל אותה. מחזיר את הרשומה שבוטלה (או null אם לא נמצאה/לא ניתן)
function cancelMeetup(meetupId, requesterId) {
  const m = store.meetups.find((x) => x.id === meetupId);
  if (!m || m.status !== "pending" || m.fromId !== requesterId) return null;
  m.status = "cancelled";
  saveStore();
  return m;
}
// רק מי שקיבל/ה הצעת מפגש (toId) יכול/ה "לדחות" אותה (לא יכול/ה להגיע) - סימטרי לביטול
// של השולח/ת, אבל בכיוון ההפוך. מחזיר את הרשומה שנדחתה (או null אם לא נמצאה/לא ניתן)
function declineMeetup(meetupId, requesterId) {
  const m = store.meetups.find((x) => x.id === meetupId);
  if (!m || m.status !== "pending" || m.toId !== requesterId) return null;
  m.status = "cancelled";
  saveStore();
  return m;
}
// שדות ציבוריים בלבד (לא חושפים notifId - שימושי רק בשרת לקישור לביטול)
function publicMeetup(m) {
  return { id: m.id, fromId: m.fromId, toId: m.toId, fromDogName: m.fromDogName, lat: m.lat, lng: m.lng, label: m.label, status: m.status, createdAt: m.createdAt };
}
function getActiveMeetupsFor(id) {
  return store.meetups.filter((m) => m.status === "pending" && (m.fromId === id || m.toId === id)).map(publicMeetup);
}
function sanitizePushSubscription(sub) {
  if (!sub || typeof sub !== "object") return null;
  if (typeof sub.endpoint !== "string" || sub.endpoint.length < 10 || sub.endpoint.length > 600) return null;
  if (!sub.keys || typeof sub.keys.p256dh !== "string" || typeof sub.keys.auth !== "string") return null;
  if (sub.keys.p256dh.length > 300 || sub.keys.auth.length > 300) return null;
  return { endpoint: sub.endpoint, keys: { p256dh: sub.keys.p256dh, auth: sub.keys.auth } };
}

const lastInviteAt = new Map(); // "fromId>toId" -> timestamp, לצורך הגבלת קצב (לא חייב לשרוד ריסטארט)

// ===== מיפוי מזהה-משתמש/ת -> socket.id נוכחי - כדי שאפשר יהיה לשדר עדכונים (כמו סיכת הצעת
// מפגש) ישירות למישהו/י *גם כשהוא/היא לא בטיול פעיל כרגע* (walkers כולל רק מטיילים פעילים).
// מתעדכן בכל אירוע socket "מזוהה" (checkin/ping/sync/invite) שמגיע עם id תקין =====
const socketsByUserId = new Map(); // id -> socket.id
function registerIdentity(socket, id) {
  if (!isValidId(id)) return;
  socketsByUserId.set(id, socket.id);
  socket.data.rid = id;
}
function emitToUser(id, event, payload) {
  const sId = socketsByUserId.get(id);
  if (sId) io.to(sId).emit(event, payload);
}
function broadcastMeetupsTo(id) {
  emitToUser(id, "meetups:state", { entries: getActiveMeetupsFor(id) });
}

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
  const toName = displayDogName(toProfile && toProfile.dogName) || "חבר/ה";
  addNotification(fromId, { kind: "yalla", message: "🎉 " + toName + " ענה/תה יאללה! מתכוננים לצאת לגינה" });
  markInviteNotificationsReplied(toId, fromId); // מסתיר את כפתור "יאללה" ברשימת ההתראות של מי שהגיב/ה
  acceptMeetupsBetween(fromId, toId); // כל הצעת מפגש ממתינה בין השניים מוסרת מהמפה משני הצדדים
  broadcastMeetupsTo(fromId);
  broadcastMeetupsTo(toId);
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

// ===== גיאוקוד כתובת ל-lat/lng, דרך Nominatim (OpenStreetMap) - חינמי, בלי מפתח API.
// עושים את זה בשרת (לא ישירות מהדפדפן) כדי לצרף User-Agent תקין כנדרש במדיניות השימוש
// של Nominatim, ולשמור על קצב עדין (בקשה אחת בשנייה לכל היותר) עם מטמון פשוט בזיכרון =====
const GEOCODE_CACHE = new Map(); // "כתובת מנורמלת" -> {lat, lng, displayName, at}
const GEOCODE_CACHE_TTL_MS = 24 * 60 * 60 * 1000;
let lastGeocodeAt = 0;
app.post("/api/geocode", async (req, res) => {
  const raw = req.body && req.body.address;
  const address = typeof raw === "string" ? raw.trim().slice(0, 200) : "";
  if (!address) return res.status(400).json({ ok: false });
  const cacheKey = address.toLowerCase();
  const cached = GEOCODE_CACHE.get(cacheKey);
  if (cached && Date.now() - cached.at < GEOCODE_CACHE_TTL_MS) {
    return res.json({ ok: true, lat: cached.lat, lng: cached.lng, displayName: cached.displayName });
  }
  const now = Date.now();
  const wait = Math.max(0, 1100 - (now - lastGeocodeAt));
  if (wait > 0) await new Promise((r) => setTimeout(r, wait));
  lastGeocodeAt = Date.now();
  try {
    const url = "https://nominatim.openstreetmap.org/search?format=json&limit=1&q=" + encodeURIComponent(address);
    const resp = await fetch(url, {
      headers: { "User-Agent": "mi-betiyul-app/1.0 (dog walking app, personal project)" }
    });
    if (!resp.ok) return res.status(502).json({ ok: false, reason: "geocode_failed" });
    const data = await resp.json();
    if (!Array.isArray(data) || !data.length) return res.json({ ok: false, reason: "not_found" });
    const lat = parseFloat(data[0].lat);
    const lng = parseFloat(data[0].lon);
    if (!isFinite(lat) || !isFinite(lng)) return res.json({ ok: false, reason: "not_found" });
    const displayName = String(data[0].display_name || address).slice(0, 200);
    GEOCODE_CACHE.set(cacheKey, { lat, lng, displayName, at: Date.now() });
    res.json({ ok: true, lat, lng, displayName });
  } catch (e) {
    res.status(502).json({ ok: false, reason: "geocode_failed" });
  }
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
  if (typeof v !== "string") return null;
  const m = /^g([1-9][0-9]*)$/.exec(v);
  if (!m) return null;
  const n = parseInt(m[1], 10);
  return n >= 1 && n <= GALLERY_ICON_COUNT ? v : null;
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
      // רושמים ליומן הטיולים גם ניתוק שקט (למשל אפליקציה שנסגרה בלי "סיימתי טיול") -
      // זמן הסיום המשוער הוא ה-ping האחרון שקיבלנו, לא "עכשיו"
      addWalkHistoryEntry(id, w.startedAt, w.lastPing);
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
    registerIdentity(socket, data.id);
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
    registerIdentity(socket, data.id);
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
      const w = walkers.get(id);
      walkers.delete(id);
      addWalkHistoryEntry(id, w.startedAt, new Date().toISOString());
      socket.emit("history:state", { entries: getWalkHistory(id) });
      broadcastWalkers();
    }
  });

  // ===== "הטיולים שלי" - יומן משכי טיולים, נשמר לצמיתות לפי מזהה =====
  socket.on("history:sync", (data) => {
    const id = data && data.id;
    if (!isValidId(id)) return;
    registerIdentity(socket, id);
    socket.emit("history:state", { entries: getWalkHistory(id) });
  });

  // ===== "התראות שקיבלתי" - יומן הודעות שנשמר לצמיתות לפי מזהה =====
  socket.on("notifications:sync", (data) => {
    const id = data && data.id;
    if (!isValidId(id)) return;
    registerIdentity(socket, id);
    socket.emit("notifications:state", { entries: getNotifications(id) });
  });

  socket.on("notifications:read", (data) => {
    const id = data && data.id;
    if (!isValidId(id)) return;
    markNotificationsRead(id);
    socket.emit("notifications:state", { entries: getNotifications(id) });
  });

  // ===== מועדפים (כוכב) - נשמרים לצמיתות, לא תלויים בהיותך "בטיול" =====
  socket.on("favorites:sync", (data) => {
    const id = data && data.id;
    if (!isValidId(id)) return;
    registerIdentity(socket, id);
    const outgoing = getOutgoingFavorites(id);
    socket.emit("favorites:state", { outgoing, incoming: getIncomingFavorites(id), profiles: getProfileSummaries(outgoing) });
  });

  // ===== הצעות נקודת מפגש פעילות (סיכות על המפה) - נשלח גם ביוזמת הלקוח (עם טעינת הדף)
  // וגם בכל פעם שמשהו משתנה (הצעה חדשה/התקבלה/בוטלה), לכל צד רלוונטי =====
  socket.on("meetups:sync", (data) => {
    const id = data && data.id;
    if (!isValidId(id)) return;
    registerIdentity(socket, id);
    socket.emit("meetups:state", { entries: getActiveMeetupsFor(id) });
  });

  // רק מי ששלח/ה הצעת מפגש יכול/ה לבטל אותה - מסיר את הסיכה משני הצדדים ומסמן את
  // ההתראה המקורית אצל מי שקיבל כמבוטלת (כדי שכפתור "יאללה" ייעלם גם שם)
  socket.on("meetup:cancel", (data) => {
    const id = data && data.id;
    if (!isValidId(id) || typeof (data && data.meetupId) !== "string") return;
    registerIdentity(socket, id);
    const m = cancelMeetup(data.meetupId, id);
    if (!m) return;
    if (m.notifId) {
      markNotificationCancelled(m.toId, m.notifId);
      emitToUser(m.toId, "notifications:state", { entries: getNotifications(m.toId) });
    }
    broadcastMeetupsTo(m.fromId);
    broadcastMeetupsTo(m.toId);
  });

  // גם מי שקיבל/ה הצעת מפגש יכול/ה לדחות אותה ("לא יכול/ה להגיע") - מסיר את הסיכה משני
  // הצדדים, ושולח התראה חדשה (לא שינוי של ההתראה הקיימת, כי זו כבר אצל מי שקיבל ולא אצל
  // מי ששלח) למי ששלח/ה את ההצעה, עם שם הכלב שדחה. בלי Push (כמו meetup:cancel) - רק
  // עדכון חי דרך socket + רשומה מתמידה ב"התראות שקיבלתי" של השולח/ת
  socket.on("meetup:decline", (data) => {
    const id = data && data.id;
    if (!isValidId(id) || typeof (data && data.meetupId) !== "string") return;
    registerIdentity(socket, id);
    const m = declineMeetup(data.meetupId, id);
    if (!m) return;
    const toProfile = store.profiles[id];
    const toWalker = walkers.get(id);
    const toName = displayDogName(toProfile && toProfile.dogName) || displayDogName(toWalker && toWalker.dogName) || "חבר/ה";
    addNotification(m.fromId, { kind: "info", message: "🚫 " + toName + " לא יכול/ה להגיע" });
    emitToUser(m.fromId, "notifications:state", { entries: getNotifications(m.fromId) });
    broadcastMeetupsTo(m.fromId);
    broadcastMeetupsTo(m.toId);
  });

  socket.on("favorite:toggle", (data) => {
    if (!data || !isValidId(data.id) || !isValidId(data.targetId) || data.id === data.targetId) return;
    setFavorite(data.id, data.targetId, !!data.on);
    const outgoing = getOutgoingFavorites(data.id);
    socket.emit("favorites:state", { outgoing, incoming: getIncomingFavorites(data.id), profiles: getProfileSummaries(outgoing) });
  });

  // ===== הזמנת "בוא/י לגינה" - דורשת כוכב הדדי, נשלחת דרך Push גם אם היעד/ת מנותק/ת.
  // אפשר לשלוח גם בלי להיות בטיול כרגע - השם לכותרת ההתראה נלקח מהפרופיל השמור.
  // אפשר גם לצרף "הצעת נקודת מפגש" (meetLat/meetLng/meetLabel אופציונליים, שנבחרו
  // בלקוח ע"י הקלדת כתובת שעברה גיאוקוד או לחיצה על מיני-מפה) - זו אותה תשתית הזמנה/
  // יאללה בדיוק, רק עם תוכן התראה שונה כשיש מיקום מצורף =====
  socket.on("invite:send", (data) => {
    if (!data || !isValidId(data.id) || !isValidId(data.targetId) || data.id === data.targetId) return;
    const fromId = data.id, toId = data.targetId;
    registerIdentity(socket, fromId);

    if (!isMutualFavorite(fromId, toId)) {
      socket.emit("invite:result", { ok: false, targetId: toId, reason: "not_mutual" });
      return;
    }
    const fromWalker = walkers.get(fromId);
    const fromProfile = store.profiles[fromId];
    // מעדיפים את השם השמור בפרופיל (הזהות הקבועה) על פני השם הרגעי בטיול הפעיל -
    // כך שגם אם הצ׳ק-אין נעשה עם שם לא מעודכן, ההודעה עדיין תציג את השם הנכון
    const fromName = displayDogName(fromProfile && fromProfile.dogName) || displayDogName(fromWalker && fromWalker.dogName) || "חבר/ה";
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

    const hasLocation = isValidCoord(data.meetLat, data.meetLng);
    const meetLabel = hasLocation ? (sanitizeMeetLabel(data.meetLabel) || "נקודה שסומנה במפה") : "";

    const pushTitle = hasLocation
      ? "📍 " + fromName + " מציע/ה נקודת מפגש!"
      : "🐾 " + fromName + " מזמינ/ה אותך לגינה!";
    const pushBody = hasLocation
      ? "מוצע להיפגש ב: " + meetLabel + " · לחצו כדי לפתוח ולהגיב."
      : "לחצו כדי לפתוח את מי בטיול ולראות איפה, או הגיבו ישר מההתראה.";
    const notifMessage = hasLocation
      ? "📍 " + fromName + " מציע/ה להיפגש ב-" + meetLabel
      : "🐾 " + fromName + " מזמינ/ה אתכם לגינה!";

    const payload = JSON.stringify(Object.assign({
      title: pushTitle,
      body: pushBody,
      kind: "invite",
      fromId: fromId,
      toId: toId
    }, hasLocation ? { meetLat: data.meetLat, meetLng: data.meetLng, meetLabel: meetLabel } : {}));
    webpush.sendNotification(sub, payload)
      .then(() => {
        const notifEntry = { kind: "invite", message: notifMessage, fromId: fromId, toId: toId };
        if (hasLocation) {
          notifEntry.meetLat = data.meetLat;
          notifEntry.meetLng = data.meetLng;
          notifEntry.meetLabel = meetLabel;
        }
        const createdNotif = addNotification(toId, notifEntry);
        if (hasLocation) {
          // רושמים גם כ"הצעת מפגש" עצמאית כדי שתוצג כסיכה חיה על המפה אצל שני הצדדים
          // (לא רק כשורת טקסט ב"התראות שקיבלתי") - ראו "הצעת נקודת מפגש כסיכה חיה במפה" למעלה
          const meetup = createMeetup(fromId, toId, fromName, data.meetLat, data.meetLng, meetLabel, createdNotif && createdNotif.id);
          broadcastMeetupsTo(fromId);
          broadcastMeetupsTo(toId);
        }
        socket.emit("invite:result", { ok: true, targetId: toId, hasLocation: hasLocation });
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
    // כן מנקים את מיפוי socketsByUserId - אבל רק אם אף חיבור אחר (למשל טאב חדש) לא
    // כבר "תפס" את אותו מזהה בינתיים, כדי לא למחוק בטעות מיפוי עדכני יותר
    const rid = socket.data && socket.data.rid;
    if (rid && socketsByUserId.get(rid) === socket.id) socketsByUserId.delete(rid);
  });
});

// ===== סנכרון עם Supabase (אם מוגדר) לפני שהשרת מתחיל לקבל בקשות - כך אנחנו תמיד
// מתחילים עם הנתונים האמיתיים והמפתחות האמיתיים, ולא עם ברירת מחדל ריקה/חדשה =====
async function bootstrapPersistence() {
  if (!supabase) return; // אין Supabase מוגדר - ממשיכים עם הקבצים המקומיים (data.json/vapid.json) כרגיל

  try {
    const { data: vapidRow, error: vapidErr } = await supabase
      .from("kv_store").select("value").eq("key", "vapid").maybeSingle();
    if (vapidErr) throw vapidErr;
    if (vapidRow && vapidRow.value && vapidRow.value.publicKey && vapidRow.value.privateKey) {
      VAPID_KEYS = vapidRow.value;
      webpush.setVapidDetails("mailto:mi-betiyul@example.com", VAPID_KEYS.publicKey, VAPID_KEYS.privateKey);
    } else {
      await supabase.from("kv_store").upsert({ key: "vapid", value: VAPID_KEYS, updated_at: new Date().toISOString() });
    }
  } catch (e) {
    console.error("שגיאה בסנכרון מפתחות VAPID עם Supabase - ממשיכים עם המפתחות המקומיים:", e.message);
  }

  try {
    const { data: storeRow, error: storeErr } = await supabase
      .from("kv_store").select("value").eq("key", "app_data").maybeSingle();
    if (storeErr) throw storeErr;
    if (storeRow && storeRow.value) {
      Object.assign(store, normalizeStoreShape(storeRow.value));
      console.log("הנתונים (מועדפים/התראות/פרופילים/טיולים) נטענו בהצלחה מ-Supabase.");
    } else {
      saveStore(); // אין עדיין נתונים ב-Supabase (הפעלה ראשונה) - נאתחל שם עם מה שיש לנו מקומית
    }
  } catch (e) {
    console.error("שגיאה בטעינת הנתונים מ-Supabase - ממשיכים עם הקובץ המקומי:", e.message);
  }
}

bootstrapPersistence().finally(() => {
  server.listen(PORT, () => {
    console.log("מי בטיול? מאזין על פורט " + PORT);
  });
});
