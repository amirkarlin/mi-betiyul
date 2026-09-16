// Service worker מינימלי: מאפשר התקנה ("הוספה למסך הבית") ומטמין את מעטפת האפליקציה
// כדי שהיא תיפתח מהר גם ברשת חלשה. לא נוגע כלל בקריאות ל-Socket.IO או API - אלה תמיד
// הולכות לרשת, כי המיקום והנתונים חייבים להיות עדכניים ולא מהמטמון.

const CACHE_NAME = "mi-betiyul-shell-v2";
const SHELL_FILES = ["/", "/manifest.json", "/icon-192.png", "/icon-512.png"];

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(SHELL_FILES))
  );
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((k) => k !== CACHE_NAME).map((k) => caches.delete(k)))
    )
  );
  self.clients.claim();
});

// התראות Push: מגיעות גם כשהאפליקציה סגורה לגמרי, כל עוד יש הרשמה תקפה (subscription).
// להזמנת "בוא/י לגינה" מוסיפים גם כפתור "יאללה" ישירות בתוך ההתראה, כדי שאפשר יהיה
// להגיב מבלי לפתוח את האפליקציה בכלל.
self.addEventListener("push", (event) => {
  let data = {};
  try { data = event.data ? event.data.json() : {}; } catch (e) { /* התעלמות ממטען לא תקין */ }
  const title = data.title || "מי בטיול?";
  const options = {
    body: data.body || "",
    icon: "/icon-192.png",
    badge: "/icon-192.png",
    tag: data.kind === "yalla" ? "mibetiyul-yalla" : "mibetiyul-invite",
    dir: "rtl",
    lang: "he",
    data: data
  };
  if (data.kind === "invite" && data.fromId && data.toId) {
    options.actions = [{ action: "yalla", title: "יאללה! 🐾" }];
  }
  event.waitUntil(self.registration.showNotification(title, options));
});

self.addEventListener("notificationclick", (event) => {
  const data = event.notification.data || {};
  event.notification.close();

  // לחיצה על כפתור "יאללה" - שולחים תגובה לשרת בלי לפתוח/למקד את האפליקציה בכלל
  if (event.action === "yalla" && data.fromId && data.toId) {
    event.waitUntil(
      fetch("/api/invite-reply", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ fromId: data.fromId, toId: data.toId })
      })
        .then(() => self.registration.showNotification("🐾 שלחתם יאללה!", {
          body: "עכשיו נשאר רק לצאת לגינה.",
          icon: "/icon-192.png",
          badge: "/icon-192.png",
          tag: "mibetiyul-yalla-sent",
          dir: "rtl",
          lang: "he"
        }))
        .catch(() => {})
    );
    return;
  }

  event.waitUntil(
    self.clients.matchAll({ type: "window", includeUncontrolled: true }).then((clientsList) => {
      for (const client of clientsList) {
        if ("focus" in client) return client.focus();
      }
      if (self.clients.openWindow) return self.clients.openWindow("/");
    })
  );
});

self.addEventListener("fetch", (event) => {
  const url = new URL(event.request.url);

  // אף פעם לא לגעת בתעבורת socket.io - זו חייבת להגיע ישירות לרשת בזמן אמת
  if (url.pathname.startsWith("/socket.io/")) return;
  if (event.request.method !== "GET") return;

  // עדיפות לרשת (network-first): כך גרסה חדשה שפורסמה מגיעה מיד בפתיחה הבאה,
  // ולא רק אחרי ריענון שני. אם אין רשת (למשל אופליין), חוזרים לגרסה השמורה במטמון.
  event.respondWith(
    fetch(event.request)
      .then((res) => {
        if (res.ok) {
          const clone = res.clone();
          caches.open(CACHE_NAME).then((cache) => cache.put(event.request, clone));
        }
        return res;
      })
      .catch(() => caches.match(event.request))
  );
});
