// ============================================================
//  LocalDesk - Serveur relais (a heberger, ex: Render.com)
//  Role : mettre en relation deux PC sur des reseaux differents.
//  - Chaque HOTE se connecte et s'enregistre avec son ID.
//  - Un CONTROLEUR se connecte en donnant l'ID cible.
//  - Le relais relie les deux et fait passer TOUS les messages
//    (texte + images binaires) de l'un a l'autre, sans les lire.
//
//  Aucune donnee n'est stockee. Le consentement reste gere par
//  l'app elle-meme (l'hote doit toujours accepter).
// ============================================================

const http = require("http");
const crypto = require("crypto");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;

// ============================================================
//  RELAIS API « ParaHost Control » (ajout)
//  Le bot ParaHost se connecte ici (sortant) avec le role "parahost-bot".
//  L'application fait ses requetes HTTP /api/... sur ce relais, qui les
//  transmet au bot et renvoie la reponse. Aucun port a ouvrir sur l'hebergeur du bot.
// ============================================================
const RELAY_SECRET = process.env.RELAY_SECRET || "";
let parahostBot = null;            // socket du bot ParaHost connecte
const apiPending = new Map();      // id -> { res, timer } (requetes de l'app en attente de reponse)

function handleApiRequest(req, res) {
  if (!parahostBot || parahostBot.readyState !== 1) {
    res.writeHead(502, { "Content-Type": "application/json; charset=utf-8" });
    res.end(JSON.stringify({ ok: false, error: "Bot hors ligne (le relais fonctionne, mais le bot n'est pas connecte au relais)." }));
    return;
  }
  const chunks = [];
  let size = 0, over = false;
  req.on("data", (c) => { size += c.length; if (size > 2000000) over = true; if (!over) chunks.push(c); });
  req.on("end", () => {
    if (over) { res.writeHead(413, { "Content-Type": "application/json; charset=utf-8" }); res.end(JSON.stringify({ ok: false, error: "Requete trop grande." })); return; }
    const id = crypto.randomBytes(8).toString("hex");
    const ip = String(req.headers["x-forwarded-for"] || "").split(",")[0].trim() || (req.socket && req.socket.remoteAddress) || "";
    const timer = setTimeout(() => {
      if (apiPending.has(id)) { apiPending.delete(id); if (!res.headersSent) { res.writeHead(504, { "Content-Type": "application/json; charset=utf-8" }); res.end(JSON.stringify({ ok: false, error: "Le bot a mis trop de temps a repondre." })); } }
    }, 25000);
    apiPending.set(id, { res, timer });
    try { parahostBot.send(JSON.stringify({ type: "req", id, method: req.method, url: req.url, headers: req.headers, body: Buffer.concat(chunks).toString("utf8"), ip })); }
    catch (e) { clearTimeout(timer); apiPending.delete(id); if (!res.headersSent) { res.writeHead(502, { "Content-Type": "application/json; charset=utf-8" }); res.end(JSON.stringify({ ok: false, error: "Relais: envoi au bot impossible." })); } }
  });
  req.on("error", () => {});
}

// ============================================================
//  MISE A JOUR : derniere version publiee de l'app LocalDesk.
//  >>> Pour publier une mise a jour : monte "version" ici,
//  mets l'URL de telechargement, et redeploie le relais.
//  Toutes les apps plus anciennes afficheront la banniere.
// ============================================================
const LATEST_APP = {
  version: "1.0.0",
  url: "https://github.com/sashaqoq126-wq/localdesk-relay/releases",
  notes: "Dernière version de LocalDesk.",
};

// Petit serveur HTTP (sante + endpoint de version)
const server = http.createServer((req, res) => {
  if (req.url === "/version") {
    res.writeHead(200, { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" });
    res.end(JSON.stringify(LATEST_APP));
    return;
  }
  if (req.url && req.url.indexOf("/api") === 0) return handleApiRequest(req, res);
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("LocalDesk relay OK\n");
});

const wss = new WebSocketServer({ server });

const hosts = new Map(); // id -> socket de l'hote

// ------------------------------------------------------------
//  Keepalive : empeche l'hebergeur de fermer les connexions
//  inactives, ce qui ferait "disparaitre" un hote du relais.
// ------------------------------------------------------------
const heartbeat = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) {
      // Pas de reponse au ping precedent -> connexion morte, on nettoie
      if (ws.role === "host" && ws.id && hosts.get(ws.id) === ws) hosts.delete(ws.id);
      if (ws.partner && ws.partner.readyState === 1) {
        ws.partner.send(JSON.stringify({ type: "session-end" }));
        ws.partner.partner = null;
      }
      return ws.terminate();
    }
    ws.isAlive = false;
    try { ws.ping(); } catch (_) {}
  });
}, 25000);
wss.on("close", () => clearInterval(heartbeat));

wss.on("connection", (ws) => {
  ws.role = null;
  ws.partner = null;
  ws.id = null;
  ws.isAlive = true;
  ws.on("pong", () => { ws.isAlive = true; });

  ws.on("message", (data, isBinary) => {
    ws.isAlive = true; // toute activite compte comme "vivant"
    // Message de keepalive applicatif : on l'absorbe (sauf si relie)
    if (!ws.partner && !isBinary) {
      const s = data.toString();
      if (s === "ping" || s.indexOf('"ping"') !== -1) {
        try { ws.send('{"type":"pong"}'); } catch (_) {}
        return;
      }
    }
    // Reponses d'API renvoyees par le bot ParaHost
    if (ws.isParahostBot && !isBinary) {
      let m;
      try { m = JSON.parse(data.toString()); } catch (_) { return; }
      if (m.type === "res") {
        const p = apiPending.get(m.id);
        if (p) {
          apiPending.delete(m.id);
          clearTimeout(p.timer);
          if (!p.res.headersSent) {
            p.res.writeHead(m.status || 200, { "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store" });
            p.res.end(m.body || "");
          }
        }
      }
      return;
    }

    // Une fois relies, on transfere tout tel quel au partenaire
    if (ws.partner && ws.partner.readyState === 1) {
      ws.partner.send(data, { binary: isBinary });
      return;
    }

    // Phase de mise en relation : on lit uniquement le message "hello"
    if (isBinary) return;
    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch (_) {
      return;
    }

    if (msg.type === "hello") {
      if (msg.role === "parahost-bot") {
        // Le bot ParaHost s'enregistre (secret partage obligatoire)
        if (RELAY_SECRET && msg.secret === RELAY_SECRET) {
          parahostBot = ws;
          ws.isParahostBot = true;
          try { ws.send(JSON.stringify({ type: "ready" })); } catch (_) {}
          console.log("ParaHost bot connecte au relais API");
        } else {
          try { ws.send(JSON.stringify({ type: "denied" })); } catch (_) {}
        }
        return;
      }
      if (msg.role === "host") {
        // Un PC se rend joignable via son ID
        ws.role = "host";
        ws.id = String(msg.id);
        hosts.set(ws.id, ws);
        ws.send(JSON.stringify({ type: "registered", id: ws.id }));
        console.log("Hote enregistre:", ws.id, "(total:", hosts.size, ")");
      } else if (msg.role === "controller") {
        // Un PC veut en controler un autre
        const target = hosts.get(String(msg.target));
        if (!target || target.readyState !== 1) {
          ws.send(JSON.stringify({ type: "no-host" }));
          return;
        }
        // On relie les deux sockets
        ws.role = "controller";
        ws.partner = target;
        target.partner = ws;
        ws.send(JSON.stringify({ type: "linked" }));
        console.log("Session reliee vers:", msg.target);
      }
    }
  });

  ws.on("close", () => {
    if (ws === parahostBot) parahostBot = null;
    if (ws.role === "host" && ws.id && hosts.get(ws.id) === ws) {
      hosts.delete(ws.id);
    }
    // Prevenir le partenaire que la session est finie
    if (ws.partner && ws.partner.readyState === 1) {
      ws.partner.send(JSON.stringify({ type: "session-end" }));
      ws.partner.partner = null;
    }
  });

  ws.on("error", () => {});
});

server.listen(PORT, () => {
  console.log("LocalDesk relay en ecoute sur le port", PORT);
});
