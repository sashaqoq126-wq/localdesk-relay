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
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;

// Petit serveur HTTP (pour que Render voie le service "vivant")
const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("LocalDesk relay OK\n");
});

const wss = new WebSocketServer({ server });

const hosts = new Map(); // id -> socket de l'hote

wss.on("connection", (ws) => {
  ws.role = null;
  ws.partner = null;
  ws.id = null;

  ws.on("message", (data, isBinary) => {
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
