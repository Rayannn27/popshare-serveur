// PopShare - serveur relais
// Chaque client se connecte en WebSocket, rejoint un "salon" grâce à un code,
// et le serveur relaie les médias envoyés aux autres membres du salon.

const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const MAX_MO = Number(process.env.MAX_MO || 25);          // taille max d'un média (Mo)
const MAX_ENVOIS_PAR_MINUTE = Number(process.env.MAX_ENVOIS || 20);
const MAX_PAYLOAD = Math.ceil(MAX_MO * 1024 * 1024 * 1.4); // base64 ≈ +33 %

// salons : code -> Map(idClient -> ws)
const salons = new Map();
let prochainId = 1;

const serveur = http.createServer((req, res) => {
  // Petite page de santé (utile pour Render & co)
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  let total = 0;
  for (const s of salons.values()) total += s.size;
  res.end(`PopShare serveur OK - ${salons.size} salon(s), ${total} connecté(s)\n`);
});

const wss = new WebSocketServer({ server: serveur, maxPayload: MAX_PAYLOAD });

function envoyer(ws, obj) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj));
}

function listeMembres(code) {
  const salon = salons.get(code);
  if (!salon) return [];
  return [...salon.values()].map((c) => ({ id: c.id, nom: c.nom, nePasDeranger: c.nePasDeranger }));
}

function diffuserMembres(code) {
  const salon = salons.get(code);
  if (!salon) return;
  const membres = listeMembres(code);
  for (const c of salon.values()) envoyer(c, { type: 'membres', membres, moi: c.id });
}

function nettoyerNom(nom) {
  return String(nom || '').replace(/[\u0000-\u001f]/g, '').trim().slice(0, 24) || 'Anonyme';
}

wss.on('connection', (ws) => {
  ws.id = String(prochainId++);
  ws.nom = null;
  ws.salon = null;
  ws.nePasDeranger = false;
  ws.envois = [];
  ws.vivant = true;
  ws.on('pong', () => { ws.vivant = true; });

  ws.on('message', (brut) => {
    let msg;
    try { msg = JSON.parse(brut); } catch { return; }
    if (!msg || typeof msg.type !== 'string') return;

    // 1) Rejoindre un salon
    if (msg.type === 'bonjour') {
      const code = String(msg.salon || '').trim().toLowerCase().slice(0, 64);
      if (code.length < 4) return envoyer(ws, { type: 'erreur', message: 'Code de salon trop court (4 caractères minimum).' });
      if (ws.salon) quitter(ws);
      ws.nom = nettoyerNom(msg.nom);
      ws.salon = code;
      if (!salons.has(code)) salons.set(code, new Map());
      salons.get(code).set(ws.id, ws);
      envoyer(ws, { type: 'bienvenue', id: ws.id, maxMo: MAX_MO });
      diffuserMembres(code);
      return;
    }

    if (msg.type === 'coucou') return; // simple signal de vie
    if (!ws.salon) return envoyer(ws, { type: 'erreur', message: 'Rejoins un salon d\'abord.' });
    const salon = salons.get(ws.salon);

    // 2) Statut "ne pas déranger"
    if (msg.type === 'statut') {
      ws.nePasDeranger = !!msg.nePasDeranger;
      diffuserMembres(ws.salon);
      return;
    }

    // 3) Envoi d'un média
    if (msg.type === 'media') {
      const maintenant = Date.now();
      ws.envois = ws.envois.filter((t) => maintenant - t < 60_000);
      if (ws.envois.length >= MAX_ENVOIS_PAR_MINUTE) {
        return envoyer(ws, { type: 'erreur', message: 'Doucement ! Trop d\'envois en une minute.' });
      }
      ws.envois.push(maintenant);

      const media = {
        type: 'media',
        de: ws.nom,
        deId: ws.id,
        genre: msg.genre === 'video' ? 'video' : msg.genre === 'texte' ? 'texte' : 'image',
        mime: String(msg.mime || '').slice(0, 64),
        donnees: typeof msg.donnees === 'string' ? msg.donnees : null, // base64
        url: typeof msg.url === 'string' ? msg.url.slice(0, 2048) : null,
        legende: String(msg.legende || '').slice(0, 140),
        envoiId: String(msg.envoiId || '').slice(0, 64),
      };
      if (!media.donnees && !media.url && !media.legende) return;

      const cibles = Array.isArray(msg.a) ? msg.a.map(String) : null; // null = tout le monde
      let livres = 0, ignores = 0;
      for (const c of salon.values()) {
        if (c.id === ws.id) continue;
        if (cibles && !cibles.includes(c.id)) continue;
        if (c.nePasDeranger) { ignores++; continue; }
        envoyer(c, media);
        livres++;
      }
      envoyer(ws, { type: 'accuse', envoiId: media.envoiId, livres, ignores });
      return;
    }
  });

  ws.on('close', () => quitter(ws));
  ws.on('error', () => {});
});

function quitter(ws) {
  const code = ws.salon;
  if (!code) return;
  const salon = salons.get(code);
  if (salon) {
    salon.delete(ws.id);
    if (salon.size === 0) salons.delete(code);
    else diffuserMembres(code);
  }
  ws.salon = null;
}

// Détection des connexions mortes
const battement = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.vivant) { ws.terminate(); continue; }
    ws.vivant = false;
    ws.ping();
  }
}, 30_000);
wss.on('close', () => clearInterval(battement));

serveur.listen(PORT, () => {
  console.log(`PopShare serveur en écoute sur le port ${PORT} (médias jusqu'à ${MAX_MO} Mo)`);
});
