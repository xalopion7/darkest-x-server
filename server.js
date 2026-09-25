'use strict';
/* ===================================================================
   DARKEST X IN THE CITY — multiplayer server
   Zero npm dependencies: hand-rolled WebSocket (RFC 6455) on Node's
   built-in http module. Run with:  node server.js
   Env: PORT (default 8080)
   =================================================================== */
const http = require('http');
const crypto = require('crypto');
const fs = require('fs');

const PORT = process.env.PORT || 8080;
const WS_MAGIC = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

/* ---------------- accounts (must match the client's ACCOUNTS list) --------------- */
const ACCOUNTS = [
  { id: 1, pass: '8282' }, { id: 2, pass: '9045' }, { id: 3, pass: '9767' },
  { id: 4, pass: '8539' }, { id: 5, pass: '9131' }, { id: 6, pass: '6201' },
  { id: 7, pass: '1447' }, { id: 8, pass: '1169' }, { id: 9, pass: '7279' },
  { id: 10, pass: '4911' }
];

/* ---------------- minimal WebSocket server ---------------- */
function acceptKey(key) {
  return crypto.createHash('sha1').update(key + WS_MAGIC).digest('base64');
}
function encodeFrame(payload, opcode) {
  opcode = opcode || 1; // text
  const data = Buffer.from(payload, 'utf8');
  const len = data.length;
  let header;
  if (len < 126) {
    header = Buffer.alloc(2);
    header[0] = 0x80 | opcode;
    header[1] = len;
  } else if (len < 65536) {
    header = Buffer.alloc(4);
    header[0] = 0x80 | opcode;
    header[1] = 126;
    header.writeUInt16BE(len, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = 0x80 | opcode;
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(len), 2);
  }
  return Buffer.concat([header, data]);
}
// Incremental frame parser bound to one socket; supports fragmented TCP reads
// and (for simplicity) assumes each WS frame's payload arrives reassembled
// per call to feed() using a rolling buffer.
function makeParser(onMessage, onClose) {
  let buf = Buffer.alloc(0);
  function feed(chunk) {
    buf = Buffer.concat([buf, chunk]);
    while (true) {
      if (buf.length < 2) return;
      const b0 = buf[0], b1 = buf[1];
      const fin = (b0 & 0x80) !== 0;
      const opcode = b0 & 0x0f;
      const masked = (b1 & 0x80) !== 0;
      let len = b1 & 0x7f;
      let off = 2;
      if (len === 126) {
        if (buf.length < 4) return;
        len = buf.readUInt16BE(2); off = 4;
      } else if (len === 127) {
        if (buf.length < 10) return;
        len = Number(buf.readBigUInt64BE(2)); off = 10;
      }
      let maskKey = null;
      if (masked) {
        if (buf.length < off + 4) return;
        maskKey = buf.slice(off, off + 4); off += 4;
      }
      if (buf.length < off + len) return; // wait for more data
      let payload = buf.slice(off, off + len);
      if (masked) {
        const out = Buffer.alloc(len);
        for (let i = 0; i < len; i++) out[i] = payload[i] ^ maskKey[i % 4];
        payload = out;
      }
      buf = buf.slice(off + len);
      if (opcode === 0x8) { onClose(); return; }        // close
      if (opcode === 0x9) { continue; }                  // ping (ignored)
      if (opcode === 0x1 || opcode === 0x2) {             // text/binary
        if (fin) onMessage(payload.toString('utf8'));
      }
    }
  }
  return { feed };
}

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('DARKEST X IN THE CITY — multiplayer server is running.\n');
});

server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key) { socket.destroy(); return; }
  const accept = acceptKey(key);
  socket.write(
    'HTTP/1.1 101 Switching Protocols\r\n' +
    'Upgrade: websocket\r\n' +
    'Connection: Upgrade\r\n' +
    'Sec-WebSocket-Accept: ' + accept + '\r\n\r\n'
  );
  const conn = { socket, id: null, acctId: null, name: null, skin: null, x: 0, z: 0, face: 0, party: null, alive: true, driving: false };
  const send = (obj) => { try { socket.write(encodeFrame(JSON.stringify(obj))); } catch (e) {} };
  conn.send = send;
  clients.add(conn);

  const parser = makeParser(
    (text) => { try { handleMessage(conn, JSON.parse(text)); } catch (e) {} },
    () => cleanup(conn)
  );
  socket.on('data', (chunk) => parser.feed(chunk));
  socket.on('close', () => cleanup(conn));
  socket.on('error', () => cleanup(conn));
});

/* ---------------- application state ---------------- */
const clients = new Set();               // all connected sockets (wrapper objects)
const worldByAcct = new Map();            // acctId -> conn (one active session per account)
const parties = new Map();                // code -> party object
const lastSeen = new Map();               // acctId -> ms epoch of last disconnect (absent/undefined = never seen or currently online)
const seats = new Map();                  // seatIdx -> acctId (school classroom seats)
const chatHistory = [];                   // rolling buffer of recent city-chat messages (persists across reconnects, lost on server restart)
let chatSeq = 0;
const scores = new Map();                 // game -> Map(acctId -> bestScore)
const pinned = new Set();                 // ids of pinned chat messages

/* ---------------- disk persistence (survives normal restarts/redeploys as long as the disk itself persists) ---------------- */
const path = require('path');
const DATA_FILE = path.join(__dirname, 'data.json');
function loadData() {
  try {
    const raw = fs.readFileSync(DATA_FILE, 'utf8');
    const d = JSON.parse(raw);
    if (Array.isArray(d.chatHistory)) chatHistory.push(...d.chatHistory);
    chatSeq = d.chatSeq || 0;
    if (Array.isArray(d.pinned)) d.pinned.forEach(id => pinned.add(id));
    if (d.scores && typeof d.scores === 'object') {
      Object.keys(d.scores).forEach(game => { scores.set(game, new Map(Object.entries(d.scores[game]).map(([k, v]) => [+k, v]))); });
    }
    console.log('Loaded', chatHistory.length, 'chat messages,', scores.size, 'score tables from disk.');
  } catch (e) { /* no saved data yet, or unreadable — start fresh */ }
}
let saveTimer = null;
function saveDataSoon() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => {
    try {
      const scoresObj = {};
      scores.forEach((gmap, game) => { scoresObj[game] = Object.fromEntries(gmap); });
      fs.writeFileSync(DATA_FILE, JSON.stringify({ chatHistory, chatSeq, pinned: [...pinned], scores: scoresObj }));
    } catch (e) { console.log('save failed', e.message); }
  }, 800);
}
loadData();

function cleanup(conn) {
  if (!conn.alive) return;
  conn.alive = false;
  clients.delete(conn);
  if (conn.acctId && worldByAcct.get(conn.acctId) === conn) {
    const ts = Date.now();
    worldByAcct.delete(conn.acctId);
    lastSeen.set(conn.acctId, ts);
    broadcastCity({ type: 'left', acctId: conn.acctId });
    broadcastCity({ type: 'presence_one', acctId: conn.acctId, online: false, lastSeen: ts });
    for (const [seatIdx, owner] of seats) { if (owner === conn.acctId) { seats.delete(seatIdx); broadcastCity({ type: 'seat_update', seat: seatIdx, acctId: null }); } }
  }
  if (conn.party) leaveParty(conn, conn.party);
}

function broadcastCity(obj, exceptConn) {
  const msg = JSON.stringify(obj);
  for (const c of worldByAcct.values()) {
    if (c === exceptConn) continue;
    try { c.socket.write(encodeFrame(msg)); } catch (e) {}
  }
}

function handleMessage(conn, msg) {
  if (!msg || typeof msg.type !== 'string') return;

  if (msg.type === 'login') {
    const acc = ACCOUNTS.find(a => a.id === +msg.id);
    if (!acc || acc.pass !== String(msg.pass || '')) { conn.send({ type: 'login_err' }); return; }
    // kick any previous session for this account
    const prev = worldByAcct.get(acc.id);
    if (prev && prev !== conn) { try { prev.socket.end(); } catch (e) {} cleanup(prev); }
    conn.acctId = acc.id; conn.name = String(msg.name || ('Account ' + acc.id)).slice(0, 14);
    conn.skin = String(msg.skin || 'minion').slice(0, 20);
    conn.x = 0; conn.z = 0; conn.face = 0;
    worldByAcct.set(acc.id, conn);
    lastSeen.delete(acc.id);
    conn.send({ type: 'login_ok', id: acc.id });
    // tell the newcomer who else is already in the city
    conn.send({
      type: 'roster',
      players: [...worldByAcct.values()].filter(c => c !== conn).map(c => ({ acctId: c.acctId, name: c.name, skin: c.skin, x: c.x, z: c.z, face: c.face, y: c.y || 0, driving: c.driving, activity: c.activity || null }))
    });
    // presence for ALL 10 accounts (online flag + last-seen for offline ones)
    conn.send({
      type: 'presence',
      list: ACCOUNTS.map(a => ({ acctId: a.id, online: worldByAcct.has(a.id), lastSeen: lastSeen.get(a.id) || null }))
    });
    // seat state
    conn.send({ type: 'seats', seats: [...seats.entries()].map(([seat, acctId]) => ({ seat, acctId })) });
    // recent chat history
    conn.send({ type: 'chat_history', messages: chatHistory });
    conn.send({ type: 'chat_pins', pinned: [...pinned] });
    broadcastCity({ type: 'joined', acctId: conn.acctId, name: conn.name, skin: conn.skin, x: conn.x, z: conn.z, face: conn.face }, conn);
    broadcastCity({ type: 'presence_one', acctId: conn.acctId, online: true, lastSeen: null }, conn);
    return;
  }

  if (!conn.acctId) return; // everything below requires login

  if (msg.type === 'pos') {
    conn.x = +msg.x || 0; conn.z = +msg.z || 0; conn.face = +msg.face || 0; conn.driving = !!msg.driving;
    conn.y = +msg.y || 0; conn.activity = msg.activity ? String(msg.activity).slice(0, 24) : null;
    broadcastCity({ type: 'pos', acctId: conn.acctId, x: conn.x, z: conn.z, face: conn.face, y: conn.y, driving: conn.driving, activity: conn.activity }, conn);
    return;
  }

  if (msg.type === 'emote') {
    const emoji = String(msg.emoji || '').slice(0, 8);
    if (!emoji) return;
    broadcastCity({ type: 'emote', acctId: conn.acctId, emoji });
    return;
  }

  if (msg.type === 'seat_take') {
    const seat = +msg.seat;
    if (!Number.isFinite(seat)) return;
    if (seats.has(seat) && seats.get(seat) !== conn.acctId) { conn.send({ type: 'seat_err', seat, msg: 'ဒီထိုင်ခုံ တစ်ယောက်ယောက် ထိုင်နေပြီးသားပါ' }); return; }
    for (const [s, owner] of seats) { if (owner === conn.acctId && s !== seat) seats.delete(s); }
    seats.set(seat, conn.acctId);
    broadcastCity({ type: 'seat_update', seat, acctId: conn.acctId });
    return;
  }
  if (msg.type === 'seat_leave') {
    for (const [s, owner] of seats) { if (owner === conn.acctId) { seats.delete(s); broadcastCity({ type: 'seat_update', seat: s, acctId: null }); } }
    return;
  }

  if (msg.type === 'chat') {
    const text = String(msg.text || '').slice(0, 500);
    let image = null, audio = null, sticker = null, audioDur = 0;
    if (typeof msg.image === 'string') {
      if (msg.image.length > 350000) { conn.send({ type: 'chat_err', msg: 'ပုံအရမ်းကြီးနေလို့ ပို့လို့မရပါ — ပုံသေးအောင် ပြန်ရွေးပေးပါ' }); return; }
      image = msg.image;
    }
    if (typeof msg.audio === 'string') {
      if (msg.audio.length > 900000) { conn.send({ type: 'chat_err', msg: 'အသံဖိုင် အရမ်းကြီးနေလို့ ပို့လို့မရပါ — ပိုတိုတိုပြန်ဖမ်းပေးပါ' }); return; }
      audio = msg.audio; audioDur = Math.max(0, Math.min(120, +msg.audioDur || 0));
    }
    if (typeof msg.sticker === 'string') sticker = msg.sticker.slice(0, 8);
    if (!text && !image && !audio && !sticker) return;
    const m = { id: ++chatSeq, acctId: conn.acctId, name: conn.name, text, image, audio, audioDur, sticker, ts: Date.now(), edited: false, deleted: false, reactions: {}, seenBy: [conn.acctId] };
    chatHistory.push(m); if (chatHistory.length > 200) chatHistory.shift();
    broadcastCity({ type: 'chat_new', m }); saveDataSoon();
    return;
  }
  if (msg.type === 'typing') {
    broadcastCity({ type: 'typing', acctId: conn.acctId });
    return;
  }
  if (msg.type === 'chat_delete') {
    const m = chatHistory.find(x => x.id === +msg.id);
    if (!m || m.acctId !== conn.acctId || m.deleted) return;
    m.deleted = true; m.text = ''; m.image = null; m.audio = null; m.sticker = null;
    broadcastCity({ type: 'chat_deleted', id: m.id }); saveDataSoon();
    return;
  }
  if (msg.type === 'chat_edit') {
    const m = chatHistory.find(x => x.id === +msg.id);
    if (!m || m.acctId !== conn.acctId || m.deleted || m.image) return;
    const text = String(msg.text || '').slice(0, 500); if (!text) return;
    m.text = text; m.edited = true;
    broadcastCity({ type: 'chat_edited', id: m.id, text }); saveDataSoon();
    return;
  }
  if (msg.type === 'chat_react') {
    const m = chatHistory.find(x => x.id === +msg.id);
    if (!m || m.deleted) return;
    const emoji = String(msg.emoji || '').slice(0, 8); if (!emoji) return;
    if (!m.reactions[emoji]) m.reactions[emoji] = [];
    const arr = m.reactions[emoji];
    const at = arr.indexOf(conn.acctId);
    if (at >= 0) arr.splice(at, 1); else { Object.keys(m.reactions).forEach(k => { const i2 = m.reactions[k].indexOf(conn.acctId); if (i2 >= 0) m.reactions[k].splice(i2, 1); }); arr.push(conn.acctId); }
    if (arr.length === 0) delete m.reactions[emoji];
    broadcastCity({ type: 'chat_reacted', id: m.id, reactions: m.reactions }); saveDataSoon();
    return;
  }
  if (msg.type === 'rename') {
    const name = String(msg.name || '').trim().slice(0, 14); if (!name) return;
    conn.name = name;
    broadcastCity({ type: 'renamed', acctId: conn.acctId, name }, null);
    return;
  }

  if (msg.type === 'score_submit') {
    const game = String(msg.game || '').slice(0, 20); const score = Math.max(0, Math.floor(+msg.score || 0));
    if (!game) return;
    if (!scores.has(game)) scores.set(game, new Map());
    const gmap = scores.get(game);
    const cur = gmap.get(conn.acctId);
    if (!cur || score > cur) { gmap.set(conn.acctId, score); broadcastCity({ type: 'scores', game, list: [...gmap.entries()].map(([acctId, s]) => ({ acctId, score: s })) }); saveDataSoon(); }
    return;
  }
  if (msg.type === 'scores_get') {
    const game = String(msg.game || '').slice(0, 20);
    const gmap = scores.get(game) || new Map();
    conn.send({ type: 'scores', game, list: [...gmap.entries()].map(([acctId, s]) => ({ acctId, score: s })) });
    return;
  }

  if (msg.type === 'chat_pin') {
    const id = +msg.id; const m = chatHistory.find(x => x.id === id);
    if (!m || m.deleted) return;
    if (pinned.has(id)) pinned.delete(id); else { pinned.add(id); if (pinned.size > 5) { const first = pinned.values().next().value; pinned.delete(first); } }
    broadcastCity({ type: 'chat_pins', pinned: [...pinned] }); saveDataSoon();
    return;
  }

  if (msg.type === 'chat_seen') {
    const upTo = +msg.upTo; if (!Number.isFinite(upTo)) return;
    let changed = false;
    for (const m of chatHistory) { if (m.id <= upTo && !m.seenBy.includes(conn.acctId)) { m.seenBy.push(conn.acctId); changed = true; } }
    if (changed) broadcastCity({ type: 'chat_seen_update', acctId: conn.acctId, upTo });
    return;
  }

  // ---------------- Imposter online party ----------------
  if (msg.type === 'party_create') { partyCreate(conn); return; }
  if (msg.type === 'party_join') { partyJoin(conn, String(msg.code || '').toUpperCase()); return; }
  if (msg.type === 'party_leave') { leaveParty(conn, conn.party); return; }
  if (msg.type === 'party_start') { partyStart(conn); return; }
  if (msg.type === 'party_hint') { partyHint(conn, String(msg.text || '')); return; }
  if (msg.type === 'party_vote') { partyVote(conn, msg.target); return; }
  if (msg.type === 'party_guess') { partyGuess(conn, msg.guess); return; }
}

/* ---------------- Imposter party logic (server-authoritative) ---------------- */
const WORDS = [
  { w: 'ဆင်', en: 'Elephant', cat: 'တိရစ္ဆာန်', hints: ['ကိုယ်ထည် အရမ်းကြီးတယ်', 'တောထဲမှာ နေတတ်တယ်', 'နားရွက် ကျယ်ကျယ်ကြီးတွေ ရှိတယ်', 'နှာမောင်းနဲ့ ရေစုပ်ပြီး ပက်တတ်တယ်', 'ဆင်စွယ်လို့ ခေါ်တဲ့ အဖြူရောင်စွယ် ရှိတယ်', 'သစ်လုံးဆွဲတာ တွေ့ဖူးတယ်'] },
  { w: 'ကြောင်', en: 'Cat', cat: 'တိရစ္ဆာန်', hints: ['အိမ်မှာ မွေးတတ်တယ်', 'အရမ်း အိပ်တတ်တယ်', 'ညဘက် မျက်လုံးတွေ လင်းတတ်တယ်', 'ခြေသံမပေးဘဲ လျှောက်တတ်တယ်', 'ကြွက်ကို လိုက်ဖမ်းတတ်တယ်', 'မြူးမြူးလို့ အသံပေးတယ်'] },
  { w: 'ဘောလုံး', en: 'Football', cat: 'အားကစား', hints: ['အုပ်စုလိုက် ကစားရတယ်', 'ခြေနဲ့ ကန်ရတယ်', 'ဂိုးသမား ရှိတယ်', 'ကွင်းထဲမှာ လူများများ ပြေးကြတယ်', 'ဂိုးသွင်းရင် အနိုင်ရတယ်', 'ကွင်းက ပန်းခြံနားမှာ ရှိတယ်'] },
  { w: 'ကျောင်း', en: 'School', cat: 'နေရာ', hints: ['မနက်ပိုင်း လူတွေ စုတဲ့ နေရာ', 'ကလေးတွေ တက်ကြတယ်', 'ဆရာ၊ ဆရာမတွေ ရှိတယ်', 'စာအုပ်တွေ ဆောင်သွားရတယ်', 'စာမေးပွဲ ဖြေရတယ်', 'အတန်းခွဲပြီး သင်တယ်'] },
  { w: 'ကော်ဖီ', en: 'Coffee', cat: 'အစားအသောက်', hints: ['မနက်ခင်း အားတက်စေတယ်', 'အရသာ ခါးတယ်', 'အိပ်ချင်နေသူတွေ သောက်တတ်တယ်', 'ပဲစေ့ကို ကြော်ပြီး ကြိတ်ထားတာ', 'ဆိုင်မှာ ပူပူလေး ရောင်းတယ်', 'ကဖိန်း ပါတယ်'] },
  { w: 'ဖုန်း', en: 'Phone', cat: 'ပစ္စည်း', hints: ['အိတ်ထဲမှာ အမြဲပါတယ်', 'တစ်နေ့တာမှာ များများ ကြည့်ဖြစ်တယ်', 'အားကုန်ရင် အားသွင်းရတယ်', 'မျက်နှာပြင်ကို ထိတယ်', 'အက်ပ်တွေ ထည့်ထားတယ်', 'ခေါ်ဆိုတာ လုပ်လို့ရတယ်'] },
  { w: 'ဆရာဝန်', en: 'Doctor', cat: 'အလုပ်အကိုင်', hints: ['ကျန်းမာရေးနဲ့ ဆိုင်တယ်', 'အဖြူရောင် ကုတ်အင်္ကျီ ဝတ်တတ်တယ်', 'ဆေးရုံမှာ အလုပ်လုပ်တယ်', 'ဆေးစာ ရေးပေးတယ်', 'နှလုံးခုန်သံ နားထောင်တတ်တယ်', 'လူနာကို ကုသတယ်'] }
];
function code4() { const A = 'ABCDEFGHJKLMNPQRSTUVWXYZ'; let s = ''; for (let i = 0; i < 4; i++) s += A[(Math.random() * A.length) | 0]; return s; }
function normText(t) { return String(t || '').trim().toLowerCase().replace(/[.,!?\u104a\u104b\s]+/g, ''); }

function partyCreate(conn) {
  if (conn.party) leaveParty(conn, conn.party);
  let code; do { code = code4(); } while (parties.has(code));
  const party = { code, host: conn.acctId, members: [{ acctId: conn.acctId, name: conn.name, skin: conn.skin }], phase: 'lobby', game: null };
  parties.set(code, party); conn.party = code;
  conn.send({ type: 'party_state', party: publicParty(party) });
}
function partyJoin(conn, code) {
  const party = parties.get(code);
  if (!party) { conn.send({ type: 'party_err', msg: 'ခန်းမ မတွေ့ပါ' }); return; }
  if (party.phase !== 'lobby') { conn.send({ type: 'party_err', msg: 'ပွဲစပြီးသားပါ' }); return; }
  if (party.members.length >= 7) { conn.send({ type: 'party_err', msg: 'ခန်းမပြည့်သွားပြီ' }); return; }
  if (party.members.some(m => m.acctId === conn.acctId)) { conn.party = code; conn.send({ type: 'party_state', party: publicParty(party) }); return; }
  if (conn.party) leaveParty(conn, conn.party);
  party.members.push({ acctId: conn.acctId, name: conn.name, skin: conn.skin });
  conn.party = code;
  partyBroadcast(party, { type: 'party_state', party: publicParty(party) });
}
function leaveParty(conn, code) {
  if (!code) return;
  const party = parties.get(code); conn.party = null;
  if (!party) return;
  party.members = party.members.filter(m => m.acctId !== conn.acctId);
  if (!party.members.length) { parties.delete(code); return; }
  if (party.host === conn.acctId) party.host = party.members[0].acctId;
  partyBroadcast(party, { type: 'party_state', party: publicParty(party) });
}
function publicParty(party) {
  return { code: party.code, host: party.host, members: party.members, phase: party.phase };
}
function partyConnByAcct(acctId) { return worldByAcct.get(acctId); }
function partyBroadcast(party, obj) {
  const msg = JSON.stringify(obj);
  for (const m of party.members) { const c = partyConnByAcct(m.acctId); if (c) try { c.socket.write(encodeFrame(msg)); } catch (e) {} }
}
function partyStart(conn) {
  const party = parties.get(conn.party);
  if (!party || party.host !== conn.acctId || party.phase !== 'lobby') return;
  if (party.members.length < 4) { conn.send({ type: 'party_err', msg: 'အနည်းဆုံး ၄ ယောက် လိုပါသေးတယ်' }); return; }
  const W = WORDS[(Math.random() * WORDS.length) | 0];
  const order = party.members.map(m => m.acctId).sort(() => Math.random() - 0.5);
  const impN = party.members.length >= 6 ? 2 : 1;
  const impSet = new Set(order.slice(0, impN));
  party.phase = 'play';
  party.game = { W, order, imp: impSet, round: 1, turnIdx: 0, spoken: [], usedNorm: new Set(), votes: {}, phaseName: 'hint' };
  for (const m of party.members) {
    const c = partyConnByAcct(m.acctId); if (!c) continue;
    const isImp = impSet.has(m.acctId);
    c.send({ type: 'party_started', role: isImp ? 'imp' : 'crew', word: isImp ? null : party.game.W.w, en: isImp ? null : party.game.W.en, order, members: party.members, round: 1 });
  }
  advanceTurn(party);
}
function advanceTurn(party) {
  const g = party.game;
  if (g.phaseName === 'hint') {
    if (g.turnIdx >= g.order.length) {
      if (g.round >= 2) { g.phaseName = 'vote'; g.turnIdx = 0; g.votes = {}; partyBroadcast(party, { type: 'party_phase', phase: 'vote', order: g.order }); advanceTurn(party); return; }
      g.round++; g.turnIdx = 0;
    }
    const who = g.order[g.turnIdx];
    partyBroadcast(party, { type: 'party_turn', acctId: who, round: g.round, phase: 'hint' });
  } else if (g.phaseName === 'vote') {
    if (g.turnIdx >= g.order.length) { resolveVotes(party); return; }
    const who = g.order[g.turnIdx];
    partyBroadcast(party, { type: 'party_turn', acctId: who, phase: 'vote' });
  }
}
function partyHint(conn, text) {
  const party = parties.get(conn.party); if (!party || !party.game) return;
  const g = party.game;
  if (g.phaseName !== 'hint' || g.order[g.turnIdx] !== conn.acctId) return;
  const t = String(text || '').trim().slice(0, 80);
  if (!t) { conn.send({ type: 'party_err', msg: 'hint ရေးရပါမယ်' }); return; }
  const n = normText(t);
  if (g.usedNorm.has(n)) { conn.send({ type: 'party_err', msg: 'ဒါကို အရင်တစ်ယောက်ယောက် ပြောပြီးသားပါ' }); return; }
  g.usedNorm.add(n); g.spoken.push({ acctId: conn.acctId, text: t });
  partyBroadcast(party, { type: 'party_hint', acctId: conn.acctId, text: t });
  g.turnIdx++; advanceTurn(party);
}
function partyVote(conn, target) {
  const party = parties.get(conn.party); if (!party || !party.game) return;
  const g = party.game;
  if (g.phaseName !== 'vote' || g.order[g.turnIdx] !== conn.acctId) return;
  g.votes[conn.acctId] = target === 'skip' ? 'skip' : +target;
  partyBroadcast(party, { type: 'party_voted', acctId: conn.acctId });
  g.turnIdx++; advanceTurn(party);
}
function resolveVotes(party) {
  const g = party.game;
  const tally = {}; let skips = 0;
  Object.values(g.votes).forEach(v => { if (v === 'skip' || !g.order.includes(v)) skips++; else tally[v] = (tally[v] || 0) + 1; });
  let best = null, bc = 0, tie = false;
  Object.keys(tally).forEach(k => { if (tally[k] > bc) { bc = tally[k]; best = +k; tie = false; } else if (tally[k] === bc) tie = true; });
  if (best === null || tie || skips >= bc) {
    partyBroadcast(party, { type: 'party_result', ejected: null, votes: g.votes, tally });
    endParty(party); return;
  }
  const wasImp = g.imp.has(best);
  partyBroadcast(party, { type: 'party_result', ejected: best, wasImp, votes: g.votes, tally, word: g.W.w, en: g.W.en });
  if (wasImp) {
    const c = partyConnByAcct(best);
    if (c) { g.phaseName = 'guess'; partyBroadcast(party, { type: 'party_guess_wait', acctId: best }); return; }
  }
  finishGame(party, !wasImp, null);
}
function partyGuess(conn, guess) {
  const party = parties.get(conn.party); if (!party || !party.game || party.game.phaseName !== 'guess') return;
  const g = party.game;
  const correct = normText(guess) === normText(g.W.w);
  finishGame(party, !correct, correct);
}
function finishGame(party, crewWin, impGuessCorrect) {
  const g = party.game;
  partyBroadcast(party, { type: 'party_over', crewWin, impGuessCorrect, word: g.W.w, en: g.W.en, imps: [...g.imp] });
  party.phase = 'lobby'; party.game = null;
  partyBroadcast(party, { type: 'party_state', party: publicParty(party) });
}

server.listen(PORT, () => console.log('DARKEST X server listening on :' + PORT));
