const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const fs = require("fs");
const path = require("path");
const { Pool } = require("pg");

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });
const PORT = Number(process.env.PORT || 3000);
const DATABASE_URL = process.env.DATABASE_URL;
const LEGACY_DB = path.join(__dirname, "data.json");

app.use(express.json({ limit: "12mb" }));
app.use(express.static(path.join(__dirname, "public")));

if (!DATABASE_URL) {
  console.error("DATABASE_URL is required. Add it in the Render service Environment settings.");
  process.exit(1);
}

const pool = new Pool({
  connectionString: DATABASE_URL,
  max: 5,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
  ssl: process.env.NODE_ENV === "production" ? { rejectUnauthorized: false } : false
});
pool.on("error", err => console.error("PostgreSQL pool error:", err.message));

const clean = v => String(v ?? "").trim();
const send = (ws, x) => { if (ws?.readyState === WebSocket.OPEN) ws.send(JSON.stringify(x)); };
const broadcast = x => wss.clients.forEach(c => send(c, x));
const online = username => { let ok = false; wss.clients.forEach(c => { if (c.username === username) ok = true; }); return ok; };
const validUsername = u => /^[A-Za-z0-9_.-]{3,32}$/.test(u);

async function getUsers() {
  const r = await pool.query("SELECT username,name,bio,avatar FROM users ORDER BY username");
  return r.rows.map(u => ({ username:u.username, name:u.name || u.username, bio:u.bio || "", avatar:u.avatar || "", online:online(u.username) }));
}
async function userExists(username) {
  const r = await pool.query("SELECT 1 FROM users WHERE username=$1 LIMIT 1", [username]);
  return r.rowCount > 0;
}
async function publishUsers() { try { broadcast({ type:"users", users: await getUsers() }); } catch (e) { console.error("Publish users:", e.message); } }
function publicMessage(row) {
  return {
    id:row.id, from:row.sender, to:row.receiver, text:row.text || "", image:row.image || "", audio:row.audio || "",
    replyTo:row.reply_to || null, edited:!!row.edited, editedAt:row.edited_at || null,
    reactions:row.reactions || {}, time:row.message_time, read:!!row.read
  };
}

async function initDatabase() {
  await pool.query(`CREATE TABLE IF NOT EXISTS users (
    username TEXT PRIMARY KEY, password TEXT NOT NULL, name TEXT NOT NULL DEFAULT '',
    bio TEXT NOT NULL DEFAULT '', avatar TEXT NOT NULL DEFAULT '', created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
  )`);
  await pool.query(`CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY, sender TEXT NOT NULL, receiver TEXT NOT NULL, text TEXT NOT NULL DEFAULT '',
    image TEXT NOT NULL DEFAULT '', audio TEXT NOT NULL DEFAULT '', reactions JSONB NOT NULL DEFAULT '{}'::jsonb,
    message_time TIMESTAMPTZ NOT NULL DEFAULT NOW(), read BOOLEAN NOT NULL DEFAULT FALSE,
    reply_to TEXT, edited BOOLEAN NOT NULL DEFAULT FALSE, edited_at TIMESTAMPTZ
  )`);
  // Safely add new fields if the database was created by an earlier version.
  await pool.query("ALTER TABLE messages ADD COLUMN IF NOT EXISTS reply_to TEXT");
  await pool.query("ALTER TABLE messages ADD COLUMN IF NOT EXISTS edited BOOLEAN NOT NULL DEFAULT FALSE");
  await pool.query("ALTER TABLE messages ADD COLUMN IF NOT EXISTS edited_at TIMESTAMPTZ");
  await pool.query("CREATE INDEX IF NOT EXISTS messages_sender_receiver_idx ON messages(sender,receiver)");
  await pool.query("CREATE INDEX IF NOT EXISTS messages_receiver_sender_idx ON messages(receiver,sender)");

  // One-time best-effort migration from data.json, only when both DB tables are empty.
  const counts = await pool.query(`SELECT (SELECT COUNT(*) FROM users)::int AS users, (SELECT COUNT(*) FROM messages)::int AS messages`);
  if (counts.rows[0].users === 0 && counts.rows[0].messages === 0 && fs.existsSync(LEGACY_DB)) {
    try {
      const old = JSON.parse(fs.readFileSync(LEGACY_DB, "utf8"));
      const client = await pool.connect();
      try {
        await client.query("BEGIN");
        for (const u of (Array.isArray(old.users) ? old.users : [])) {
          if (!u?.username || typeof u.password !== "string") continue;
          await client.query(`INSERT INTO users(username,password,name,bio,avatar) VALUES($1,$2,$3,$4,$5) ON CONFLICT(username) DO NOTHING`,
            [u.username,u.password,u.name || u.username,u.bio || "",u.avatar || ""]);
        }
        for (const m of (Array.isArray(old.messages) ? old.messages : [])) {
          if (!m?.id || !m.from || !m.to) continue;
          await client.query(`INSERT INTO messages(id,sender,receiver,text,image,audio,reactions,message_time,read,reply_to,edited,edited_at)
            VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9,$10,$11,$12) ON CONFLICT(id) DO NOTHING`,
            [String(m.id),m.from,m.to,m.text || "",m.image || "",m.audio || "",JSON.stringify(m.reactions || {}),m.time || new Date().toISOString(),!!m.read,m.replyTo || null,!!m.edited,m.editedAt || null]);
        }
        await client.query("COMMIT");
        console.log("Legacy data.json migration completed (if it contained records).");
      } catch (e) { await client.query("ROLLBACK"); throw e; }
      finally { client.release(); }
    } catch (e) { console.error("Legacy migration skipped/failed:", e.message); }
  }
  console.log("PostgreSQL connected; reply/edit/delete enabled.");
}

app.get("/api/health", async (req,res) => {
  try {
    await pool.query("SELECT 1");
    const r = await pool.query(`SELECT (SELECT COUNT(*) FROM users)::int AS users, (SELECT COUNT(*) FROM messages)::int AS messages`);
    res.json({ ok:true, version:"4.2.0", database:true, users:r.rows[0].users, messages:r.rows[0].messages });
  } catch (e) { console.error("Health error:", e.message); res.status(500).json({ ok:false, database:false }); }
});

app.get("/api/users", async (req,res) => { try { res.json(await getUsers()); } catch(e) { console.error(e); res.status(500).json({error:"خطا در دریافت کاربران."}); } });

app.post("/api/register", async (req,res) => {
  try {
    const username=clean(req.body.username), password=String(req.body.password || "");
    if (!validUsername(username)) return res.json({error:"نام کاربری ۳ تا ۳۲ کاراکتر و فقط حروف انگلیسی، عدد، نقطه، - یا _ باشد."});
    if (password.length < 4) return res.json({error:"رمز حداقل ۴ کاراکتر باشد."});
    const exists=await pool.query("SELECT 1 FROM users WHERE LOWER(username)=LOWER($1) LIMIT 1",[username]);
    if (exists.rowCount) return res.json({error:"این نام کاربری وجود دارد."});
    await pool.query("INSERT INTO users(username,password,name,bio,avatar) VALUES($1,$2,$1,'','')",[username,password]);
    await publishUsers(); res.json({success:true,username});
  } catch(e) { console.error("Register:",e.message); res.status(500).json({error:"خطا در ساخت حساب."}); }
});

app.post("/api/login", async (req,res) => {
  try {
    const username=clean(req.body.username), password=String(req.body.password || "");
    const r=await pool.query("SELECT username FROM users WHERE LOWER(username)=LOWER($1) AND password=$2 LIMIT 1",[username,password]);
    if (!r.rowCount) return res.json({error:"نام کاربری یا رمز اشتباه است."});
    res.json({success:true,username:r.rows[0].username});
  } catch(e) { console.error("Login:",e.message); res.status(500).json({error:"خطا در ورود."}); }
});

app.get("/api/messages", async (req,res) => {
  try {
    const a=clean(req.query.a), b=clean(req.query.b);
    const r=await pool.query(`SELECT * FROM (
      SELECT id,sender,receiver,text,image,audio,reactions,message_time,read,reply_to,edited,edited_at
      FROM messages WHERE (sender=$1 AND receiver=$2) OR (sender=$2 AND receiver=$1)
      ORDER BY message_time DESC LIMIT 500
    ) recent ORDER BY message_time ASC`,[a,b]);
    res.json(r.rows.map(publicMessage));
  } catch(e) { console.error("Get messages:",e.message); res.status(500).json({error:"خطا در دریافت پیام‌ها."}); }
});

app.post("/api/messages", async (req,res) => {
  try {
    const from=clean(req.body.from), to=clean(req.body.to), text=clean(req.body.text);
    const image=String(req.body.image || ""), audio=String(req.body.audio || ""), replyTo=clean(req.body.replyTo || "") || null;
    if (!from || !to) return res.json({error:"مخاطب انتخاب نشده."});
    if (!(await userExists(from))) return res.json({error:"فرستنده پیدا نشد. دوباره وارد شو."});
    if (!(await userExists(to))) return res.json({error:"مخاطب پیدا نشد. از فهرست کاربران انتخابش کن."});
    if (!text && !image && !audio) return res.json({error:"پیام خالی است."});
    if (image.length > 3000000) return res.json({error:"عکس خیلی بزرگ است."});
    if (audio.length > 8000000) return res.json({error:"ویس خیلی بزرگ است."});
    if (replyTo) {
      const q=await pool.query("SELECT id FROM messages WHERE id=$1 AND ((sender=$2 AND receiver=$3) OR (sender=$3 AND receiver=$2))",[replyTo,from,to]);
      if (!q.rowCount) return res.json({error:"پیام اصلی برای پاسخ پیدا نشد."});
    }
    const id=Date.now().toString(36)+Math.random().toString(36).slice(2);
    const r=await pool.query(`INSERT INTO messages(id,sender,receiver,text,image,audio,reactions,message_time,read,reply_to)
      VALUES($1,$2,$3,$4,$5,$6,'{}'::jsonb,NOW(),FALSE,$7) RETURNING *`,[id,from,to,text,image,audio,replyTo]);
    const message=publicMessage(r.rows[0]);
    wss.clients.forEach(c=>{if(c.username===from || c.username===to)send(c,{type:"message",message});});
    res.json({success:true,message});
  } catch(e) { console.error("Send message:",e.message); res.status(500).json({error:"خطا در ارسال پیام."}); }
});

app.post("/api/messages/edit", async (req,res) => {
  try {
    const id=clean(req.body.id), username=clean(req.body.username), text=clean(req.body.text);
    if (!text) return res.json({error:"متن پیام نمی‌تواند خالی باشد."});
    const r=await pool.query("SELECT * FROM messages WHERE id=$1",[id]);
    if (!r.rowCount) return res.status(404).json({error:"پیام پیدا نشد."});
    if (r.rows[0].sender !== username) return res.status(403).json({error:"فقط فرستنده می‌تواند پیام خودش را ویرایش کند."});
    if (r.rows[0].image || r.rows[0].audio) return res.json({error:"فعلاً فقط پیام متنی قابل ویرایش است."});
    const updated=await pool.query("UPDATE messages SET text=$1,edited=TRUE,edited_at=NOW() WHERE id=$2 RETURNING *",[text,id]);
    const message=publicMessage(updated.rows[0]);
    wss.clients.forEach(c=>{if(c.username===message.from || c.username===message.to)send(c,{type:"message-updated",message});});
    res.json({success:true,message});
  } catch(e) { console.error("Edit message:",e.message); res.status(500).json({error:"ویرایش پیام انجام نشد."}); }
});

app.post("/api/messages/delete", async (req,res) => {
  try {
    const id=clean(req.body.id), username=clean(req.body.username);
    const r=await pool.query("SELECT * FROM messages WHERE id=$1",[id]);
    if (!r.rowCount) return res.status(404).json({error:"پیام پیدا نشد."});
    const m=publicMessage(r.rows[0]);
    if (m.from !== username) return res.status(403).json({error:"فقط فرستنده می‌تواند پیام خودش را حذف کند."});
    await pool.query("DELETE FROM messages WHERE id=$1",[id]);
    wss.clients.forEach(c=>{if(c.username===m.from || c.username===m.to)send(c,{type:"message-deleted",id});});
    res.json({success:true,id});
  } catch(e) { console.error("Delete message:",e.message); res.status(500).json({error:"حذف پیام انجام نشد."}); }
});

app.post("/api/read", async (req,res) => {
  try {
    const from=clean(req.body.from), to=clean(req.body.to);
    await pool.query("UPDATE messages SET read=TRUE WHERE sender=$1 AND receiver=$2",[from,to]);
    broadcast({type:"read",from,to}); res.json({success:true});
  } catch(e) { console.error("Read:",e.message); res.status(500).json({error:"خطا در خواندن پیام."}); }
});

app.post("/api/react", async (req,res) => {
  try {
    const id=clean(req.body.id), emoji=String(req.body.emoji || ""), username=clean(req.body.username);
    const r=await pool.query("SELECT reactions FROM messages WHERE id=$1",[id]);
    if (!r.rowCount) return res.json({error:"پیام پیدا نشد."});
    const reactions=r.rows[0].reactions || {};
    for (const e of Object.keys(reactions)) { reactions[e]=(reactions[e] || []).filter(x=>x!==username); if (!reactions[e].length) delete reactions[e]; }
    if (emoji !== "none") { reactions[emoji]=reactions[emoji] || []; if (!reactions[emoji].includes(username)) reactions[emoji].push(username); }
    await pool.query("UPDATE messages SET reactions=$1::jsonb WHERE id=$2",[JSON.stringify(reactions),id]);
    broadcast({type:"reaction",id,reactions}); res.json({success:true});
  } catch(e) { console.error("Reaction:",e.message); res.status(500).json({error:"خطا در واکنش."}); }
});

app.get("/api/profile", async (req,res) => {
  try {
    const r=await pool.query("SELECT username,name,bio,avatar FROM users WHERE username=$1",[clean(req.query.username)]);
    if (!r.rowCount) return res.status(404).json({error:"کاربر پیدا نشد."});
    const u=r.rows[0]; res.json({username:u.username,name:u.name || u.username,bio:u.bio || "",avatar:u.avatar || ""});
  } catch(e) { console.error("Profile:",e.message); res.status(500).json({error:"خطا در دریافت پروفایل."}); }
});
app.post("/api/profile", async (req,res) => {
  try {
    const username=clean(req.body.username), name=clean(req.body.name) || username, bio=String(req.body.bio || "").slice(0,200), avatar=String(req.body.avatar || "");
    if (avatar.length > 1800000) return res.json({error:"عکس خیلی بزرگ است."});
    const r=await pool.query("UPDATE users SET name=$1,bio=$2,avatar=$3 WHERE username=$4 RETURNING username",[name,bio,avatar,username]);
    if (!r.rowCount) return res.json({error:"کاربر پیدا نشد."});
    await publishUsers(); res.json({success:true});
  } catch(e) { console.error("Save profile:",e.message); res.status(500).json({error:"خطا در ذخیره پروفایل."}); }
});

wss.on("connection", ws => {
  ws.on("message", async raw => {
    try {
      const d=JSON.parse(raw);
      if (d.type === "auth") {
        const username=clean(d.username);
        if (await userExists(username)) { ws.username=username; send(ws,{type:"users",users:await getUsers()}); await publishUsers(); }
      }
    } catch(e) { console.error("WebSocket:",e.message); }
  });
  ws.on("close", () => { publishUsers(); });
});

async function start() {
  try { await initDatabase(); server.listen(PORT,"0.0.0.0",()=>console.log(`=== Karen Messenger V4.2 PostgreSQL ===\nListening on 0.0.0.0:${PORT}`)); }
  catch(e) { console.error("Database startup failed:",e.message); process.exit(1); }
}
start();
