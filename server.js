const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const path = require("path");
const fs = require("fs");
const { Pool } = require("pg");

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const PORT = Number(process.env.PORT || 3000);
const DATABASE_URL = process.env.DATABASE_URL || "";

app.use(express.json({ limit: "12mb" }));
app.use(express.static(path.join(__dirname, "public")));

// ======================================================
// DATABASE
// ======================================================

let pool = null;

if (DATABASE_URL) {
  pool = new Pool({
    connectionString: DATABASE_URL,
    max: 5,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 10000
  });

  pool.on("error", err => {
    console.error("PostgreSQL pool error:", err.message);
  });
}

async function initDatabase() {
  if (!pool) {
    console.log("WARNING: DATABASE_URL is missing.");
    console.log("Running without PostgreSQL.");
    return;
  }

  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      username TEXT PRIMARY KEY,
      password TEXT NOT NULL,
      name TEXT NOT NULL DEFAULT '',
      bio TEXT NOT NULL DEFAULT '',
      avatar TEXT NOT NULL DEFAULT '',
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS messages (
      id TEXT PRIMARY KEY,
      sender TEXT NOT NULL,
      receiver TEXT NOT NULL,
      text TEXT NOT NULL DEFAULT '',
      image TEXT NOT NULL DEFAULT '',
      audio TEXT NOT NULL DEFAULT '',
      reactions JSONB NOT NULL DEFAULT '{}'::jsonb,
      message_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      read BOOLEAN NOT NULL DEFAULT FALSE
    )
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS messages_sender_receiver_idx
    ON messages(sender, receiver)
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS messages_receiver_sender_idx
    ON messages(receiver, sender)
  `);

  console.log("PostgreSQL connected.");
}

// ======================================================
// HELPERS
// ======================================================

const clean = v => String(v ?? "").trim();

const validUsername = u =>
  /^[A-Za-z0-9_.-]{3,32}$/.test(u);

const send = (ws, data) => {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
};

const broadcast = data => {
  wss.clients.forEach(ws => send(ws, data));
};

const online = username => {
  let found = false;

  wss.clients.forEach(ws => {
    if (ws.username === username) {
      found = true;
    }
  });

  return found;
};

async function getUsers() {
  if (!pool) return [];

  const result = await pool.query(`
    SELECT username, name, bio, avatar
    FROM users
    ORDER BY username
  `);

  return result.rows.map(u => ({
    username: u.username,
    name: u.name || u.username,
    bio: u.bio || "",
    avatar: u.avatar || "",
    online: online(u.username)
  }));
}

async function userExists(username) {
  if (!pool) return false;

  const result = await pool.query(
    `SELECT 1 FROM users WHERE username = $1 LIMIT 1`,
    [username]
  );

  return result.rowCount > 0;
}

// ======================================================
// HEALTH
// ======================================================

app.get("/api/health", async (req, res) => {
  try {
    if (!pool) {
      return res.json({
        ok: true,
        version: "4.2.0",
        database: false
      });
    }

    await pool.query("SELECT 1");

    const users = await pool.query(
      `SELECT COUNT(*)::int AS count FROM users`
    );

    const messages = await pool.query(
      `SELECT COUNT(*)::int AS count FROM messages`
    );

    res.json({
      ok: true,
      version: "4.2.0",
      database: true,
      users: users.rows[0].count,
      messages: messages.rows[0].count
    });
  } catch (err) {
    console.error("Health error:", err);

    res.status(500).json({
      ok: false,
      database: false,
      error: "Database connection failed"
    });
  }
});

// ======================================================
// USERS
// ======================================================

app.get("/api/users", async (req, res) => {
  try {
    res.json(await getUsers());
  } catch (err) {
    console.error("Users error:", err);
    res.status(500).json({ error: "خطا در دریافت کاربران." });
  }
});

// ======================================================
// REGISTER
// ======================================================

app.post("/api/register", async (req, res) => {
  try {
    const username = clean(req.body.username);
    const password = String(req.body.password || "");

    if (!pool) {
      return res.json({
        error: "Database is not connected."
      });
    }

    if (!validUsername(username)) {
      return res.json({
        error:
          "نام کاربری ۳ تا ۳۲ کاراکتر و فقط حروف انگلیسی، عدد، نقطه، - یا _ باشد."
      });
    }

    if (password.length < 4) {
      return res.json({
        error: "رمز حداقل ۴ کاراکتر باشد."
      });
    }

    const exists = await pool.query(
      `SELECT 1 FROM users WHERE LOWER(username)=LOWER($1) LIMIT 1`,
      [username]
    );

    if (exists.rowCount > 0) {
      return res.json({
        error: "این نام کاربری وجود دارد."
      });
    }

    await pool.query(
      `
      INSERT INTO users
      (username, password, name, bio, avatar)
      VALUES ($1, $2, $1, '', '')
      `,
      [username, password]
    );

    broadcast({
      type: "users",
      users: await getUsers()
    });

    res.json({
      success: true,
      username
    });
  } catch (err) {
    console.error("Register error:", err);

    res.status(500).json({
      error: "خطا در ساخت حساب."
    });
  }
});

// ======================================================
// LOGIN
// ======================================================

app.post("/api/login", async (req, res) => {
  try {
    const username = clean(req.body.username);
    const password = String(req.body.password || "");

    if (!pool) {
      return res.json({
        error: "Database is not connected."
      });
    }

    const result = await pool.query(
      `
      SELECT username
      FROM users
      WHERE LOWER(username)=LOWER($1)
      AND password=$2
      LIMIT 1
      `,
      [username, password]
    );

    if (result.rowCount === 0) {
      return res.json({
        error: "نام کاربری یا رمز اشتباه است."
      });
    }

    res.json({
      success: true,
      username: result.rows[0].username
    });
  } catch (err) {
    console.error("Login error:", err);

    res.status(500).json({
      error: "خطا در ورود."
    });
  }
});

// ======================================================
// MESSAGES
// ======================================================

app.get("/api/messages", async (req, res) => {
  try {
    const a = clean(req.query.a);
    const b = clean(req.query.b);

    const result = await pool.query(
      `
      SELECT
        id,
        sender AS "from",
        receiver AS "to",
        text,
        image,
        audio,
        reactions,
        message_time AS time,
        read
      FROM messages
      WHERE
        (sender=$1 AND receiver=$2)
        OR
        (sender=$2 AND receiver=$1)
      ORDER BY message_time ASC
      LIMIT 500
      `,
      [a, b]
    );

    res.json(result.rows);
  } catch (err) {
    console.error("Messages GET error:", err);

    res.status(500).json({
      error: "خطا در دریافت پیام‌ها."
    });
  }
});

app.post("/api/messages", async (req, res) => {
  try {
    const from = clean(req.body.from);
    const to = clean(req.body.to);
    const text = clean(req.body.text);
    const image = String(req.body.image || "");
    const audio = String(req.body.audio || "");

    if (!from || !to) {
      return res.json({
        error: "مخاطب انتخاب نشده."
      });
    }

    if (!(await userExists(from))) {
      return res.json({
        error: "فرستنده پیدا نشد. دوباره وارد شو."
      });
    }

    if (!(await userExists(to))) {
      return res.json({
        error: "مخاطب پیدا نشد. از فهرست کاربران انتخابش کن."
      });
    }

    if (!text && !image && !audio) {
      return res.json({
        error: "پیام خالی است."
      });
    }

    if (image.length > 3000000) {
      return res.json({
        error: "عکس خیلی بزرگ است."
      });
    }

    if (audio.length > 8000000) {
      return res.json({
        error: "ویس خیلی بزرگ است."
      });
    }

    const id =
      Date.now().toString(36) +
      Math.random().toString(36).slice(2);

    const result = await pool.query(
      `
      INSERT INTO messages
      (
        id,
        sender,
        receiver,
        text,
        image,
        audio,
        reactions,
        message_time,
        read
      )
      VALUES
      ($1,$2,$3,$4,$5,$6,'{}'::jsonb,NOW(),FALSE)
      RETURNING
        id,
        sender AS "from",
        receiver AS "to",
        text,
        image,
        audio,
        reactions,
        message_time AS time,
        read
      `,
      [id, from, to, text, image, audio]
    );

    const message = result.rows[0];

    wss.clients.forEach(ws => {
      if (ws.username === from || ws.username === to) {
        send(ws, {
          type: "message",
          message
        });
      }
    });

    res.json({
      success: true,
      message
    });
  } catch (err) {
    console.error("Messages POST error:", err);

    res.status(500).json({
      error: "خطا در ارسال پیام."
    });
  }
});

// ======================================================
// READ
// ======================================================

app.post("/api/read", async (req, res) => {
  try {
    const from = clean(req.body.from);
    const to = clean(req.body.to);

    await pool.query(
      `
      UPDATE messages
      SET read=TRUE
      WHERE sender=$1 AND receiver=$2
      `,
      [from, to]
    );

    broadcast({
      type: "read",
      from,
      to
    });

    res.json({
      success: true
    });
  } catch (err) {
    console.error("Read error:", err);

    res.status(500).json({
      error: "خطا در خواندن پیام."
    });
  }
});

// ======================================================
// REACTIONS
// ======================================================

app.post("/api/react", async (req, res) => {
  try {
    const id = clean(req.body.id);
    const emoji = String(req.body.emoji || "");
    const username = clean(req.body.username);

    const result = await pool.query(
      `SELECT reactions FROM messages WHERE id=$1`,
      [id]
    );

    if (result.rowCount === 0) {
      return res.json({
        error: "پیام پیدا نشد."
      });
    }

    const reactions = result.rows[0].reactions || {};

    for (const e of Object.keys(reactions)) {
      reactions[e] = (reactions[e] || []).filter(
        x => x !== username
      );

      if (reactions[e].length === 0) {
        delete reactions[e];
      }
    }

    if (emoji !== "none") {
      reactions[emoji] = reactions[emoji] || [];

      if (!reactions[emoji].includes(username)) {
        reactions[emoji].push(username);
      }
    }

    await pool.query(
      `
      UPDATE messages
      SET reactions=$1::jsonb
      WHERE id=$2
      `,
      [JSON.stringify(reactions), id]
    );

    broadcast({
      type: "reaction",
      id,
      reactions
    });

    res.json({
      success: true
    });
  } catch (err) {
    console.error("Reaction error:", err);

    res.status(500).json({
      error: "خطا در واکنش."
    });
  }
});

// ======================================================
// PROFILE
// ======================================================

app.get("/api/profile", async (req, res) => {
  try {
    const username = clean(req.query.username);

    const result = await pool.query(
      `
      SELECT username, name, bio, avatar
      FROM users
      WHERE username=$1
      LIMIT 1
      `,
      [username]
    );

    if (result.rowCount === 0) {
      return res.status(404).json({
        error: "کاربر پیدا نشد."
      });
    }

    const u = result.rows[0];

    res.json({
      username: u.username,
      name: u.name || u.username,
      bio: u.bio || "",
      avatar: u.avatar || ""
    });
  } catch (err) {
    console.error("Profile GET error:", err);

    res.status(500).json({
      error: "خطا در دریافت پروفایل."
    });
  }
});

app.post("/api/profile", async (req, res) => {
  try {
    const username = clean(req.body.username);
    const name = clean(req.body.name);
    const bio = String(req.body.bio || "").slice(0, 200);
    const avatar = String(req.body.avatar || "");

    if (avatar.length > 1800000) {
      return res.json({
        error: "عکس خیلی بزرگ است."
      });
    }

    const result = await pool.query(
      `
      UPDATE users
      SET
        name=$1,
        bio=$2,
        avatar=$3
      WHERE username=$4
      RETURNING username
      `,
      [
        name || username,
        bio,
        avatar,
        username
      ]
    );

    if (result.rowCount === 0) {
      return res.json({
        error: "کاربر پیدا نشد."
      });
    }

    broadcast({
      type: "users",
      users: await getUsers()
    });

    res.json({
      success: true
    });
  } catch (err) {
    console.error("Profile POST error:", err);

    res.status(500).json({
      error: "خطا در ذخیره پروفایل."
    });
  }
});

// ======================================================
// WEBSOCKET
// ======================================================

wss.on("connection", ws => {
  ws.on("message", async raw => {
    try {
      const data = JSON.parse(raw);

      if (data.type === "auth") {
        const username = clean(data.username);

        if (await userExists(username)) {
          ws.username = username;

          send(ws, {
            type: "users",
            users: await getUsers()
          });

          broadcast({
            type: "users",
            users: await getUsers()
          });
        }
      }
    } catch (err) {
      console.error("WebSocket error:", err.message);
    }
  });

  ws.on("close", async () => {
    broadcast({
      type: "users",
      users: await getUsers()
    });
  });
});

// ======================================================
// START
// ======================================================

async function start() {
  try {
    await initDatabase();

    server.listen(
      PORT,
      "0.0.0.0",
      () => {
        console.log(
          `=== Karen Messenger V4.2 PostgreSQL ===`
        );

        console.log(
          `Listening on 0.0.0.0:${PORT}`
        );
      }
    );
  } catch (err) {
    console.error("DATABASE START ERROR:");
    console.error(err);

    process.exit(1);
  }
}

start();
