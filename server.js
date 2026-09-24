const express=require("express");
const http=require("http");
const WebSocket=require("ws");
const fs=require("fs");
const path=require("path");

const app=express();
const server=http.createServer(app);
const wss=new WebSocket.Server({server});
const PORT=Number(process.env.PORT||3000);
const DB=path.join(__dirname,"data.json");

app.use(express.json({limit:"12mb"}));
app.use(express.static(path.join(__dirname,"public")));

let db={users:[],messages:[]};
function save(){fs.writeFileSync(DB,JSON.stringify(db,null,2));}
function load(){try{db=JSON.parse(fs.readFileSync(DB,"utf8"));if(!Array.isArray(db.users))db.users=[];if(!Array.isArray(db.messages))db.messages=[];}catch{db={users:[],messages:[]};save();}}
load();

const clean=v=>String(v??"").trim();
const send=(ws,x)=>{if(ws?.readyState===WebSocket.OPEN)ws.send(JSON.stringify(x));};
const online=u=>{let ok=false;wss.clients.forEach(c=>{if(c.username===u)ok=true});return ok;};
const publicUsers=()=>db.users.map(u=>({username:u.username,name:u.name||u.username,bio:u.bio||"",avatar:u.avatar||"",online:online(u.username)}));
const broadcast=x=>wss.clients.forEach(c=>send(c,x));
const validUsername=u=>/^[A-Za-z0-9_.-]{3,32}$/.test(u);

app.get("/api/health",(req,res)=>res.json({ok:true,version:"4.1.0",users:db.users.length,messages:db.messages.length}));
app.get("/api/users",(req,res)=>res.json(publicUsers()));

app.post("/api/register",(req,res)=>{
 const username=clean(req.body.username),password=String(req.body.password||"");
 if(!validUsername(username))return res.json({error:"نام کاربری ۳ تا ۳۲ کاراکتر و فقط حروف انگلیسی، عدد، نقطه، - یا _ باشد."});
 if(password.length<4)return res.json({error:"رمز حداقل ۴ کاراکتر باشد."});
 if(db.users.some(u=>u.username.toLowerCase()===username.toLowerCase()))return res.json({error:"این نام کاربری وجود دارد."});
 db.users.push({username,password,name:username,bio:"",avatar:""});save();
 res.json({success:true,username});
});

app.post("/api/login",(req,res)=>{
 const username=clean(req.body.username),password=String(req.body.password||"");
 const u=db.users.find(x=>x.username.toLowerCase()===username.toLowerCase()&&x.password===password);
 if(!u)return res.json({error:"نام کاربری یا رمز اشتباه است."});
 res.json({success:true,username:u.username});
});

app.get("/api/messages",(req,res)=>{
 const a=clean(req.query.a),b=clean(req.query.b);
 res.json(db.messages.filter(m=>(m.from===a&&m.to===b)||(m.from===b&&m.to===a)).slice(-500));
});

app.post("/api/messages",(req,res)=>{
 const from=clean(req.body.from),to=clean(req.body.to);
 const text=clean(req.body.text),image=String(req.body.image||""),audio=String(req.body.audio||"");
 if(!from||!to)return res.json({error:"مخاطب انتخاب نشده."});
 if(!db.users.some(u=>u.username===from))return res.json({error:"فرستنده پیدا نشد. دوباره وارد شو."});
 if(!db.users.some(u=>u.username===to))return res.json({error:"مخاطب پیدا نشد. از فهرست کاربران انتخابش کن."});
 if(!text&&!image&&!audio)return res.json({error:"پیام خالی است."});
 if(image.length>3000000)return res.json({error:"عکس خیلی بزرگ است."});
 if(audio.length>8000000)return res.json({error:"ویس خیلی بزرگ است."});
 const m={id:Date.now().toString(36)+Math.random().toString(36).slice(2),from,to,text,image,audio,reactions:{},time:new Date().toISOString(),read:false};
 db.messages.push(m);if(db.messages.length>20000)db.messages=db.messages.slice(-20000);save();
 wss.clients.forEach(c=>{if(c.username===from||c.username===to)send(c,{type:"message",message:m});});
 res.json({success:true,message:m});
});

app.post("/api/read",(req,res)=>{
 const from=clean(req.body.from),to=clean(req.body.to);
 db.messages.forEach(m=>{if(m.from===from&&m.to===to)m.read=true});save();broadcast({type:"read",from,to});res.json({success:true});
});

app.post("/api/react",(req,res)=>{
 const id=clean(req.body.id),emoji=String(req.body.emoji||""),username=clean(req.body.username);
 const m=db.messages.find(x=>x.id===id);if(!m)return res.json({error:"پیام پیدا نشد."});
 m.reactions=m.reactions||{};
 Object.keys(m.reactions).forEach(e=>m.reactions[e]=(m.reactions[e]||[]).filter(x=>x!==username));
 if(emoji!=="none"){m.reactions[emoji]=m.reactions[emoji]||[];m.reactions[emoji].push(username)}
 save();broadcast({type:"reaction",id,reactions:m.reactions});res.json({success:true});
});

app.get("/api/profile",(req,res)=>{
 const u=db.users.find(x=>x.username===clean(req.query.username));
 if(!u)return res.status(404).json({error:"کاربر پیدا نشد."});
 res.json({username:u.username,name:u.name||u.username,bio:u.bio||"",avatar:u.avatar||""});
});
app.post("/api/profile",(req,res)=>{
 const u=db.users.find(x=>x.username===clean(req.body.username));
 if(!u)return res.json({error:"کاربر پیدا نشد."});
 u.name=clean(req.body.name)||u.username;u.bio=String(req.body.bio||"").slice(0,200);u.avatar=String(req.body.avatar||"");
 if(u.avatar.length>1800000)return res.json({error:"عکس خیلی بزرگ است."});
 save();broadcast({type:"users",users:publicUsers()});res.json({success:true});
});

wss.on("connection",ws=>{
 ws.on("message",raw=>{try{const d=JSON.parse(raw);if(d.type==="auth"){const u=clean(d.username);if(db.users.some(x=>x.username===u)){ws.username=u;send(ws,{type:"users",users:publicUsers()});broadcast({type:"users",users:publicUsers()});}}}catch{}});
 ws.on("close",()=>broadcast({type:"users",users:publicUsers()}));
});

server.listen(PORT,"0.0.0.0",()=>console.log(`=== Karen Messenger V4.1 REMOTE ===\nListening on 0.0.0.0:${PORT}`));
