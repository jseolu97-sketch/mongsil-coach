// 내 컴퓨터에서 미리 보기용 서버: npm run dev → http://localhost:3000
// 배포(Vercel)에서는 쓰이지 않는다.
import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { extname } from "node:path";
import handler from "./api/coach.js";
import dataHandler from "./api/data.js";

const PORT = Number(process.env.PORT || 3000);
const TYPES = { ".html": "text/html; charset=utf-8", ".glb": "model/gltf-binary", ".json": "application/json", ".js": "text/javascript" };
createServer(async (req, res) => {
  const url = new URL(req.url, "http://localhost");
  if (url.pathname === "/api/coach") return handler(req, res);
  if (url.pathname === "/api/data") return dataHandler(req, res);
  let p = decodeURIComponent(url.pathname); if (p === "/") p = "/index.html";
  if (p.includes("..")) { res.statusCode = 400; return res.end(); }
  try {
    const body = await readFile(new URL("./public" + p, import.meta.url));
    res.setHeader("Content-Type", TYPES[extname(p)] || "application/octet-stream");
    res.end(body);
  } catch { res.statusCode = 404; res.end("not found"); }
}).listen(PORT, "127.0.0.1", () => console.log(`몽실이 미리 보기: http://localhost:${PORT}`));
