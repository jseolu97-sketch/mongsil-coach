// 학생별 공부 기록 저장 (Vercel Serverless Function)
// 학생마다 브라우저가 만든 '내 공부 열쇠'로 자기 공간에만 읽고 쓴다. 다른 학생의 기록은 볼 수 없다.
// 저장소: Upstash Redis (Vercel Marketplace의 "Upstash for Redis"를 연결하면 환경변수가 자동으로 들어옴)
// 서버에는 열쇠 원문이 아니라 해시값만 저장한다. 기록 내용은 로그로 남기지 않는다.
import { createHash } from "node:crypto";

const env = (k, d) => (process.env[k] ?? "").trim() || d;
const REDIS_URL = () => env("UPSTASH_REDIS_REST_URL", env("KV_REST_API_URL", ""));
const REDIS_TOKEN = () => env("UPSTASH_REDIS_REST_TOKEN", env("KV_REST_API_TOKEN", ""));
const MAX_BYTES = 200_000;          // 학생 한 명 기록 최대 크기 (사진은 기기에만 저장되므로 충분)
const KEEP_DAYS = Number(env("DATA_KEEP_DAYS", "400")); // 이 기간 동안 접속이 없으면 자동 삭제

const mem = globalThis.__mongsilMem || (globalThis.__mongsilMem = new Map()); // 내 컴퓨터 미리 보기 전용

async function redis(cmd) {
  const r = await fetch(REDIS_URL(), {
    method: "POST",
    headers: { Authorization: `Bearer ${REDIS_TOKEN()}`, "Content-Type": "application/json" },
    body: JSON.stringify(cmd),
  });
  if (!r.ok) throw new Error("redis_" + r.status);
  return (await r.json()).result;
}
function backend() {
  if (REDIS_URL() && REDIS_TOKEN()) return "redis";
  if (env("DEV_MEMORY_STORE", "") === "1") return "memory";
  return null;
}
async function store(op, key, value, ttl) {
  const b = backend();
  if (b === "redis") {
    if (op === "get") return await redis(["GET", key]);
    if (op === "set") return await redis(["SET", key, value, "EX", String(ttl)]);
    if (op === "incr") { const n = await redis(["INCR", key]); if (n === 1) await redis(["EXPIRE", key, String(ttl)]); return n; }
  }
  const now = Date.now(), e = mem.get(key);
  const live = e && e.exp > now ? e : null;
  if (op === "get") return live ? live.v : null;
  if (op === "set") { mem.set(key, { v: value, exp: now + ttl * 1000 }); return "OK"; }
  if (op === "incr") { const n = (live ? Number(live.v) : 0) + 1; mem.set(key, { v: n, exp: live ? live.exp : now + ttl * 1000 }); return n; }
}

function send(res, status, obj) {
  res.statusCode = status;
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.setHeader("Cache-Control", "no-store");
  res.end(JSON.stringify(obj));
}
async function readBody(req) {
  if (req.body && typeof req.body === "object") return req.body;
  if (typeof req.body === "string") return JSON.parse(req.body);
  const chunks = []; let size = 0;
  for await (const c of req) { size += c.length; if (size > MAX_BYTES + 10_000) throw new Error("too_large"); chunks.push(c); }
  return JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}");
}
const ipOf = req => String(req.headers["x-forwarded-for"] || req.socket?.remoteAddress || "unknown").split(",")[0].trim();
const hourSlot = () => Math.floor(Date.now() / 3600e3);

export default async function handler(req, res) {
  if (!backend()) return send(res, 503, { error: "storage_not_configured", message: "서버 저장소가 아직 연결되지 않아 이 기기에만 저장돼요." });

  const key = String(req.headers["x-study-key"] || "");
  if (!/^mong(-[a-z0-9]{4}){4}$/.test(key)) return send(res, 400, { error: "bad_key", message: "공부 열쇠 모양이 올바르지 않아요." });
  const id = createHash("sha256").update(env("DATA_SALT", "mongsil") + ":" + key).digest("hex").slice(0, 40);
  const ttl = KEEP_DAYS * 86400;

  try {
    // 남용 방지: 열쇠당 시간당 요청 수, 같은 인터넷 주소의 시간당 요청 수
    const perKey = await store("incr", `rk:${id}:${hourSlot()}`, null, 3700);
    const perIp = await store("incr", `ri:${ipOf(req)}:${hourSlot()}`, null, 3700);
    if (perKey > 400 || perIp > Number(env("DATA_IP_HOURLY_LIMIT", "3000")))
      return send(res, 429, { error: "rate", message: "저장 요청이 너무 많아요. 잠시 뒤에 다시 시도해 주세요." });

    if (req.method === "GET") {
      const raw = await store("get", `st:${id}`);
      if (!raw) return send(res, 200, { state: null });
      const doc = typeof raw === "string" ? JSON.parse(raw) : raw;
      await store("set", `st:${id}`, typeof raw === "string" ? raw : JSON.stringify(doc), ttl); // 접속하면 보관 기간 연장
      return send(res, 200, { state: doc.s ?? null, updatedAt: doc.u ?? 0 });
    }
    if (req.method === "PUT") {
      let body;
      try { body = await readBody(req); } catch { return send(res, 413, { error: "too_large", message: "기록이 너무 커서 저장할 수 없어요." }); }
      const s = body?.state;
      if (!s || typeof s !== "object" || Array.isArray(s)) return send(res, 400, { error: "bad_state", message: "저장할 내용이 올바르지 않아요." });
      const value = JSON.stringify({ s, u: Number(s.updatedAt) || Date.now() });
      if (Buffer.byteLength(value) > MAX_BYTES) return send(res, 413, { error: "too_large", message: "기록이 너무 커서 저장할 수 없어요. 오래된 오답을 정리해 주세요." });
      await store("set", `st:${id}`, value, ttl);
      return send(res, 200, { ok: true });
    }
    return send(res, 405, { error: "method" });
  } catch (e) {
    console.warn("data_error", String(e?.message || e).slice(0, 80)); // 기록 내용은 남기지 않음
    return send(res, 502, { error: "storage_error", message: "저장소에 잠깐 연결하지 못했어요. 이 기기에는 저장돼 있어요." });
  }
}
