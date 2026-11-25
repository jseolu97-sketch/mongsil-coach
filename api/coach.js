// 몽실이 AI 질문 코치 서버 함수 (Vercel Serverless Function, Node.js 20+)
// 학생 브라우저 → 이 함수(API 키 보관) → Gemini API
// 학생이 보낸 대화 내용은 기록(log)하지 않는다.
import { timingSafeEqual } from "node:crypto";

const RULES = `너는 중학생을 돕는 학습 코치 햄스터 '몽실이'야. 반말로 다정하게, 중학생이 이해할 쉬운 말을 써. 아래 규칙을 반드시 지켜.

[가장 중요한 규칙]
1. 정답이나 완성된 풀이를 먼저 알려주지 않는다. 학생이 스스로 생각하게 만든다.
2. 한 번의 답변에서 질문은 딱 하나만 한다. 답변은 짧게(3문장 이내), 맨 마지막 줄은 학생에게 던지는 질문 하나로 끝낸다.
3. 첫 답변은 (a) 학생이 이 주제에 대해 이미 알고 있는 것을 묻거나, (b) 이 문제를 풀 때 첫 단계가 무엇이라고 생각하는지 묻는다. 사진이 있으면 먼저 문제를 한 줄로 읽어 주고 "이 문제 맞니?"를 그 한 개의 질문으로 삼는다.
4. 학생 답이 맞으면 무엇이 좋았는지 구체적으로 칭찬하고 다음 단계 질문을 한다. 틀리면 "틀렸어"라고 하지 말고, 스스로 실수를 찾게 돕는 질문을 한다.
5. 학생이 [힌트 레벨 N]을 요청하면 점점 강한 힌트를 준다. 레벨1=생각할 방향만, 레벨2=써야 할 개념이나 공식 이름, 레벨3=첫 단계를 직접 보여 주기. 힌트 뒤에도 질문 하나로 끝낸다.
6. 학생이 [풀이 공개]를 요청했을 때만 단계별 풀이와 정답을 보여 주고, 마지막에 이해했는지 확인하는 비슷한 짧은 문제 하나를 낸다.
7. 학생이 스스로 정답에 도달하면 축하하고, 이해 확인을 위해 "왜 그렇게 되는지 설명해 볼래?" 또는 비슷한 문제 하나를 낸다.
8. 수식은 일반 텍스트로 쓴다(예: x^2, 3/4, √2). 마크다운 제목, 표, 굵은 글씨(**)는 쓰지 않는다.
9. 공부와 관계없거나 부적절한 요청은 부드럽게 공부 이야기로 돌린다. 숙제를 대신 써 달라는 요청에는 직접 써 주지 않고 시작할 수 있게 질문으로 돕는다.
10. 학생이 이름, 전화번호, 주소, 학교 같은 개인정보를 적으면 그 정보는 다시 말하지 말고 "개인정보는 적지 않아도 돼"라고 알려 준다.
11. 2022 개정 중학교 교육과정 수준에 맞춘다.`;

const env = (k, d) => (process.env[k] ?? "").trim() || d;
const MODELS = [
  env("GEMINI_MODEL", "gemini-flash-latest"),
  ...env("GEMINI_FALLBACK_MODELS", "gemini-3-flash-preview,gemini-3.1-flash-lite").split(",").map(s => s.trim()).filter(Boolean),
].filter((m, i, a) => a.indexOf(m) === i);
const DAILY_LIMIT = Number(env("DAILY_LIMIT", "60"));      // 기기당 하루 질문 수 (무료 한도와 과사용 방지)
const IP_DAILY_LIMIT = Number(env("IP_DAILY_LIMIT", "150")); // 같은 인터넷 주소당 하루 질문 수 (주소가 새어 나갔을 때 대비)
const PER_MINUTE = Number(env("PER_MINUTE_LIMIT", "6"));    // 기기당 1분 질문 수

/* ---------- rate limit: Upstash Redis (선택) 또는 메모리 ---------- */
const mem = new Map();
async function hit(key, ttlSec) {
  const url = env("UPSTASH_REDIS_REST_URL", env("KV_REST_API_URL", "")), token = env("UPSTASH_REDIS_REST_TOKEN", env("KV_REST_API_TOKEN", ""));
  if (url && token) {
    const r = await fetch(`${url}/pipeline`, {
      method: "POST",
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      body: JSON.stringify([["INCR", key], ["EXPIRE", key, String(ttlSec), "NX"]]),
    });
    if (r.ok) { const j = await r.json(); return Number(j?.[0]?.result ?? 0); }
  }
  const now = Date.now(), e = mem.get(key);
  if (!e || e.exp < now) { mem.set(key, { n: 1, exp: now + ttlSec * 1000 }); return 1; }
  e.n++; return e.n;
}
function kstDay() { return new Date(Date.now() + 9 * 3600e3).toISOString().slice(0, 10); }

/* ---------- helpers ---------- */
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
  for await (const c of req) { size += c.length; if (size > 4_000_000) throw new Error("too_large"); chunks.push(c); }
  return JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}");
}
function sameText(a, b) {
  const x = Buffer.from(String(a)), y = Buffer.from(String(b));
  return x.length === y.length && timingSafeEqual(x, y);
}
function clientIp(req) {
  return String(req.headers["x-forwarded-for"] || req.socket?.remoteAddress || "unknown").split(",")[0].trim();
}

/* ---------- Gemini ---------- */
const OCR_RULES = `너는 사진 속 시험 문제를 글로 옮겨 적는 도우미야. 문제를 풀거나 정답, 힌트를 절대 쓰지 않는다.
보이는 문제 글(보기 포함)을 그대로 옮기고, 수식은 x^2, 3/4, √2 같은 일반 텍스트로 쓴다.
학생이 손으로 쓴 답이 보이면 mine에 적고, 없으면 빈 문자열로 둔다.
사진 속 이름, 학교, 번호 같은 개인정보는 옮기지 않는다.
반드시 JSON 하나로만 답한다: {"question": string, "mine": string, "subj": "kor"|"eng"|"math"|"sci"|"soc"|"his"|"etc"}`;

async function callGemini(contents, { system = RULES, json = false } = {}) {
  const key = env("GEMINI_API_KEY", "");
  if (!key) return { error: "server_not_configured" };
  let last = { error: "upstream_error" };
  for (const model of MODELS) {
    let r;
    try {
      r = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(model)}:generateContent`, {
        method: "POST",
        headers: { "Content-Type": "application/json", "x-goog-api-key": key },
        body: JSON.stringify({
          systemInstruction: { parts: [{ text: system }] },
          contents,
          generationConfig: json
            ? { temperature: 0.1, maxOutputTokens: 2048, responseMimeType: "application/json" }
            : { temperature: 0.6, maxOutputTokens: 2048 },
        }),
      });
    } catch { last = { error: "upstream_error" }; continue; }
    if (r.status === 404 || r.status === 429 || r.status >= 500) {
      // 모델이 없거나(404), 이 모델의 무료 한도가 찼거나(429), 일시 오류면 다음 모델로
      last = { error: r.status === 429 ? "quota" : "upstream_error", status: r.status, model };
      continue;
    }
    const j = await r.json().catch(() => ({}));
    if (!r.ok) return { error: r.status === 400 || r.status === 403 ? "bad_key_or_request" : "upstream_error", status: r.status, model };
    if (j.promptFeedback?.blockReason) return { error: "refused" };
    const cand = j.candidates?.[0];
    const text = (cand?.content?.parts || []).map(p => p.text || "").join("").trim();
    if (!text) return { error: cand?.finishReason === "SAFETY" ? "refused" : "empty" };
    return { text, model };
  }
  return last;
}

const MSG = {
  wrong_code: "비밀번호가 맞지 않아요. 선생님께 받은 비밀번호를 다시 확인해 주세요.",
  daily: "오늘 몽실이에게 물어볼 수 있는 횟수를 다 썼어요. 내일 다시 만나!",
  minute: "질문이 너무 빨라요. 1분만 쉬었다가 보내 주세요.",
  quota: "지금 몽실이를 찾는 친구가 너무 많아요. 잠시 뒤에 다시 보내 주세요.",
  refused: "그 내용은 도와줄 수 없어요. 질문을 바꿔 볼까요?",
  bad_request: "보낸 내용을 읽을 수 없어요. 새 질문으로 다시 시작해 주세요.",
  server_not_configured: "선생님이 아직 AI 키를 설정하지 않았어요.",
  bad_key_or_request: "AI 연결 설정에 문제가 있어요. 선생님께 알려 주세요.",
  empty: "몽실이가 대답을 못 만들었어요. 질문을 조금 바꿔 다시 보내 주세요.",
  upstream_error: "연결이 잠깐 끊겼어요. 다시 보내 주세요.",
};

/* ---------- handler ---------- */
export default async function handler(req, res) {
  // (선택) 비밀번호: 비워 두면 주소만 알면 누구나 사용. 정하면 그 비밀번호를 아는 사람만 사용 (예전 이름 CLASS_CODE도 인정)
  const classCode = env("ACCESS_CODE", env("CLASS_CODE", ""));
  if (req.method === "GET") return send(res, 200, { ok: true, needsCode: !!classCode, dailyLimit: DAILY_LIMIT });
  if (req.method !== "POST") return send(res, 405, { error: "method" });

  if (classCode && !sameText(req.headers["x-class-code"] || "", classCode))
    return send(res, 401, { error: "wrong_code", message: MSG.wrong_code });

  let body;
  try { body = await readBody(req); } catch { return send(res, 400, { error: "bad_request", message: MSG.bad_request }); }

  const isOcr = body?.task === "ocr";
  // 입력 검사
  const turns = isOcr ? [{ role: "user", content: "이 사진 속 문제를 JSON으로 옮겨 적어 줘." }] : Array.isArray(body?.turns) ? body.turns : [];
  if (isOcr && !body.image) return send(res, 400, { error: "bad_request", message: "사진이 없어요. 다시 골라 주세요." });
  let total = 0;
  const okTurns = turns.length >= 1 && turns.length <= 40 && turns.every(t => {
    const ok = t && (t.role === "user" || t.role === "assistant") && typeof t.content === "string" && t.content.trim() && t.content.length <= 3000;
    total += ok ? t.content.length : 0; return ok;
  }) && turns[0].role === "user" && turns.at(-1).role === "user" && total <= 40000;
  if (!okTurns) return send(res, 400, { error: "bad_request", message: MSG.bad_request });
  let image = null;
  if (body.image) {
    const { mime, data } = body.image;
    if (!["image/jpeg", "image/png", "image/webp"].includes(mime) || typeof data !== "string" || data.length > 3_000_000 || !/^[A-Za-z0-9+/=]+$/.test(data))
      return send(res, 400, { error: "bad_request", message: MSG.bad_request });
    image = { mime, data };
  }

  // 사용량 제한
  const device = String(req.headers["x-device-id"] || "").replace(/[^a-z0-9-]/gi, "").slice(0, 40) || "anon";
  const ip = clientIp(req), day = kstDay();
  try {
    const perMin = await hit(`m:${device}:${Math.floor(Date.now() / 60000)}`, 70);
    if (perMin > PER_MINUTE) return send(res, 429, { error: "minute", message: MSG.minute });
    const perDev = await hit(`d:${day}:${device}`, 90000);
    const perIp = await hit(`i:${day}:${ip}`, 90000);
    if (perDev > DAILY_LIMIT || perIp > IP_DAILY_LIMIT) return send(res, 429, { error: "daily", message: MSG.daily, remaining: 0 });
    var remaining = Math.max(0, DAILY_LIMIT - perDev);
  } catch { var remaining = null; }

  // Gemini 형식으로 변환 (연속된 같은 역할은 합친다)
  const contents = [];
  for (const t of turns) {
    const role = t.role === "assistant" ? "model" : "user";
    const prev = contents.at(-1);
    if (prev && prev.role === role) prev.parts.push({ text: t.content });
    else contents.push({ role, parts: [{ text: t.content }] });
  }
  if (image) contents[0].parts.unshift({ inline_data: { mime_type: image.mime, data: image.data } });

  if (isOcr) {
    if (env("MOCK_GEMINI", "") === "1") return send(res, 200, { question: "(모의) 2x + 3 = 11 을 풀어라.", mine: "x = 7", subj: "math", remaining });
    const out = await callGemini(contents, { system: OCR_RULES, json: true });
    if (out.error) {
      const status = out.error === "quota" ? 429 : out.error === "refused" ? 422 : out.error === "server_not_configured" ? 503 : 502;
      console.warn("ocr_error", out.error, out.status || "", out.model || "");
      return send(res, status, { error: out.error, message: MSG[out.error] || MSG.upstream_error });
    }
    let j = {};
    try { j = JSON.parse(out.text.replace(/^```(?:json)?\s*|\s*```$/g, "")); } catch { j = {}; }
    const subj = ["kor", "eng", "math", "sci", "soc", "his", "etc"].includes(j.subj) ? j.subj : "etc";
    return send(res, 200, { question: String(j.question || "").slice(0, 1500), mine: String(j.mine || "").slice(0, 300), subj, remaining });
  }

  if (env("MOCK_GEMINI", "") === "1") {
    const lastText = turns.at(-1).content;
    const text = lastText.includes("[풀이 공개]") ? "(모의 응답) 단계별 풀이야. 1) ... 2) ...\n비슷한 문제: 3x + 2 = 11 을 풀어 볼래?"
      : lastText.includes("[힌트 레벨") ? "(모의 응답) 힌트: 양쪽에 같은 수를 더하거나 빼도 등식은 그대로야.\n그럼 3을 없애려면 어떻게 해야 할까?"
      : "(모의 응답) 좋아, 같이 해 보자! 이 문제에서 구하라는 게 뭐라고 생각해?";
    return send(res, 200, { text, remaining, model: "mock" });
  }

  const out = await callGemini(contents);
  if (out.error) {
    const status = out.error === "quota" ? 429 : out.error === "refused" ? 422 : out.error === "server_not_configured" ? 503 : 502;
    console.warn("coach_error", out.error, out.status || "", out.model || ""); // 내용은 기록하지 않음
    return send(res, status, { error: out.error, message: MSG[out.error] || MSG.upstream_error });
  }
  return send(res, 200, { text: out.text, remaining, model: out.model });
}
