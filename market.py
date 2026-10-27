"""بازارچه امن آیدی سروش‌پلاس — بات + API (دیتابیس: Turso روی HTTP)
pip install aiohttp
ENV: BOT_TOKEN, ADMIN_ID, CARD, TURSO_URL, TURSO_TOKEN
     [LOCK_HOURS=12] [FEE_PERCENT=5] [NEW_SELLER_MAX=2000000] [PORT=8080] [FRAME_ANCESTORS=https://*.splus.ir]
"""
import os, time, json, hmac, hashlib, random, re, asyncio, logging
from pathlib import Path
from urllib.parse import parse_qsl
import aiohttp
from aiohttp import web

TOKEN = os.environ["BOT_TOKEN"]
ADMIN = int(os.environ.get("ADMIN_ID", "0"))
CARD = os.environ.get("CARD", "")
LOCK_H = float(os.environ.get("LOCK_HOURS", "12"))
FEE = float(os.environ.get("FEE_PERCENT", "5"))
NEW_MAX = int(os.environ.get("NEW_SELLER_MAX", "2000000"))
FRAME = os.environ.get("FRAME_ANCESTORS", "https://*.splus.ir")
TURSO_URL = os.environ["TURSO_URL"].strip().replace("libsql://", "https://", 1).rstrip("/")
TURSO_TOKEN = os.environ["TURSO_TOKEN"].strip()
API = f"https://api.splus.ir/bot{TOKEN}/"
log = logging.getLogger("market")
BOT_USER = ""
SES = None

SCHEMA = [
    "CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY, username TEXT, name TEXT, done INTEGER DEFAULT 0)",
    """CREATE TABLE IF NOT EXISTS pending(uid INTEGER PRIMARY KEY, uname TEXT, code TEXT, price INTEGER, descr TEXT,
       exp REAL, tries INTEGER DEFAULT 0, state TEXT DEFAULT 'wait', why TEXT DEFAULT '')""",
    """CREATE TABLE IF NOT EXISTS listings(id INTEGER PRIMARY KEY AUTOINCREMENT, seller INTEGER, uname TEXT,
       price INTEGER, descr TEXT, status TEXT DEFAULT 'active', ts REAL)""",
    """CREATE TABLE IF NOT EXISTS deals(id INTEGER PRIMARY KEY AUTOINCREMENT, lid INTEGER, buyer INTEGER, seller INTEGER,
       uname TEXT, price INTEGER, status TEXT, lock REAL, ts REAL)""",
]


# ---------- Turso (HTTP / pipeline API) ----------
def _arg(v):
    if v is None:
        return {"type": "null"}
    if isinstance(v, (bool, int)):
        return {"type": "integer", "value": str(int(v))}
    if isinstance(v, float):
        return {"type": "float", "value": v}
    return {"type": "text", "value": str(v)}


def _val(c):
    t, v = c.get("type"), c.get("value")
    if t == "null":
        return None
    if t == "integer":
        return int(v)
    if t == "float":
        return float(v)
    return v


class Res:
    def __init__(self, r):
        cols = [c["name"] for c in r.get("cols", [])]
        self.rows = [dict(zip(cols, map(_val, row))) for row in r.get("rows", [])]
        self.rowcount = r.get("affected_row_count", 0)
        lid = r.get("last_insert_rowid")
        self.lastrowid = int(lid) if lid is not None else None

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows


async def q(sql, *a):
    body = {"requests": [{"type": "execute", "stmt": {"sql": sql, "args": [_arg(x) for x in a]}}, {"type": "close"}]}
    for attempt in (1, 2):
        try:
            async with SES.post(TURSO_URL + "/v2/pipeline", json=body,
                                headers={"Authorization": "Bearer " + TURSO_TOKEN},
                                timeout=aiohttp.ClientTimeout(total=20)) as r:
                j = await r.json(content_type=None)
                if r.status != 200:
                    raise RuntimeError(f"turso http {r.status}: {j}")
            break
        except (aiohttp.ClientError, asyncio.TimeoutError):
            if attempt == 2:
                raise
            await asyncio.sleep(0.3)
    res = j["results"][0]
    if res["type"] == "error":
        raise RuntimeError("turso: " + res["error"]["message"])
    return Res(res["response"]["result"])


class Err(Exception):
    pass


# ---------- Soroush Plus bot API ----------
async def call(ses, method, **p):
    async with ses.post(API + method, json=p, timeout=aiohttp.ClientTimeout(total=45)) as r:
        j = await r.json()
    if not j.get("ok"):
        raise RuntimeError(f"{method}: {j.get('description')}")
    return j["result"]


async def say(chat, text, kb=None):
    p = {"chat_id": chat, "text": text}
    if kb:
        p["reply_markup"] = {"inline_keyboard": kb}
    try:
        await call(SES, "sendMessage", **p)
    except Exception as e:
        log.warning("send fail %s", e)


def btn(t, d):
    return [{"text": t, "callback_data": d}]


# ---------- initData validation (per Soroush Plus docs) ----------
def check_init(raw):
    try:
        d = dict(parse_qsl(raw, keep_blank_values=True))
        h = d.pop("hash")
        d.pop("signature", None)
        s = "\n".join(f"{k}={d[k]}" for k in sorted(d)).encode()
        keys = (hmac.new(b"WebAppData", TOKEN.encode(), hashlib.sha256).digest(),
                hmac.new(TOKEN.encode(), b"WebAppData", hashlib.sha256).digest())  # docs wording is ambiguous: try both
        for k in keys:
            if hmac.compare_digest(hmac.new(k, s, hashlib.sha256).hexdigest(), h):
                if time.time() - int(d["auth_date"]) > 86400:
                    return None
                return json.loads(d["user"])
    except Exception:
        pass
    return None


def api(fn):
    async def w(req):
        u = check_init(req.headers.get("X-Init", ""))
        if not u:
            return web.json_response({"error": "احراز هویت ناموفق؛ برنامک را از داخل سروش‌پلاس باز کنید"}, status=401)
        await q("INSERT INTO users(id,username,name) VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET username=excluded.username,name=excluded.name",
                u["id"], (u.get("username") or "").lower(), u.get("first_name", ""))
        try:
            return web.json_response(await fn(req, u))
        except Err as e:
            return web.json_response({"error": str(e)}, status=400)
    return w


# ---------- API ----------
@api
async def listings(req, u):
    rows, sold, st = await asyncio.gather(
        q("""SELECT l.id,l.uname,l.price,l.descr,l.seller,l.ts,(SELECT done FROM users WHERE id=l.seller) done
             FROM listings l WHERE status='active' ORDER BY id DESC LIMIT 300"""),
        q("SELECT uname,price,ts FROM listings WHERE status='sold' ORDER BY ts DESC LIMIT 60"),
        q("""SELECT (SELECT COUNT(*) FROM listings WHERE status='active') active,
                    (SELECT COUNT(*) FROM listings WHERE status='sold') sold,
                    (SELECT COALESCE(SUM(price),0) FROM listings WHERE status='sold') volume"""))
    return {"items": [dict(r, mine=r["seller"] == u["id"]) for r in rows.fetchall()],
            "sold": sold.fetchall(), "stats": st.fetchone()}


@api
async def verify_start(req, u):
    b = await req.json()
    un = str(b.get("uname", "")).strip().lstrip("@").lower()
    try:
        price = int(b.get("price"))
    except Exception:
        raise Err("قیمت نامعتبر است")
    if not re.fullmatch(r"[a-z0-9_]{3,32}", un):
        raise Err("آیدی نامعتبر است")
    if price < 10000:
        raise Err("حداقل قیمت ۱۰,۰۰۰ تومان است")
    me = (await q("SELECT done FROM users WHERE id=?", u["id"])).fetchone()
    if ((me or {}).get("done") or 0) == 0 and price > NEW_MAX:
        raise Err(f"فروشنده‌های جدید تا سقف {NEW_MAX:,} تومان می‌توانند آگهی بدهند")
    if (await q("SELECT 1 FROM listings WHERE uname=? AND status IN('active','reserved')", un)).fetchone():
        raise Err("این آیدی همین الان آگهی فعال دارد")
    code = f"{random.SystemRandom().randrange(1000, 10000)}"
    await q("REPLACE INTO pending(uid,uname,code,price,descr,exp,tries,state,why) VALUES(?,?,?,?,?,?,0,'wait','')",
            u["id"], un, code, price, str(b.get("descr", ""))[:300], time.time() + 300)
    return {"code": code, "ttl": 300, "bot": BOT_USER}


@api
async def verify_status(req, u):
    p = (await q("SELECT * FROM pending WHERE uid=?", u["id"])).fetchone()
    if not p:
        return {"state": "none"}
    st = p["state"]
    if st == "wait" and time.time() > p["exp"]:
        await q("UPDATE pending SET state='expired' WHERE uid=?", u["id"])
        st = "expired"
    return {"state": st, "why": p["why"], "left": max(0, int(p["exp"] - time.time())), "tries": 3 - p["tries"]}


def dview(d, uid):
    o = dict(id=d["id"], uname=d["uname"], price=d["price"], status=d["status"],
             role="buyer" if d["buyer"] == uid else "seller",
             lock=max(0, int((d["lock"] or 0) - time.time())), payout=int(d["price"] * (100 - FEE) / 100))
    if o["role"] == "buyer" and d["status"] == "wait_pay":
        o["card"] = CARD
    return o


@api
async def deals_new(req, u):
    lid = int((await req.json()).get("lid", 0))
    l = (await q("SELECT * FROM listings WHERE id=? AND status='active'", lid)).fetchone()
    if not l:
        raise Err("این آگهی دیگر موجود نیست")
    if l["seller"] == u["id"]:
        raise Err("آگهی خودتان را نمی‌توانید بخرید")
    if (await q("SELECT COUNT(*) c FROM deals WHERE buyer=? AND status='wait_pay'", u["id"])).fetchone()["c"] >= 2:
        raise Err("ابتدا معاملات در انتظار پرداخت قبلی را تعیین تکلیف کنید")
    # رزرو اتمیک: اگر دو نفر همزمان بزنند فقط یکی موفق می‌شود
    if not (await q("UPDATE listings SET status='reserved' WHERE id=? AND status='active'", lid)).rowcount:
        raise Err("این آگهی دیگر موجود نیست")
    cur = await q("INSERT INTO deals(lid,buyer,seller,uname,price,status,ts) VALUES(?,?,?,?,?,'wait_pay',?)",
                  lid, u["id"], l["seller"], l["uname"], l["price"], time.time())
    return {"id": cur.lastrowid}


@api
async def deals_list(req, u):
    rows = (await q("SELECT * FROM deals WHERE buyer=? OR seller=? ORDER BY id DESC LIMIT 40", u["id"], u["id"])).fetchall()
    return {"items": [dview(r, u["id"]) for r in rows]}


@api
async def deal_get(req, u):
    d = (await q("SELECT * FROM deals WHERE id=? AND (buyer=? OR seller=?)", int(req.match_info["id"]), u["id"], u["id"])).fetchone()
    if not d:
        raise Err("معامله پیدا نشد")
    return dview(d, u["id"])


@api
async def deal_act(req, u):
    did, act = int(req.match_info["id"]), (await req.json()).get("act")
    d = (await q("SELECT * FROM deals WHERE id=? AND (buyer=? OR seller=?)", did, u["id"], u["id"])).fetchone()
    if not d:
        raise Err("معامله پیدا نشد")
    buyer, seller, st = d["buyer"] == u["id"], d["seller"] == u["id"], d["status"]
    if act == "cancel" and buyer and st == "wait_pay":
        if (await q("UPDATE deals SET status='cancel' WHERE id=? AND status='wait_pay'", did)).rowcount:
            await q("UPDATE listings SET status='active' WHERE id=?", d["lid"])
    elif act == "handed" and seller and st == "paid":
        if (await q("UPDATE deals SET status='handed',lock=? WHERE id=? AND status='paid'", time.time() + LOCK_H * 3600, did)).rowcount:
            await say(d["buyer"], f"فروشنده آیدی @{d['uname']} را تحویل داد.\nتا {LOCK_H:g} ساعت فرصت دارید تأیید یا شکایت کنید (از داخل برنامک).")
    elif act == "confirm" and buyer and st == "handed":
        await set_due(did)
    elif act == "dispute" and buyer and st == "handed":
        if (await q("UPDATE deals SET status='dispute' WHERE id=? AND status='handed'", did)).rowcount:
            await say(ADMIN, f"⚠️ شکایت در معامله #{did} (@{d['uname']}، {d['price']:,} تومان).\nبا طرفین هماهنگ کنید و تصمیم بگیرید:",
                      [btn("واریز به فروشنده", f"pay:{did}"), btn("بازگشت وجه به خریدار", f"ref:{did}")])
    else:
        raise Err("این عمل در وضعیت فعلی ممکن نیست")
    return {"ok": True}


async def set_due(did):
    if not (await q("UPDATE deals SET status='due' WHERE id=? AND status='handed'", did)).rowcount:
        return
    d = (await q("SELECT * FROM deals WHERE id=?", did)).fetchone()
    await say(ADMIN, f"✅ معامله #{did} آزاد شد. مبلغ قابل واریز به فروشنده: {int(d['price'] * (100 - FEE) / 100):,} تومان.",
              [btn("واریز شد", f"pay:{did}")])
    await say(d["seller"], f"معامله #{did} تأیید شد و مبلغ به‌زودی توسط پشتیبانی واریز می‌شود.")


# ---------- bot updates ----------
async def on_message(m):
    if m["chat"]["type"] != "private" or "from" not in m:
        return
    u, uid, txt = m["from"], m["from"]["id"], (m.get("text") or "").strip()
    if txt.startswith("/start"):
        return await say(uid, "به بازارچه امن آیدی خوش آمدید 👋\nبرای خرید و فروش، برنامک را از دکمه منوی همین بات باز کنید.\n"
                              "پس از پرداخت، عکس رسید را همین‌جا بفرستید.")
    if m.get("photo"):
        d = (await q("SELECT * FROM deals WHERE buyer=? AND status='wait_pay' ORDER BY id DESC LIMIT 1", uid)).fetchone()
        if not d:
            return await say(uid, "معامله‌ی در انتظار پرداخت ندارید.")
        try:
            await call(SES, "forwardMessage", chat_id=ADMIN, from_chat_id=m["chat"]["id"], message_id=m["message_id"])
        except Exception as e:
            log.warning(e)
        await say(ADMIN, f"رسید معامله #{d['id']} — @{d['uname']} — {d['price']:,} تومان — خریدار {uid}",
                  [btn("تأیید پرداخت", f"ok:{d['id']}"), btn("رد", f"no:{d['id']}")])
        return await say(uid, "رسید دریافت شد؛ پس از تأیید پشتیبانی به شما خبر می‌دهیم.")
    if re.fullmatch(r"\d{4}", txt):
        p = (await q("SELECT * FROM pending WHERE uid=? AND state='wait'", uid)).fetchone()
        if not p:
            return await say(uid, "درخواست تأیید فعالی ندارید؛ از داخل برنامک کد بگیرید.")
        if time.time() > p["exp"]:
            await q("UPDATE pending SET state='expired' WHERE uid=?", uid)
            return await say(uid, "کد منقضی شده است.")
        if (u.get("username") or "").lower() != p["uname"]:
            await q("UPDATE pending SET state='fail',why='آیدی واردشده متعلق به این اکانت نیست' WHERE uid=?", uid)
            return await say(uid, "❌ آیدی واردشده با اکانت شما یکی نیست.")
        if hmac.compare_digest(txt, p["code"]):
            if (await q("UPDATE pending SET state='ok' WHERE uid=? AND state='wait'", uid)).rowcount:
                await q("INSERT INTO listings(seller,uname,price,descr,ts) VALUES(?,?,?,?,?)", uid, p["uname"], p["price"], p["descr"], time.time())
                return await say(uid, f"✅ مالکیت @{p['uname']} تأیید و آگهی ثبت شد.")
            return
        t = p["tries"] + 1
        if t >= 3:
            await q("UPDATE pending SET state='fail',tries=?,why='سه بار کد اشتباه' WHERE uid=?", t, uid)
            return await say(uid, "❌ سه بار کد اشتباه؛ درخواست باطل شد.")
        await q("UPDATE pending SET tries=? WHERE uid=?", t, uid)
        return await say(uid, f"کد اشتباه است. {3 - t} تلاش باقی مانده.")


async def on_callback(c):
    await call(SES, "answerCallbackQuery", callback_query_id=c["id"])
    if c["from"]["id"] != ADMIN or not c.get("data"):
        return
    act, did = c["data"].split(":")
    did = int(did)
    d = (await q("SELECT * FROM deals WHERE id=?", did)).fetchone()
    if not d:
        return
    if act == "ok":
        if (await q("UPDATE deals SET status='paid' WHERE id=? AND status='wait_pay'", did)).rowcount:
            await say(d["buyer"], f"پرداخت معامله #{did} تأیید شد؛ منتظر تحویل آیدی از فروشنده باشید.")
            await say(d["seller"], f"خریدار پرداخت کرد ✅ آیدی @{d['uname']} را به خریدار تحویل دهید و سپس در برنامک «تحویل دادم» را بزنید.")
    elif act == "no":
        if (await q("UPDATE deals SET status='cancel' WHERE id=? AND status='wait_pay'", did)).rowcount:
            await q("UPDATE listings SET status='active' WHERE id=?", d["lid"])
            await say(d["buyer"], f"رسید معامله #{did} تأیید نشد.")
    elif act == "pay":
        if (await q("UPDATE deals SET status='done' WHERE id=? AND status IN('due','dispute')", did)).rowcount:
            await q("UPDATE listings SET status='sold' WHERE id=?", d["lid"])
            await q("UPDATE users SET done=done+1 WHERE id=?", d["seller"])
            await say(d["seller"], f"✅ مبلغ معامله #{did} واریز شد.")
            await say(d["buyer"], f"معامله #{did} با موفقیت بسته شد.")
    elif act == "ref":
        if (await q("UPDATE deals SET status='cancel' WHERE id=? AND status='dispute'", did)).rowcount:
            await q("UPDATE listings SET status='cancel' WHERE id=?", d["lid"])
            await say(d["buyer"], f"وجه معامله #{did} به شما بازگردانده می‌شود.")


async def poll():
    off = 0
    while True:
        try:
            for up in await call(SES, "getUpdates", offset=off, timeout=25, allowed_updates=["message", "callback_query"]):
                off = up["update_id"] + 1
                try:
                    if "message" in up:
                        await on_message(up["message"])
                    elif "callback_query" in up:
                        await on_callback(up["callback_query"])
                except Exception:
                    log.exception("handler")
        except Exception as e:
            log.warning("poll: %s", e)
            await asyncio.sleep(3)


async def ticker():  # قفل زمانی تمام شد → آزادسازی خودکار
    while True:
        await asyncio.sleep(60)
        try:
            for d in (await q("SELECT id FROM deals WHERE status='handed' AND lock<=?", time.time())).fetchall():
                await set_due(d["id"])
        except Exception:
            log.exception("ticker")


# ---------- web ----------
async def page(req):
    return web.FileResponse(Path(__file__).with_name("index.html"),
                            headers={"Content-Security-Policy": f"frame-ancestors {FRAME}", "Cache-Control": "no-store"})


async def health(req):
    return web.Response(text="ok")


async def lifespan(app):
    global SES, BOT_USER
    SES = aiohttp.ClientSession()
    for s in SCHEMA:
        await q(s)
    BOT_USER = (await call(SES, "getMe")).get("username", "")
    try:
        await call(SES, "deleteWebhook")
    except Exception:
        pass
    tasks = [asyncio.create_task(poll()), asyncio.create_task(ticker())]
    yield
    for t in tasks:
        t.cancel()
    await SES.close()


def main():
    logging.basicConfig(level=logging.INFO)
    app = web.Application()
    app.cleanup_ctx.append(lifespan)
    app.add_routes([
        web.get("/", health),
        web.get("/app", page),
        web.get("/api/listings", listings),
        web.post("/api/verify/start", verify_start),
        web.get("/api/verify/status", verify_status),
        web.post("/api/deals", deals_new),
        web.get("/api/deals", deals_list),
        web.get("/api/deals/{id}", deal_get),
        web.post("/api/deals/{id}/act", deal_act),
    ])
    web.run_app(app, port=int(os.environ.get("PORT", "8080")))


if __name__ == "__main__":
    main()
