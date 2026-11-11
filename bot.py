import asyncio
import base64
import html
import json
import logging
import os
import secrets
import sqlite3
import time
import urllib.parse
import uuid as uuidlib
from datetime import datetime

import asyncssh
from aiohttp import web
from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandObject
from aiogram.types import CallbackQuery, InlineKeyboardButton as Btn, InlineKeyboardMarkup as Kb, Message
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("vpnbot")

BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_ID = int(os.environ["ADMIN_ID"])
BASE_URL = os.environ["BASE_URL"].rstrip("/")  # https://sub.example.com
PAY_CONTACT = os.getenv("PAY_CONTACT", "@mannapov_ulugbek")
PORT = int(os.getenv("PORT", "8080"))
DAYS = 30
PAGE = 8  # клиентов на странице в админке
PAID_COOLDOWN = 60  # секунд между заявками «Я оплатил» от одного клиента

# цена -> сколько первых серверов из servers.json получает клиент
PLANS = {100: 3, 200: 6}

SERVERS_FILE = os.getenv("SERVERS_FILE", "servers.json")
DB_PATH = os.getenv("DB_PATH", "vpn.db")  # в Docker: /data/vpn.db (папка = volume)



def load_servers():
    """Список серверов: из переменной SERVERS_JSON (JSON или base64) либо из файла."""
    raw = os.getenv("SERVERS_JSON", "").strip()
    if raw:
        if not raw.startswith("["):
            raw = base64.b64decode(raw).decode("utf-8")
        return json.loads(raw)
    cands = [SERVERS_FILE, "/data/servers.json"]
    found = []
    for d in {os.getcwd(), os.path.dirname(os.path.abspath(__file__))}:
        try:
            for fn in os.listdir(d):  # имя файла без учёта регистра: Servers.json тоже подойдёт
                if fn.lower() in ("servers.json", "servers.example.json"):
                    found.append(os.path.join(d, fn))
        except OSError:
            pass
    cands += sorted(found, key=lambda p: ("example" in p.lower(), p))
    for p in cands:
        if os.path.isfile(p):
            if "example" in os.path.basename(p).lower():
                log.warning("Использую %s: лучше переименовать в servers.json", p)
            with open(p, encoding="utf-8") as f:
                return json.load(f)
    raise SystemExit(
        f"Не найден список серверов ({SERVERS_FILE}). Задайте переменную SERVERS_JSON "
        "(base64 от servers.json: base64 -w0 servers.json) или примонтируйте файл servers.json."
    )


SERVERS = load_servers()
_key_cache = {}


def ssh_key_path(s):
    """Путь к приватному ключу: файл из servers.json либо ключ из переменной SSH_KEY_B64 / SSH_KEY."""
    path = s.get("ssh_key", "")
    if path and os.path.isfile(path):
        return path
    if "env" not in _key_cache:
        data = os.getenv("SSH_KEY", "")
        if os.getenv("SSH_KEY_B64"):
            data = base64.b64decode(os.environ["SSH_KEY_B64"]).decode("utf-8")
        if not data.strip():
            raise FileNotFoundError(
                f"SSH-ключ не найден ({path or 'ssh_key не задан'}). Задайте SSH_KEY_B64 "
                "(base64 -w0 ssh/vpnbot) или примонтируйте файл ключа."
            )
        fd = os.open("/tmp/vpnbot_ssh_key", os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as f:
            f.write(data.replace("\\n", "\n").strip() + "\n")
        _key_cache["env"] = "/tmp/vpnbot_ssh_key"
    return _key_cache["env"]


db = sqlite3.connect(DB_PATH, check_same_thread=False)
db.row_factory = sqlite3.Row
db.execute(
    """CREATE TABLE IF NOT EXISTS users(
        tg_id INTEGER PRIMARY KEY,
        username TEXT,
        uuid TEXT NOT NULL,
        token TEXT NOT NULL,
        plan INTEGER NOT NULL DEFAULT 0,
        expire INTEGER NOT NULL DEFAULT 0,
        warned INTEGER NOT NULL DEFAULT 0
    )"""
)
db.execute(
    """CREATE TABLE IF NOT EXISTS payments(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tg_id INTEGER NOT NULL,
        plan INTEGER NOT NULL,
        amount INTEGER NOT NULL,
        ts INTEGER NOT NULL
    )"""
)
db.commit()

ADMIN_FILTER = F.from_user.id == ADMIN_ID

admin = Router()  # всё, что доступно только админу
admin.message.filter(ADMIN_FILTER)
admin.callback_query.filter(ADMIN_FILTER)
router = Router()  # обычные клиенты

_sync_lock = None
_done = set()  # обработанные заявки (чтобы двойной тап не продлил дважды)
_recent = {}  # антидребезг кнопок админки
_last_paid = {}  # антиспам заявок на оплату


def sync_lock():
    # создаём лениво, внутри работающего event loop (важно для Python < 3.10)
    global _sync_lock
    if _sync_lock is None:
        _sync_lock = asyncio.Lock()
    return _sync_lock


def debounce(key, sec=3):
    now = time.time()
    if now - _recent.get(key, 0) < sec:
        return True
    _recent[key] = now
    return False


# ---------- БД ----------
def get_user(tg_id):
    return db.execute("SELECT * FROM users WHERE tg_id=?", (tg_id,)).fetchone()


def get_user_by_ref(ref):
    ref = ref.strip()
    if ref.startswith("@"):
        return db.execute(
            "SELECT * FROM users WHERE lower(username)=lower(?)", (ref[1:],)
        ).fetchone()
    if ref.lstrip("-").isdigit():
        return get_user(int(ref))
    return None


def ensure_user(tg_id, username=None):
    u = get_user(tg_id)
    if u is None:
        db.execute(
            "INSERT INTO users(tg_id, username, uuid, token) VALUES(?,?,?,?)",
            (tg_id, username, str(uuidlib.uuid4()), secrets.token_urlsafe(12)),
        )
        db.commit()
    elif username and u["username"] != username:
        db.execute("UPDATE users SET username=? WHERE tg_id=?", (username, tg_id))
        db.commit()
    return get_user(tg_id)


def is_active(u):
    return u["plan"] in PLANS and u["expire"] > time.time()


def sub_url(u):
    return f"{BASE_URL}/sub/{u['token']}"


def fmt_date(ts):
    return datetime.fromtimestamp(ts).strftime("%d.%m.%Y")


async def notify(bot, tg_id, text):
    try:
        await bot.send_message(tg_id, text)
        return True
    except Exception as e:  # noqa: BLE001
        log.warning("cannot notify %s: %r", tg_id, e)
        return False


async def safe_edit(c: CallbackQuery, text, kb=None):
    try:
        await c.message.edit_text(text, reply_markup=kb)
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            raise


# ---------- синхронизация клиентов на серверах Xray ----------
def desired_clients(server_index, server):
    now = int(time.time())
    rows = db.execute("SELECT * FROM users WHERE plan>0 AND expire>?", (now,)).fetchall()
    flow = server.get("flow", "xtls-rprx-vision")
    out = []
    for r in rows:
        if server_index < PLANS.get(r["plan"], 0):
            c = {"id": r["uuid"], "email": f"tg_{r['tg_id']}"}
            if flow:
                c["flow"] = flow
            out.append(c)
    return out


async def sync_server(i, s):
    path = s.get("config_path", "/usr/local/etc/xray/config.json")
    xray = s.get("xray_bin", "xray")
    async with asyncssh.connect(
        s["ssh_host"],
        port=s.get("ssh_port", 22),
        username=s.get("ssh_user", "root"),
        client_keys=[ssh_key_path(s)],
        known_hosts=None,
    ) as conn:
        res = await conn.run(f"cat {path}", check=True)
        cfg = json.loads(res.stdout)
        inbounds = [x for x in cfg["inbounds"] if x.get("protocol") == "vless"]
        if s.get("inbound_tag"):
            inbounds = [x for x in inbounds if x.get("tag") == s["inbound_tag"]]
        ib = inbounds[0]
        old = ib["settings"]["clients"]
        # не трогаем клиентов, добавленных вручную (без email tg_...)
        keep = [c for c in old if not str(c.get("email", "")).startswith("tg_")]
        new = keep + desired_clients(i, s)
        if new == old:
            return
        ib["settings"]["clients"] = new
        await conn.run(f"cat > {path}.new", input=json.dumps(cfg, indent=2), check=True)
        await conn.run(f"{xray} run -test -config {path}.new", check=True)
        await conn.run(f"cp {path} {path}.bak", check=True)
        # cp, а не mv: сохраняются владелец и права исходного конфига
        await conn.run(f"cp {path}.new {path} && rm {path}.new", check=True)
        r = await conn.run("systemctl restart xray")
        if r.exit_status != 0:
            await conn.run(f"cp {path}.bak {path} && systemctl restart xray")
            raise RuntimeError("xray не перезапустился, конфиг откатан")
        log.info("synced %s", s["name"])


async def sync_all():
    """Возвращает список имён серверов, где синхронизация не удалась."""
    async with sync_lock():
        results = await asyncio.gather(
            *(sync_server(i, s) for i, s in enumerate(SERVERS)), return_exceptions=True
        )
    failed = []
    for s, r in zip(SERVERS, results):
        if isinstance(r, Exception):
            log.error("sync failed for %s: %r", s["name"], r)
            failed.append(s["name"])
    return failed


# ---------- подписка (то, что открывает Happ/v2rayN) ----------
async def sub_handler(request):
    u = db.execute("SELECT * FROM users WHERE token=?", (request.match_info["token"],)).fetchone()
    if not u:
        raise web.HTTPNotFound()
    links = []
    if is_active(u):
        for s in SERVERS[: PLANS[u["plan"]]]:
            links.append(
                s["link"].replace("{uuid}", u["uuid"]) + "#" + urllib.parse.quote(s["name"])
            )
    body = base64.b64encode("\n".join(links).encode()).decode()
    title = base64.b64encode("Мой VPN".encode()).decode()
    return web.Response(
        text=body,
        headers={
            "profile-title": f"base64:{title}",
            "profile-update-interval": "6",
            "subscription-userinfo": f"upload=0; download=0; total=0; expire={u['expire']}",
        },
    )


def make_app():
    app = web.Application()
    app.router.add_get("/sub/{token}", sub_handler)
    return app


# ---------- операции над клиентом ----------
async def grant(bot: Bot, tg_id: int, plan: int, days: int = DAYS, record: bool = False):
    """Выдаёт/продлевает доступ. record=True — записать оплату в статистику.
    Возвращает (failed_servers, user, notified)."""
    u = ensure_user(tg_id)
    expire = max(u["expire"], int(time.time())) + days * 86400
    db.execute("UPDATE users SET plan=?, expire=?, warned=0 WHERE tg_id=?", (plan, expire, tg_id))
    if record:
        db.execute(
            "INSERT INTO payments(tg_id, plan, amount, ts) VALUES(?,?,?,?)",
            (tg_id, plan, plan, int(time.time())),
        )
    db.commit()
    failed = await sync_all()
    u = get_user(tg_id)
    notified = await notify(
        bot,
        tg_id,
        f"✅ Доступ активирован!\nТариф: {plan} ₽ ({PLANS[plan]} серверов)\n"
        f"Действует до <b>{fmt_date(u['expire'])}</b>\n\n"
        f"Ваша ссылка-подписка:\n<code>{sub_url(u)}</code>\n\n"
        f"Добавьте её в Happ (+ → вставить из буфера). Серверы обновятся сами.",
    )
    return failed, u, notified


async def revoke_user(bot: Bot, tg_id: int):
    db.execute("UPDATE users SET plan=0, expire=0 WHERE tg_id=?", (tg_id,))
    db.commit()
    failed = await sync_all()
    await notify(bot, tg_id, f"Ваш доступ отключён. Вопросы: {PAY_CONTACT}")
    return failed


async def reset_keys(bot: Bot, tg_id: int):
    db.execute(
        "UPDATE users SET uuid=?, token=? WHERE tg_id=?",
        (str(uuidlib.uuid4()), secrets.token_urlsafe(12), tg_id),
    )
    db.commit()
    failed = await sync_all()
    u = get_user(tg_id)
    notified = True
    if is_active(u):
        notified = await notify(
            bot,
            tg_id,
            f"🔑 Ваша ссылка обновлена. Новая подписка:\n<code>{sub_url(u)}</code>\n\n"
            f"Удалите старую подписку в Happ и добавьте эту.",
        )
    return failed, notified


def toast_for(failed, notified=True):
    parts = []
    if not notified:
        parts.append("клиент не получил сообщение — отправьте ему ссылку сами")
    if failed:
        parts.append("серверы без синхронизации: " + ", ".join(failed))
    return ("⚠️ " + "; ".join(parts))[:190] if parts else None


# ---------- клавиатуры и тексты клиента ----------
def main_kb():
    return Kb(
        inline_keyboard=[
            [Btn(text="💳 Купить / продлить", callback_data="buy")],
            [Btn(text="🔗 Моя подписка", callback_data="mysub")],
            [Btn(text="📖 Как подключиться", callback_data="help")],
        ]
    )


def plans_kb():
    rows = [
        [Btn(text=f"{p} ₽ / мес — {n} серверов", callback_data=f"plan:{p}")]
        for p, n in PLANS.items()
    ]
    rows.append([Btn(text="⬅️ Назад", callback_data="home")])
    return Kb(inline_keyboard=rows)


HOME_TEXT = "Привет! Здесь можно купить доступ к VPN и получить ссылку-подписку."
HELP_TEXT = (
    "📖 <b>Как подключиться</b>\n\n"
    "1. Установите приложение <b>Happ</b> (App Store / Google Play).\n"
    "2. В боте откройте «🔗 Моя подписка» и скопируйте ссылку (нажмите на неё).\n"
    "3. В Happ нажмите «+» → «Вставить из буфера».\n"
    "4. Выберите сервер и включите VPN. Список серверов обновляется сам."
)


# ---------- хендлеры клиента ----------
@router.message(Command("start"))
async def start(m: Message):
    ensure_user(m.from_user.id, m.from_user.username)
    await m.answer(HOME_TEXT, reply_markup=main_kb())


@router.callback_query(F.data == "home")
async def home(c: CallbackQuery):
    await safe_edit(c, HOME_TEXT, main_kb())
    await c.answer()


@router.callback_query(F.data == "help")
async def help_(c: CallbackQuery):
    await safe_edit(c, HELP_TEXT, Kb(inline_keyboard=[[Btn(text="⬅️ Назад", callback_data="home")]]))
    await c.answer()


@router.callback_query(F.data == "buy")
async def buy(c: CallbackQuery):
    tariffs = "\n".join(f"• {p} ₽ — {n} серверов на {DAYS} дней" for p, n in PLANS.items())
    await safe_edit(c, f"Выберите тариф:\n\n{tariffs}", plans_kb())
    await c.answer()


@router.callback_query(F.data.startswith("plan:"))
async def choose_plan(c: CallbackQuery):
    plan = int(c.data.split(":")[1])
    if plan not in PLANS:
        return await c.answer()
    kb = Kb(
        inline_keyboard=[
            [Btn(text="✅ Я оплатил", callback_data=f"paid:{plan}")],
            [Btn(text="⬅️ Назад", callback_data="buy")],
        ]
    )
    await safe_edit(
        c,
        f"Тариф: <b>{plan} ₽</b> ({PLANS[plan]} серверов, {DAYS} дней)\n\n"
        f"1. Напишите в личку {PAY_CONTACT} и оплатите.\n"
        f"2. В сообщении укажите ваш ID: <code>{c.from_user.id}</code>\n"
        f"3. Нажмите «Я оплатил» — админ получит заявку и подтвердит.",
        kb,
    )
    await c.answer()


@router.callback_query(F.data.startswith("paid:"))
async def paid(c: CallbackQuery):
    plan = int(c.data.split(":")[1])
    if plan not in PLANS:
        return await c.answer()
    u = c.from_user
    if time.time() - _last_paid.get(u.id, 0) < PAID_COOLDOWN:
        return await c.answer("Заявка уже отправлена, подождите подтверждения.", show_alert=True)
    _last_paid[u.id] = time.time()
    ensure_user(u.id, u.username)
    kb = Kb(
        inline_keyboard=[
            [
                Btn(text=f"✅ Подтвердить {plan} ₽", callback_data=f"ok:{u.id}:{plan}"),
                Btn(text="❌ Отклонить", callback_data=f"no:{u.id}"),
            ],
            [Btn(text="👤 Карточка клиента", callback_data=f"adm:u:{u.id}")],
        ]
    )
    name = html.escape(u.full_name)
    uname = f"@{u.username}" if u.username else "без username"
    await c.bot.send_message(
        ADMIN_ID,
        f"💰 Заявка на оплату\nКлиент: {name} ({uname})\nID: <code>{u.id}</code>\nТариф: {plan} ₽",
        reply_markup=kb,
    )
    await safe_edit(c, "Заявка отправлена. Как только оплата будет подтверждена, я пришлю ссылку.")
    await c.answer()


@router.callback_query(F.data == "mysub")
async def mysub(c: CallbackQuery):
    u = ensure_user(c.from_user.id, c.from_user.username)
    if is_active(u):
        text = (
            f"✅ Подписка активна до <b>{fmt_date(u['expire'])}</b>\n"
            f"Тариф: {u['plan']} ₽ ({PLANS[u['plan']]} серверов)\n\n"
            f"Ваша ссылка (добавьте её в Happ как подписку):\n<code>{sub_url(u)}</code>"
        )
    else:
        text = "У вас нет активной подписки. Нажмите «Купить / продлить»."
    await safe_edit(c, text, main_kb())
    await c.answer()


# ---------- админ: заявки на оплату ----------
@admin.callback_query(F.data.startswith("ok:"))
async def admin_ok(c: CallbackQuery):
    key = (c.message.message_id, c.data)
    if key in _done:
        return await c.answer("Уже обработано")
    _done.add(key)
    try:
        _, tg_id, plan = c.data.split(":")
        failed, u, notified = await grant(c.bot, int(tg_id), int(plan), record=True)
    except Exception:
        _done.discard(key)
        raise
    note = ""
    if failed:
        note += f"\n⚠️ Не синхронизировались: {', '.join(failed)}"
    if not notified:
        note += "\n⚠️ Клиент не получил сообщение"
    await safe_edit(c, f"{c.message.html_text}\n\n✅ Подтверждено{note}")
    await c.answer()


@admin.callback_query(F.data.startswith("no:"))
async def admin_no(c: CallbackQuery):
    key = (c.message.message_id, c.data)
    if key in _done:
        return await c.answer("Уже обработано")
    _done.add(key)
    tg_id = int(c.data.split(":")[1])
    await notify(c.bot, tg_id, f"❌ Оплата не подтверждена. Напишите {PAY_CONTACT}.")
    await safe_edit(c, f"{c.message.html_text}\n\n❌ Отклонено")
    await c.answer()


# ---------- админ-панель ----------
def admin_menu_kb():
    return Kb(
        inline_keyboard=[
            [
                Btn(text="📊 Статистика", callback_data="adm:stats"),
                Btn(text="👥 Клиенты", callback_data="adm:l:0"),
            ],
            [Btn(text="🔄 Синхронизировать серверы", callback_data="adm:sync")],
        ]
    )


def back_menu_kb():
    return Kb(inline_keyboard=[[Btn(text="⬅️ Меню", callback_data="adm:menu")]])


def stats_text():
    now = int(time.time())
    one = lambda q, *a: db.execute(q, a).fetchone()[0]  # noqa: E731
    total = one("SELECT COUNT(*) FROM users")
    active = one("SELECT COUNT(*) FROM users WHERE plan>0 AND expire>?", now)
    by_plan = ", ".join(
        f"{p} ₽: {one('SELECT COUNT(*) FROM users WHERE plan=? AND expire>?', p, now)}" for p in PLANS
    )
    soon = one(
        "SELECT COUNT(*) FROM users WHERE plan>0 AND expire>? AND expire<?", now, now + 3 * 86400
    )
    rev30 = one("SELECT COALESCE(SUM(amount),0) FROM payments WHERE ts>?", now - 30 * 86400)
    rev_all = one("SELECT COALESCE(SUM(amount),0) FROM payments")
    return (
        "📊 <b>Статистика</b>\n\n"
        f"Всего клиентов: {total}\n"
        f"Активных: {active} ({by_plan})\n"
        f"Истекают за 3 дня: {soon}\n\n"
        f"Выручка за 30 дней: {rev30} ₽\n"
        f"Выручка всего: {rev_all} ₽"
    )


def users_page(page):
    now = int(time.time())
    total = db.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    pages = max(1, -(-total // PAGE))
    page = min(max(page, 0), pages - 1)
    rows = db.execute(
        "SELECT * FROM users ORDER BY CASE WHEN plan>0 AND expire>? THEN 0 ELSE 1 END, "
        "expire ASC, tg_id LIMIT ? OFFSET ?",
        (now, PAGE, page * PAGE),
    ).fetchall()
    kb = []
    for r in rows:
        mark = "🟢" if is_active(r) else "🔴"
        label = f"@{r['username']}" if r["username"] else str(r["tg_id"])
        plan = f"{r['plan']}₽" if r["plan"] else "—"
        until = fmt_date(r["expire"]) if r["expire"] else "—"
        kb.append([Btn(text=f"{mark} {label} · {plan} · {until}", callback_data=f"adm:u:{r['tg_id']}")])
    nav = []
    if page > 0:
        nav.append(Btn(text="◀️", callback_data=f"adm:l:{page - 1}"))
    nav.append(Btn(text=f"{page + 1}/{pages}", callback_data="adm:noop"))
    if page < pages - 1:
        nav.append(Btn(text="▶️", callback_data=f"adm:l:{page + 1}"))
    kb.append(nav)
    kb.append([Btn(text="⬅️ Меню", callback_data="adm:menu")])
    return f"👥 <b>Клиенты</b>: {total}", Kb(inline_keyboard=kb)


def user_card(tg_id):
    u = get_user(tg_id)
    back = Kb(inline_keyboard=[[Btn(text="⬅️ К списку", callback_data="adm:l:0")]])
    if not u:
        return "Клиент не найден.", back
    un = f"@{u['username']}" if u["username"] else "—"
    if is_active(u):
        left = max(0, (u["expire"] - int(time.time())) // 86400)
        status = f"🟢 активен до {fmt_date(u['expire'])} (осталось ~{left} дн.)"
    else:
        status = "🔴 не активен"
    pays = db.execute(
        "SELECT COUNT(*) n, COALESCE(SUM(amount),0) s FROM payments WHERE tg_id=?", (tg_id,)
    ).fetchone()
    plan = f"{u['plan']} ₽" if u["plan"] else "—"
    text = (
        f"👤 <code>{tg_id}</code> {un}\n{status}\nТариф: {plan}\n"
        f"Оплат: {pays['n']} на {pays['s']} ₽\n\nСсылка: <code>{sub_url(u)}</code>"
    )
    kb = [
        [Btn(text=f"➕ 30 дн · {p} ₽", callback_data=f"adm:g:{tg_id}:{p}") for p in PLANS],
        [
            Btn(text="🎁 +7 дней", callback_data=f"adm:d:{tg_id}:7"),
            Btn(text="⛔ Отключить", callback_data=f"adm:x:{tg_id}"),
        ],
        [Btn(text="🔑 Сбросить ссылку", callback_data=f"adm:r:{tg_id}")],
        [Btn(text="⬅️ К списку", callback_data="adm:l:0")],
    ]
    return text, Kb(inline_keyboard=kb)


@admin.message(Command("admin"))
async def cmd_admin(m: Message):
    await m.answer("🛠 <b>Админ-панель</b>", reply_markup=admin_menu_kb())


@admin.callback_query(F.data.startswith("adm:"))
async def adm(c: CallbackQuery):
    p = c.data.split(":")
    act = p[1]
    toast = None
    if act == "noop":
        pass
    elif act == "menu":
        await safe_edit(c, "🛠 <b>Админ-панель</b>", admin_menu_kb())
    elif act == "stats":
        await safe_edit(c, stats_text(), back_menu_kb())
    elif act == "l":
        text, kb = users_page(int(p[2]))
        await safe_edit(c, text, kb)
    elif act == "u":
        text, kb = user_card(int(p[2]))
        await safe_edit(c, text, kb)
    elif act in ("g", "d", "x", "xy", "r"):
        tg_id = int(p[2])
        if act == "x":  # подтверждение перед отключением
            kb = Kb(
                inline_keyboard=[
                    [
                        Btn(text="⛔ Да, отключить", callback_data=f"adm:xy:{tg_id}"),
                        Btn(text="Отмена", callback_data=f"adm:u:{tg_id}"),
                    ]
                ]
            )
            await safe_edit(c, f"Отключить клиента <code>{tg_id}</code>? Срок обнулится.", kb)
        elif debounce(c.data):
            return await c.answer("Уже выполнено")
        elif act == "g":
            plan = int(p[3])
            if plan not in PLANS:
                return await c.answer()
            failed, _, notified = await grant(c.bot, tg_id, plan, record=True)
            toast = toast_for(failed, notified)
            await safe_edit(c, *user_card(tg_id))
        elif act == "d":
            u = ensure_user(tg_id)
            plan = u["plan"] if u["plan"] in PLANS else min(PLANS)
            failed, _, notified = await grant(c.bot, tg_id, plan, days=int(p[3]))
            toast = toast_for(failed, notified)
            await safe_edit(c, *user_card(tg_id))
        elif act == "xy":
            toast = toast_for(await revoke_user(c.bot, tg_id))
            await safe_edit(c, *user_card(tg_id))
        elif act == "r":
            failed, notified = await reset_keys(c.bot, tg_id)
            toast = toast_for(failed, notified)
            await safe_edit(c, *user_card(tg_id))
    elif act == "sync":
        if debounce("sync", 5):
            return await c.answer("Уже выполняется")
        failed = await sync_all()
        toast = toast_for(failed) or f"✅ Все серверы ({len(SERVERS)}) синхронизированы"
    await c.answer(toast, show_alert=bool(toast and toast.startswith("⚠️")))


@admin.message(Command("user"))
async def cmd_user(m: Message, command: CommandObject):
    """/user <id или @username> — открыть карточку клиента"""
    u = get_user_by_ref(command.args or "")
    if not u:
        return await m.answer("Формат: /user <id или @username> (клиент должен был запустить бота)")
    text, kb = user_card(u["tg_id"])
    await m.answer(text, reply_markup=kb)


@admin.message(Command("grant"))
async def cmd_grant(m: Message, command: CommandObject):
    """/grant <tg_id> <100|200> [дней] — выдать доступ БЕЗ записи оплаты (подарок/тест)"""
    try:
        a = (command.args or "").split()
        tg_id, plan = int(a[0]), int(a[1])
        days = int(a[2]) if len(a) > 2 else DAYS
        assert plan in PLANS
    except Exception:
        return await m.answer("Формат: /grant <tg_id> <100|200> [дней]")
    failed, u, notified = await grant(m.bot, tg_id, plan, days)
    text = f"✅ Выдано до {fmt_date(u['expire'])}\nСсылка: <code>{sub_url(u)}</code>"
    if not notified:
        text += "\n⚠️ Клиент не получил сообщение (не запускал бота?). Перешлите ссылку сами."
    if failed:
        text += f"\n⚠️ Не синхронизировались: {', '.join(failed)}"
    await m.answer(text)


@admin.message(Command("revoke"))
async def cmd_revoke(m: Message, command: CommandObject):
    """/revoke <tg_id> — отключить клиента"""
    try:
        tg_id = int((command.args or "").strip())
    except Exception:
        return await m.answer("Формат: /revoke <tg_id>")
    failed = await revoke_user(m.bot, tg_id)
    await m.answer("Отключён." + (f"\n⚠️ Не синхронизировались: {', '.join(failed)}" if failed else ""))


@admin.message(Command("broadcast"))
async def cmd_broadcast(m: Message, command: CommandObject):
    """/broadcast <текст> — написать всем клиентам с активной подпиской"""
    if not command.args:
        return await m.answer("Формат: /broadcast текст (получат клиенты с активной подпиской)")
    now = int(time.time())
    ids = [r["tg_id"] for r in db.execute("SELECT tg_id FROM users WHERE plan>0 AND expire>?", (now,))]
    sent = 0
    for tg_id in ids:
        if await notify(m.bot, tg_id, html.escape(command.args)):
            sent += 1
        await asyncio.sleep(0.05)
    await m.answer(f"Отправлено: {sent}/{len(ids)}")


# ---------- фоновая проверка сроков ----------
async def watcher(bot: Bot):
    last_failed = None
    while True:
        try:
            now = int(time.time())
            soon = db.execute(
                "SELECT * FROM users WHERE plan>0 AND expire>? AND expire<? AND warned=0",
                (now, now + 3 * 86400),
            ).fetchall()
            for u in soon:
                await notify(
                    bot,
                    u["tg_id"],
                    f"⏰ Подписка заканчивается {fmt_date(u['expire'])}. Продлить: /start → «Купить / продлить».",
                )
                db.execute("UPDATE users SET warned=1 WHERE tg_id=?", (u["tg_id"],))
            db.commit()

            expired = db.execute("SELECT * FROM users WHERE plan>0 AND expire<=?", (now,)).fetchall()
            if expired:
                db.execute("UPDATE users SET plan=0 WHERE plan>0 AND expire<=?", (now,))
                db.commit()

            # синхронизация каждый час: сама чинит серверы, которые были недоступны
            failed = set(await sync_all())
            if failed and failed != last_failed:
                await notify(bot, ADMIN_ID, "⚠️ Не синхронизированы серверы: " + ", ".join(sorted(failed)))
            last_failed = failed

            for u in expired:
                await notify(bot, u["tg_id"], "Подписка закончилась. Чтобы продолжить, оплатите: /start")
        except Exception:
            log.exception("watcher error")
        await asyncio.sleep(3600)


async def main():
    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    dp.include_router(admin)
    dp.include_router(router)

    runner = web.AppRunner(make_app())
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()

    tasks = [asyncio.create_task(watcher(bot))]  # ссылка нужна, чтобы задачу не собрал GC
    try:
        await dp.start_polling(bot)
    finally:
        for t in tasks:
            t.cancel()
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
