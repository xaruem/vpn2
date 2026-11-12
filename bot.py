import asyncio
import base64
import html
import json
import logging
import os
import re
import secrets
import shlex
import socket
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
        b64 = os.getenv("SSH_KEY_B64", "").strip().strip("\"'")
        if b64:
            if b64.startswith("-----BEGIN"):  # вставили сам ключ, а не base64 от него
                data = b64
            else:
                b64 = "".join(b64.split())  # убираем переносы и пробелы
                n = len(b64)
                b64 += "=" * (-n % 4)
                try:
                    data = base64.b64decode(b64).decode("utf-8")
                except Exception:
                    raise ValueError(
                        f"SSH_KEY_B64 повреждена: в ней {n} символов, а ключ в base64 это "
                        "обычно 400-700 символов, начинающихся с LS0tLS1CRUdJTi. Закодируйте файл "
                        "приватного ключа (vpnbot, без .pub) и вставьте значение целиком."
                    ) from None
        data = data.replace("\\n", "\n").strip()
        if not data:
            raise FileNotFoundError(
                f"SSH-ключ не найден ({path or 'ssh_key не задан'}). Задайте SSH_KEY_B64 "
                "(base64 -w0 ssh/vpnbot) или примонтируйте файл ключа."
            )
        try:
            asyncssh.import_private_key(data)
        except Exception as e:  # noqa: BLE001
            hint = " Это публичный ключ (.pub): нужен приватный файл без расширения." if data.startswith("ssh-") else ""
            raise ValueError(f"SSH_KEY_B64 не содержит рабочего приватного ключа ({e}).{hint}") from None
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


async def safe_answer(c: CallbackQuery, *a, **kw):
    """Ответ на нажатие кнопки; если он запоздал (долгая синхронизация) — не падаем."""
    try:
        await c.answer(*a, **kw)
    except TelegramBadRequest as e:
        log.warning("callback answer: %s", e)


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


# Где обычно лежит конфиг Xray (если по пути из servers.json файла нет)
CONFIG_CANDIDATES = [
    "/usr/local/etc/xray/config.json",
    "/etc/xray/config.json",
    "/opt/xray/config.json",
    "/usr/local/x-ui/bin/config.json",
    "/etc/x-ui/config.json",
    "/usr/local/etc/v2ray/config.json",
    "/var/lib/marzban/xray_config.json",
]
SSH_CONNECT_TIMEOUT = 12  # секунд на TCP-соединение
SSH_LOGIN_TIMEOUT = 20  # секунд на вход по ключу
SYNC_TIMEOUT = 60  # секунд на один сервер целиком
LAST_ERRORS = {}  # имя сервера -> причина последнего сбоя (для сообщений админу)


def strip_json_comments(text):
    """Xray допускает // и /* */ в конфиге, обычный json их не принимает; строки не трогаем."""
    out, i, n, in_str = [], 0, len(text), False
    while i < n:
        c = text[i]
        if in_str:
            out.append(c)
            if c == "\\" and i + 1 < n:
                out.append(text[i + 1])
                i += 1
            elif c == '"':
                in_str = False
        elif c == '"':
            in_str = True
            out.append(c)
        elif c == "/" and text[i + 1 : i + 2] == "/":
            while i < n and text[i] != "\n":
                i += 1
            continue
        elif c == "/" and text[i + 1 : i + 2] == "*":
            end = text.find("*/", i + 2)
            i = n if end == -1 else end + 2
            continue
        else:
            out.append(c)
        i += 1
    return "".join(out)


def parse_config(text):
    try:
        return json.loads(text)
    except ValueError:
        return json.loads(strip_json_comments(text))


def link_port(s):
    """Порт сервера из ссылки подключения (по нему выбираем нужный inbound)."""
    m = re.search(r"@(?:\[[^\]]+\]|[^:/?#@]+):(\d+)", s.get("link", ""))
    return m.group(1) if m else None


def vless_inbounds(cfg):
    return [
        x
        for x in cfg.get("inbounds", [])
        if isinstance(x, dict) and str(x.get("protocol", "")).lower() == "vless"
    ]


def describe_inbounds(vless):
    return "; ".join(f"порт {x.get('port')}, tag {x.get('tag')!r}" for x in vless) or "нет"


def pick_inbound(cfg, s):
    vless = vless_inbounds(cfg)
    if not vless:
        raise RuntimeError("в конфиге нет ни одного vless-inbound")
    tag = s.get("inbound_tag")
    if tag:
        sel = [x for x in vless if x.get("tag") == tag]
        if len(sel) != 1:
            raise RuntimeError(f"inbound_tag {tag!r} не найден. Есть: {describe_inbounds(vless)}")
        return sel[0]
    port = link_port(s)
    sel = [x for x in vless if port and str(x.get("port")) == port]
    if not sel and len(vless) == 1:
        sel = vless
    if len(sel) != 1:
        raise RuntimeError(
            f"не удалось выбрать inbound (порт из ссылки: {port}). Есть: {describe_inbounds(vless)}. "
            "Укажите inbound_tag для этого сервера в servers.json"
        )
    return sel[0]


def ssh_connect(s):
    return asyncssh.connect(
        s["ssh_host"],
        port=s.get("ssh_port", 22),
        username=s.get("ssh_user", "root"),
        client_keys=[ssh_key_path(s)],
        known_hosts=None,
        connect_timeout=SSH_CONNECT_TIMEOUT,
        login_timeout=SSH_LOGIN_TIMEOUT,
    )


async def sh(conn, cmd, input=None):
    """Команда на сервере; при ошибке исключение с кодом выхода и текстом stderr."""
    r = await conn.run(cmd, input=input, check=False)
    if r.exit_status != 0:
        err = (getattr(r, "stderr", "") or r.stdout or "").strip().splitlines()
        raise RuntimeError(f"«{cmd[:60]}» вернула код {r.exit_status}" + (f": {err[-1][:200]}" if err else ""))
    return r.stdout or ""


async def read_remote(conn, path):
    r = await conn.run(f"cat {shlex.quote(path)}", check=False)
    return (r.stdout or "") if r.exit_status == 0 else None


async def find_config(conn, s):
    """(путь, текст). Сначала config_path из servers.json, затем процесс xray, затем типовые места."""
    want = (s.get("config_path") or "").strip()
    tried = []
    if want and want != "auto":
        text = await read_remote(conn, want)
        if text is not None:
            return want, text
        tried.append(want)
    found = []
    r = await conn.run("ps -eo args", check=False)
    if r.exit_status == 0:
        for line in (r.stdout or "").splitlines():
            if "xray" in line.lower():
                found += re.findall(r"(?:^|\s)(?:-c|-config|--config)(?:=|\s+)(/\S+\.json)", line)
    for p in dict.fromkeys(found + CONFIG_CANDIDATES):
        if p in tried:
            continue
        tried.append(p)
        text = await read_remote(conn, p)
        if text is None:
            continue
        try:
            cfg = parse_config(text)
        except ValueError:
            continue
        if isinstance(cfg, dict) and vless_inbounds(cfg):
            if want and want != "auto":
                log.warning("%s: файла %s нет, использую найденный %s (укажите его в config_path)", s["name"], want, p)
            return p, text
    raise RuntimeError("конфиг Xray не найден. Проверенные пути: " + ", ".join(tried))


async def sync_server(i, s):
    q = shlex.quote
    xray = s.get("xray_bin", "xray")
    restart = s.get("restart_cmd") or "systemctl restart xray"
    # после перезапуска проверяем, что служба жива (для своей restart_cmd — только если задан check_cmd)
    check = s["check_cmd"] if "check_cmd" in s else ("" if s.get("restart_cmd") else "systemctl is-active --quiet xray")
    tmp = f"/tmp/vpnbot-xray-{i}.json"  # расширение .json обязательно: иначе xray не определит формат
    async with ssh_connect(s) as conn:
        path, text = await find_config(conn, s)
        try:
            cfg = parse_config(text)
        except ValueError as e:
            raise RuntimeError(f"{path} не читается как JSON: {e}") from None
        ib = pick_inbound(cfg, s)
        settings = ib.setdefault("settings", {})
        old = settings.get("clients") or []
        # не трогаем клиентов, добавленных вручную (без email tg_...)
        keep = [c for c in old if not str(c.get("email", "")).startswith("tg_")]
        new = keep + desired_clients(i, s)
        if new == old:
            return False
        settings["clients"] = new
        try:
            await sh(conn, f"cat > {tmp}", input=json.dumps(cfg, indent=2, ensure_ascii=False))
            await sh(conn, f"{xray} run -test -config {tmp}")
            await sh(conn, f"cp {q(path)} {q(path)}.bak")
            # cp, а не mv: сохраняются владелец и права исходного конфига
            await sh(conn, f"cp {tmp} {q(path)}")
        finally:
            await conn.run(f"rm -f {tmp}", check=False)
        try:
            await sh(conn, restart)
            if check:
                await sh(conn, "sleep 2")
                await sh(conn, check)
        except Exception as e:  # noqa: BLE001
            await conn.run(f"cp {q(path)}.bak {q(path)}", check=False)
            r = await conn.run(restart, check=False)
            back = "старый конфиг возвращён" if r.exit_status == 0 else "старый конфиг возвращён, но перезапуск не удался"
            raise RuntimeError(f"xray не запустился с новым конфигом ({e}); {back}") from None
        log.info("synced %s", s["name"])
        return True


def explain(e, s):
    """Понятная причина сбоя для админа."""
    host, port = s.get("ssh_host"), s.get("ssh_port", 22)
    if isinstance(e, asyncssh.PermissionDenied):
        return (
            "сервер не принял SSH-ключ (Permission denied). Добавьте публичный ключ в "
            f"~/.ssh/authorized_keys пользователя {s.get('ssh_user', 'root')}"
        )
    if isinstance(e, asyncssh.DisconnectError):
        return f"сервер разорвал SSH-соединение: {e.reason}"
    if isinstance(e, ConnectionRefusedError):
        return f"соединение отклонено: на {host}:{port} никто не слушает SSH"
    if isinstance(e, socket.gaierror):
        return f"не удаётся найти адрес {host}"
    if isinstance(e, (asyncio.TimeoutError, TimeoutError)):
        return f"нет ответа от {host}:{port} (таймаут). Порт закрыт файрволом или сервер недоступен"
    if isinstance(e, OSError) and not isinstance(e, FileNotFoundError):
        return f"сетевая ошибка при подключении к {host}:{port}: {e}"
    return str(e) or type(e).__name__


async def _sync_one(i, s):
    return await asyncio.wait_for(sync_server(i, s), SYNC_TIMEOUT)


async def sync_all():
    """Возвращает список имён серверов, где синхронизация не удалась (причины в LAST_ERRORS)."""
    async with sync_lock():
        results = await asyncio.gather(
            *(_sync_one(i, s) for i, s in enumerate(SERVERS)), return_exceptions=True
        )
    failed = []
    for s, r in zip(SERVERS, results):
        if isinstance(r, Exception):
            LAST_ERRORS[s["name"]] = explain(r, s)
            log.error("sync failed for %s: %s", s["name"], LAST_ERRORS[s["name"]])
            failed.append(s["name"])
        else:
            LAST_ERRORS.pop(s["name"], None)
    return failed


def failed_text(failed):
    rows = "\n".join(
        f"• {html.escape(n)}: {html.escape(LAST_ERRORS.get(n, 'неизвестная ошибка'))}" for n in failed
    )
    return f"⚠️ Не синхронизированы серверы:\n{rows}\n\nПодробная проверка: /diag"


# ---------- диагностика (только чтение, ничего на серверах не меняет) ----------
async def discover_hints(conn):
    """Если конфиг не нашли: что вообще запущено на сервере."""
    out = []
    r = await conn.run("ps -eo args | grep -i '[x]ray' | head -3", check=False)
    procs = [l.strip()[:110] for l in (r.stdout or "").splitlines() if l.strip()]
    out.append("   · процессы xray: " + (html.escape(" | ".join(procs)) if procs else "не найдены"))
    r = await conn.run("docker ps --format '{{.Names}} ({{.Image}})' 2>/dev/null | head -5", check=False)
    if (r.stdout or "").strip():
        out.append("   · docker: " + html.escape(" | ".join(r.stdout.split("\n")[:5])))
    r = await conn.run(
        "ls -d /usr/local/x-ui /etc/x-ui /opt/marzban /var/lib/marzban /opt/remnanode 2>/dev/null", check=False
    )
    if (r.stdout or "").strip():
        out.append("   · панель/каталоги: " + html.escape(" ".join(r.stdout.split())))
        out.append("   ⚠️ если Xray управляется панелью или docker, бот должен менять её конфиг, а не файл на диске")
    return out


async def diag_server(i, s):
    """(строки отчёта, всё ли в порядке) по одному серверу."""
    q = shlex.quote
    xray = s.get("xray_bin", "xray")
    lines = [f"<b>{html.escape(s['name'])}</b> · {html.escape(str(s['ssh_host']))}:{s.get('ssh_port', 22)}"]
    state = {"ok": True}

    def good(t):
        lines.append("✅ " + t)

    def bad(t):
        state["ok"] = False
        lines.append("❌ " + t)

    try:
        async with ssh_connect(s) as conn:
            who = ((await conn.run("id -un", check=False)).stdout or "").strip() or "?"
            good(f"SSH: вход выполнен (пользователь {html.escape(who)})")
            try:
                path, text = await find_config(conn, s)
                cfg = parse_config(text)
            except Exception as e:  # noqa: BLE001
                bad("конфиг: " + html.escape(explain(e, s)))
                lines += await discover_hints(conn)
                return lines, False
            good(f"конфиг: <code>{html.escape(path)}</code>")
            vless = vless_inbounds(cfg)
            for x in vless:
                cl = (x.get("settings") or {}).get("clients") or []
                ours = sum(1 for c in cl if str(c.get("email", "")).startswith("tg_"))
                sec = (x.get("streamSettings") or {}).get("security", "none")
                lines.append(
                    f"   · vless порт {x.get('port')}, tag {html.escape(str(x.get('tag')))}, {html.escape(str(sec))}, "
                    f"клиентов {len(cl)} (от бота {ours})"
                )
            try:
                ib = pick_inbound(cfg, s)
                good(f"inbound для клиентов: порт {ib.get('port')} (в ссылке {link_port(s)})")
            except Exception as e:  # noqa: BLE001
                bad(html.escape(str(e)))
            r = await conn.run(f"{xray} version", check=False)
            xray_ok = r.exit_status == 0
            if xray_ok:
                good("xray: " + html.escape(((r.stdout or "").strip().splitlines() or ["?"])[0][:60]))
            else:
                bad(f"команда «{html.escape(xray)}» не запускается. Укажите полный путь в xray_bin (servers.json)")
            r = await conn.run(f"test -w {q(path)} && test -w /tmp", check=False)
            if r.exit_status == 0:
                good("права на запись есть")
            else:
                bad(f"нет прав на запись в {html.escape(path)} или /tmp (пользователь {html.escape(who)})")
            if xray_ok:
                tmp = f"/tmp/vpnbot-diag-{i}.json"
                try:
                    await sh(conn, f"cat > {tmp}", input=json.dumps(cfg, indent=2, ensure_ascii=False))
                    await sh(conn, f"{xray} run -test -config {tmp}")
                    good("пробная проверка конфига (xray -test) проходит")
                except Exception as e:  # noqa: BLE001
                    bad("xray -test: " + html.escape(str(e)))
                finally:
                    await conn.run(f"rm -f {tmp}", check=False)
            if s.get("restart_cmd"):
                lines.append(f"   · перезапуск: <code>{html.escape(s['restart_cmd'])}</code> (не проверяется)")
            else:
                r = await conn.run("systemctl is-active xray", check=False)
                if r.exit_status == 0:
                    good("служба xray активна")
                else:
                    st = (r.stdout or "").strip() or "не найдена"
                    bad(
                        f"служба xray: {html.escape(st)}. Если xray запущен иначе (docker, x-ui), "
                        "задайте restart_cmd в servers.json"
                    )
            if state["ok"]:
                lines.append("➡️ готов к синхронизации")
    except Exception as e:  # noqa: BLE001
        bad("SSH: " + html.escape(explain(e, s)))
    return lines, state["ok"]


async def diag_all():
    """Список сообщений (по ≤3500 символов) с отчётом по всем серверам."""
    res = await asyncio.gather(
        *(asyncio.wait_for(diag_server(i, s), 45) for i, s in enumerate(SERVERS)), return_exceptions=True
    )
    blocks, ready = [], 0
    for s, r in zip(SERVERS, res):
        if isinstance(r, Exception):
            blocks.append(f"<b>{html.escape(s['name'])}</b>\n❌ {html.escape(explain(r, s))}")
        else:
            lines, ok = r
            ready += 1 if ok else 0
            blocks.append("\n".join(lines))
    cur = f"🩺 <b>Диагностика серверов</b>: готово {ready} из {len(SERVERS)}"
    chunks = []
    for b in blocks:
        if len(cur) + len(b) + 2 > 3500:
            chunks.append(cur)
            cur = b
        else:
            cur += "\n\n" + b
    chunks.append(cur)
    return chunks


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
    await safe_answer(c, "Выдаю доступ…")  # синхронизация серверов может занять время
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
    if failed:
        await c.bot.send_message(ADMIN_ID, failed_text(failed))


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
            [Btn(text="🩺 Диагностика серверов", callback_data="adm:diag")],
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
            if failed:
                await c.bot.send_message(ADMIN_ID, failed_text(failed))
        elif act == "d":
            u = ensure_user(tg_id)
            plan = u["plan"] if u["plan"] in PLANS else min(PLANS)
            failed, _, notified = await grant(c.bot, tg_id, plan, days=int(p[3]))
            toast = toast_for(failed, notified)
            await safe_edit(c, *user_card(tg_id))
            if failed:
                await c.bot.send_message(ADMIN_ID, failed_text(failed))
        elif act == "xy":
            failed = await revoke_user(c.bot, tg_id)
            toast = toast_for(failed)
            await safe_edit(c, *user_card(tg_id))
            if failed:
                await c.bot.send_message(ADMIN_ID, failed_text(failed))
        elif act == "r":
            failed, notified = await reset_keys(c.bot, tg_id)
            toast = toast_for(failed, notified)
            await safe_edit(c, *user_card(tg_id))
            if failed:
                await c.bot.send_message(ADMIN_ID, failed_text(failed))
    elif act == "sync":
        if debounce("sync", 5):
            return await c.answer("Уже выполняется")
        await safe_answer(c, "Синхронизирую серверы…")
        failed = await sync_all()
        await c.bot.send_message(
            ADMIN_ID, failed_text(failed) if failed else f"✅ Все серверы ({len(SERVERS)}) синхронизированы"
        )
        return
    elif act == "diag":
        if debounce("diag", 5):
            return await c.answer("Уже выполняется")
        await safe_answer(c, "Проверяю серверы…")
        for chunk in await diag_all():
            await c.bot.send_message(ADMIN_ID, chunk)
        return
    await safe_answer(c, toast, show_alert=bool(toast and toast.startswith("⚠️")))


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
        text += "\n\n" + failed_text(failed)
    await m.answer(text)


@admin.message(Command("revoke"))
async def cmd_revoke(m: Message, command: CommandObject):
    """/revoke <tg_id> — отключить клиента"""
    try:
        tg_id = int((command.args or "").strip())
    except Exception:
        return await m.answer("Формат: /revoke <tg_id>")
    failed = await revoke_user(m.bot, tg_id)
    await m.answer("Отключён." + ("\n\n" + failed_text(failed) if failed else ""))


@admin.message(Command("diag"))
async def cmd_diag(m: Message):
    """/diag — пошаговая проверка всех серверов (SSH, конфиг, xray, права), ничего не меняет"""
    await m.answer("🩺 Проверяю серверы, это займёт до минуты…")
    for chunk in await diag_all():
        await m.answer(chunk)


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
                await notify(bot, ADMIN_ID, failed_text(sorted(failed)))
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
