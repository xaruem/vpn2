import asyncio
import base64
import json
import os
import sys
import tempfile
import time
import traceback
import types
import urllib.parse

from aiogram import Bot, Dispatcher
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import Update
from aiohttp.test_utils import TestClient, TestServer

BOTDIR = os.path.abspath(sys.argv[1])
work = tempfile.mkdtemp()

_sj = os.path.join(BOTDIR, "servers.json")
if not os.path.exists(_sj):
    _sj = os.path.join(BOTDIR, "servers.example.json")
base = json.load(open(_sj, encoding="utf-8"))
servers = []
for i in range(8):
    s = dict(base[0])
    s["name"] = f"🇷🇺 Сервер {i + 1}"
    s["ssh_host"] = f"10.0.0.{i + 1}"
    s["link"] = s["link"].replace("2.26.129.81", f"10.0.0.{i + 1}")
    servers.append(s)
json.dump(servers, open(os.path.join(work, "servers.json"), "w", encoding="utf-8"), ensure_ascii=False)

os.chdir(work)
os.environ.update(BOT_TOKEN="1:x", ADMIN_ID="111", BASE_URL="https://sub.test")
sys.path.insert(0, BOTDIR)
import bot  # noqa: E402

ADMIN = 111
ADMIN_UUID = "bcd508e0-1a55-48f2-878c-8d603d3d8a5e"
CFG = "/usr/local/etc/xray/config.json"
DAY = 86400


# ---------- фейковый SSH ----------
class R:
    def __init__(self, out="", code=0):
        self.stdout = out
        self.exit_status = code


class FakeConn:
    hosts = {}
    down = set()

    def __init__(self, host):
        self.host = host

    async def __aenter__(self):
        if self.host in FakeConn.down:
            raise OSError("connect failed")
        return self

    async def __aexit__(self, *a):
        return False

    async def run(self, cmd, input=None, check=False):
        await asyncio.sleep(0)  # отдаём управление, как настоящая сеть
        st = FakeConn.hosts[self.host]
        f = st["files"]
        out, code = "", 0
        for part in cmd.split(" && "):
            p = part.split()
            try:
                if p[0] == "cat" and p[1] == ">":
                    f[p[2]] = input
                elif p[0] == "cat":
                    out += f[p[1]]
                elif p[0] == "cp":
                    f[p[2]] = f[p[1]]
                elif p[0] == "mv":
                    f[p[2]] = f.pop(p[1])
                elif p[0] == "rm":
                    f.pop(p[1])
                elif p[0] == "systemctl":
                    if st.get("fail_restart_once"):
                        st["fail_restart_once"] = False
                        raise RuntimeError("restart failed")
                    st["restarts"] += 1
                elif "-test" in p:
                    cfg = json.loads(f[p[-1]])
                    assert isinstance(cfg["inbounds"][0]["settings"]["clients"], list)
                else:
                    raise RuntimeError(f"unknown: {part}")
            except Exception:
                code = 1
                break
        if check and code:
            raise RuntimeError(f"command failed: {cmd}")
        return R(out, code)


bot.asyncssh.connect = lambda host, **kw: FakeConn(host)
bot.ssh_key_path = lambda s: "/dev/null"  # ключ в тестах не нужен (отдельный тест ниже проверяет настоящую функцию)
_real_ssh_key_path = None


def reset_hosts():
    FakeConn.down = set()
    for i in range(8):
        cfg = {
            "inbounds": [
                {
                    "tag": "vless-in",
                    "protocol": "vless",
                    "port": 443,
                    "settings": {
                        "clients": [{"id": ADMIN_UUID, "flow": "xtls-rprx-vision"}],
                        "decryption": "none",
                    },
                }
            ]
        }
        FakeConn.hosts[f"10.0.0.{i + 1}"] = {"files": {CFG: json.dumps(cfg)}, "restarts": 0}


def clients_on(i):
    cfg = json.loads(FakeConn.hosts[f"10.0.0.{i + 1}"]["files"][CFG])
    return cfg["inbounds"][0]["settings"]["clients"]


def ids_on(i):
    return {c["id"] for c in clients_on(i)}


def restarts():
    return sum(h["restarts"] for h in FakeConn.hosts.values())


# ---------- фейковый Telegram (юнит-уровень) ----------
class FakeBot:
    def __init__(self):
        self.sent = []  # (chat_id, text)
        self.kbs = []  # (chat_id, reply_markup)
        self.fail_for = set()

    async def send_message(self, chat_id, text, **kw):
        if chat_id in self.fail_for:
            raise RuntimeError("Forbidden: bot can't initiate conversation")
        self.sent.append((chat_id, text))
        self.kbs.append((chat_id, kw.get("reply_markup")))

    def to(self, chat_id):
        return [t for c, t in self.sent if c == chat_id]


class FakeMsg:
    def __init__(self, text="start", html_text=None, mid=1):
        self.text = text
        self.html_text = html_text or text
        self.message_id = mid
        self.edits = []
        self.last_text = None
        self.last_kb = None

    async def edit_text(self, text, reply_markup=None, **kw):
        key = (text, repr(reply_markup))
        if self.edits and self.edits[-1] == key:
            raise TelegramBadRequest(method=None, message="Bad Request: message is not modified")
        self.edits.append(key)
        self.last_text, self.last_kb = text, reply_markup


class FakeCb:
    def __init__(self, data, uid, fb, msg=None, username="user"):
        self.data = data
        self.from_user = types.SimpleNamespace(id=uid, username=username, full_name="Имя <b>")
        self.message = msg or FakeMsg()
        self.bot = fb
        self.answers = []

    async def answer(self, text=None, show_alert=False, **kw):
        self.answers.append((text, show_alert))


class FakeMessage:
    def __init__(self, uid, fb):
        self.from_user = types.SimpleNamespace(id=uid, username="adm")
        self.bot = fb
        self.answers = []

    async def answer(self, text, reply_markup=None, **kw):
        self.answers.append((text, reply_markup))


def cmd(args):
    return types.SimpleNamespace(args=args)


def cbs(kb):
    return [b.callback_data for row in kb.inline_keyboard for b in row]


def mk_user(uid, plan, days, username=None):
    bot.ensure_user(uid, username or f"user{uid}")
    bot.db.execute(
        "UPDATE users SET plan=?, expire=? WHERE tg_id=?", (plan, int(time.time()) + int(days * DAY), uid)
    )
    bot.db.commit()
    return bot.get_user(uid)


def fresh():
    reset_hosts()
    bot._recent.clear()
    bot._done.clear()
    bot._last_paid.clear()
    bot.db.execute("DELETE FROM users")
    bot.db.execute("DELETE FROM payments")
    bot.db.commit()
    return FakeBot()


def days_left(uid):
    return (bot.get_user(uid)["expire"] - time.time()) / DAY


def pay_count(uid):
    r = bot.db.execute("SELECT COUNT(*) n, COALESCE(SUM(amount),0) s FROM payments WHERE tg_id=?", (uid,)).fetchone()
    return r["n"], r["s"]


async def run_watcher(fb, t=0.3):
    try:
        await asyncio.wait_for(bot.watcher(fb), t)
    except asyncio.TimeoutError:
        pass


# ---------- раннер ----------
results = []
tests = []


def check(name):
    def deco(fn):
        async def run():
            try:
                await fn()
                results.append(("PASS", name, ""))
            except AssertionError as e:
                results.append(("FAIL", name, f"assert: {e}"))
            except Exception as e:  # noqa: BLE001
                results.append(("FAIL", name, f"{type(e).__name__}: {e}"))
                traceback.print_exc()

        tests.append(run)
        return run

    return deco


# =============== СИНХРОНИЗАЦИЯ И ПОДПИСКА ===============
@check("sync: 100₽ → серверы 1-3, 200₽ → 1-6; просрочен/без плана нигде; UUID админа цел; email/flow; без лишних рестартов")
async def t_sync():
    fresh()
    a, b = mk_user(1001, 100, 10), mk_user(1002, 200, 10)
    c, d = mk_user(1003, 200, -1), mk_user(1004, 0, 0)
    assert await bot.sync_all() == []
    for i in range(8):
        ids = ids_on(i)
        assert (a["uuid"] in ids) == (i < 3), f"A на сервере {i + 1}"
        assert (b["uuid"] in ids) == (i < 6), f"B на сервере {i + 1}"
        assert c["uuid"] not in ids and d["uuid"] not in ids, f"лишний клиент на {i + 1}"
        assert ADMIN_UUID in ids, f"UUID админа пропал на {i + 1}"
    cl = [x for x in clients_on(0) if x["id"] == a["uuid"]][0]
    assert cl["flow"] == "xtls-rprx-vision" and cl["email"] == "tg_1001", cl
    before = restarts()
    await bot.sync_all()
    assert restarts() == before, "лишние рестарты Xray"


@check("sync: сервер без flow (xhttp) получает клиента без поля flow")
async def t_noflow():
    fresh()
    saved = bot.SERVERS[0]["flow"]
    bot.SERVERS[0]["flow"] = ""
    try:
        a = mk_user(1010, 100, 5)
        await bot.sync_all()
        cl = [x for x in clients_on(0) if x["id"] == a["uuid"]][0]
        assert "flow" not in cl, cl
        cl2 = [x for x in clients_on(1) if x["id"] == a["uuid"]][0]
        assert cl2["flow"] == "xtls-rprx-vision"
    finally:
        bot.SERVERS[0]["flow"] = saved


@check("подписка: 3 ссылки для 100₽, 6 для 200₽, имена Сервер N, заголовки; просрочен → пусто; чужой токен → 404")
async def t_sub():
    fresh()
    a, b, c = mk_user(1101, 100, 10), mk_user(1102, 200, 10), mk_user(1103, 200, -1)
    async with TestClient(TestServer(bot.make_app())) as cl:
        r = await cl.get(f"/sub/{a['token']}")
        assert r.status == 200
        lines = base64.b64decode(await r.text()).decode().split("\n")
        assert len(lines) == 3, len(lines)
        names = [urllib.parse.unquote(l.split("#")[1]) for l in lines]
        assert names == ["🇷🇺 Сервер 1", "🇷🇺 Сервер 2", "🇷🇺 Сервер 3"], names
        assert all(a["uuid"] in l for l in lines)
        assert "expire=" in r.headers["subscription-userinfo"]
        assert r.headers["profile-title"].startswith("base64:")
        r = await cl.get(f"/sub/{b['token']}")
        assert len(base64.b64decode(await r.text()).decode().split("\n")) == 6
        r = await cl.get(f"/sub/{c['token']}")
        assert r.status == 200 and base64.b64decode(await r.text()) == b""
        assert (await cl.get("/sub/nope")).status == 404


@check("сбой одного сервера не ломает остальные, его имя попадает в failed")
async def t_isolation():
    fresh()
    u = mk_user(1201, 200, 10)
    FakeConn.down = {"10.0.0.2"}
    assert await bot.sync_all() == ["🇷🇺 Сервер 2"]
    assert u["uuid"] in ids_on(0) and u["uuid"] in ids_on(2) and u["uuid"] not in ids_on(1)


@check("откат: Xray не стартовал после правки → конфиг возвращён, сервер в failed")
async def t_rollback():
    fresh()
    mk_user(1301, 100, 10)
    before = FakeConn.hosts["10.0.0.1"]["files"][CFG]
    FakeConn.hosts["10.0.0.1"]["fail_restart_once"] = True
    failed = await bot.sync_all()
    assert "🇷🇺 Сервер 1" in failed, failed
    assert FakeConn.hosts["10.0.0.1"]["files"][CFG] == before, "конфиг не откатился"
    assert await bot.sync_all() == [], "следующая синхронизация не починила сервер"


# =============== ОПЛАТА И ВЫДАЧА ===============
@check("подтверждение заявки: тариф, +30 дней, ссылка клиенту, сервер обновлён, оплата записана")
async def t_ok():
    fb = fresh()
    bot.ensure_user(2001, "newbie")
    c = FakeCb("ok:2001:100", ADMIN, fb, FakeMsg(html_text="заявка"))
    await bot.admin_ok(c)
    u = bot.get_user(2001)
    assert u["plan"] == 100 and abs(days_left(2001) - 30) < 0.01
    assert any("/sub/" + u["token"] in t for t in fb.to(2001)), fb.sent
    assert u["uuid"] in ids_on(0) and u["uuid"] not in ids_on(3)
    assert pay_count(2001) == (1, 100)
    assert c.answers and "Подтверждено" in c.message.last_text and c.message.last_kb is None


@check("двойной тап «Подтвердить» (гонка) продлевает только один раз")
async def t_double_ok():
    fb = fresh()
    bot.ensure_user(2002, "x")
    msg = FakeMsg(html_text="заявка", mid=77)
    c1, c2 = FakeCb("ok:2002:100", ADMIN, fb, msg), FakeCb("ok:2002:100", ADMIN, fb, msg)
    await asyncio.gather(bot.admin_ok(c1), bot.admin_ok(c2))
    assert abs(days_left(2002) - 30) < 0.01, f"{days_left(2002):.1f} дней"
    assert len(fb.to(2002)) == 1 and pay_count(2002) == (1, 100)
    assert any(a[0] == "Уже обработано" for a in c1.answers + c2.answers)


@check("«Отклонить»: клиенту уходит отказ, доступ не выдан, повторный тап игнорируется")
async def t_no():
    fb = fresh()
    bot.ensure_user(2003, "x")
    msg = FakeMsg(html_text="заявка", mid=78)
    await bot.admin_no(FakeCb("no:2003", ADMIN, fb, msg))
    await bot.admin_no(FakeCb("no:2003", ADMIN, fb, msg))
    assert bot.get_user(2003)["plan"] == 0 and len(fb.to(2003)) == 1


@check("продление от текущего срока (+60), апгрейд 100→200 добавляет серверы 4-6")
async def t_extend_upgrade():
    fb = fresh()
    bot.ensure_user(2101, "x")
    await bot.grant(fb, 2101, 100)
    await bot.grant(fb, 2101, 100)
    assert abs(days_left(2101) - 60) < 0.01
    await bot.grant(fb, 2101, 200)
    u = bot.get_user(2101)
    for i in range(8):
        assert (u["uuid"] in ids_on(i)) == (i < 6), f"сервер {i + 1}"


@check("«Я оплатил»: антиспам (одна заявка в минуту), кнопки у админа, неверный тариф игнорируется")
async def t_paid():
    fb = fresh()
    bot.ensure_user(6001, "p")
    c1, c2 = FakeCb("paid:100", 6001, fb, username="p"), FakeCb("paid:100", 6001, fb, username="p")
    await bot.paid(c1)
    await bot.paid(c2)
    assert len(fb.to(ADMIN)) == 1, "заявка продублировалась"
    assert c2.answers[0][1] is True
    kb = [k for chat, k in fb.kbs if chat == ADMIN][0]
    assert {"ok:6001:100", "no:6001", "adm:u:6001"} <= set(cbs(kb)), cbs(kb)
    c3 = FakeCb("paid:999", 6002, fb)
    await bot.paid(c3)
    assert len(fb.to(ADMIN)) == 1


@check("двойное нажатие «Моя подписка» не падает с 'message is not modified'")
async def t_double_mysub():
    fb = fresh()
    bot.ensure_user(5001, "dbl")
    msg = FakeMsg()
    c1, c2 = FakeCb("mysub", 5001, fb, msg), FakeCb("mysub", 5001, fb, msg)
    await bot.mysub(c1)
    await bot.mysub(c2)
    assert c2.answers, "второй callback не подтверждён"


@check("/grant: создаёт клиента, показывает админу ссылку, оплату НЕ записывает; без аргументов — подсказка")
async def t_grant_cmd():
    fb = fresh()
    m = FakeMessage(ADMIN, fb)
    await bot.cmd_grant(m, cmd("7777 100"))
    u = bot.get_user(7777)
    assert u and u["plan"] == 100 and "/sub/" + u["token"] in m.answers[0][0]
    assert pay_count(7777) == (0, 0)
    m2 = FakeMessage(ADMIN, fb)
    await bot.cmd_grant(m2, cmd(None))
    assert "Формат" in m2.answers[0][0]


@check("клиент не запускал бота: grant не падает, админу сообщают «не получил» и дают ссылку")
async def t_notify_fail():
    fb = fresh()
    fb.fail_for = {9001}
    failed, u, notified = await bot.grant(fb, 9001, 100)
    assert notified is False and bot.get_user(9001)["plan"] == 100 and failed == []
    m = FakeMessage(ADMIN, fb)
    await bot.cmd_grant(m, cmd("9001 100"))
    assert "не получил" in m.answers[0][0] and "/sub/" in m.answers[0][0]


# =============== СРОКИ ===============
@check("истечение срока: watcher отключает клиента на всех серверах, остальных не трогает, уведомляет")
async def t_expire():
    fb = fresh()
    a, b = mk_user(3001, 100, 10), mk_user(3002, 200, 10)
    await bot.sync_all()
    bot.db.execute("UPDATE users SET expire=? WHERE tg_id=3001", (int(time.time()) - 5,))
    bot.db.commit()
    await run_watcher(fb)
    assert bot.get_user(3001)["plan"] == 0
    for i in range(8):
        assert a["uuid"] not in ids_on(i), f"A остался на сервере {i + 1}"
    assert b["uuid"] in ids_on(0) and b["uuid"] in ids_on(5)
    assert fb.to(3001), "нет уведомления об окончании"


@check("предупреждение за 3 дня шлётся один раз")
async def t_warn():
    fb = fresh()
    mk_user(3003, 100, 2)
    await run_watcher(fb)
    await run_watcher(fb)
    assert len(fb.to(3003)) == 1, fb.sent


@check("watcher: недоступный сервер → одно уведомление админу (не спам каждый час); после восстановления сервер чинится")
async def t_watcher_alert():
    fb = fresh()
    u = mk_user(8101, 100, 5)
    FakeConn.down = {"10.0.0.2"}
    orig = asyncio.sleep

    async def fast(d, *a, **k):
        await orig(0 if d >= 3600 else d)

    asyncio.sleep = fast
    try:
        await run_watcher(fb, 0.5)  # много итераций цикла подряд
    finally:
        asyncio.sleep = orig
    alerts = [t for t in fb.to(ADMIN) if "Сервер 2" in t]
    assert len(alerts) == 1, f"{len(alerts)} уведомлений"
    FakeConn.down = set()
    await run_watcher(fb)
    assert u["uuid"] in ids_on(1), "сервер 2 не починился сам"


# =============== АДМИН-ПАНЕЛЬ ===============
@check("/admin: меню со статистикой, клиентами и синхронизацией")
async def t_menu():
    fb = fresh()
    m = FakeMessage(ADMIN, fb)
    await bot.cmd_admin(m)
    assert {"adm:stats", "adm:l:0", "adm:sync"} <= set(cbs(m.answers[0][1]))


@check("статистика: клиенты, активные по тарифам, истекающие, выручка 30 дней и всего")
async def t_stats():
    fb = fresh()
    mk_user(4101, 100, 2)
    mk_user(4102, 200, 20)
    mk_user(4103, 100, -1)
    mk_user(4104, 0, 0)
    now = int(time.time())
    for uid, plan, ts in [(4101, 100, now), (4102, 200, now), (4101, 100, now - 40 * DAY)]:
        bot.db.execute("INSERT INTO payments(tg_id,plan,amount,ts) VALUES(?,?,?,?)", (uid, plan, plan, ts))
    bot.db.commit()
    c = FakeCb("adm:stats", ADMIN, fb)
    await bot.adm(c)
    t = c.message.last_text
    for needle in ["Всего клиентов: 4", "Активных: 2 (100 ₽: 1, 200 ₽: 1)", "Истекают за 3 дня: 1",
                   "Выручка за 30 дней: 300 ₽", "Выручка всего: 400 ₽"]:
        assert needle in t, f"нет «{needle}» в:\n{t}"


@check("список клиентов: пагинация по 8, навигация вперёд/назад")
async def t_pagination():
    fb = fresh()
    for i in range(10):
        mk_user(4200 + i, 100, 5 + i)
    c0 = FakeCb("adm:l:0", ADMIN, fb)
    await bot.adm(c0)
    k0 = cbs(c0.message.last_kb)
    assert sum(x.startswith("adm:u:") for x in k0) == 8 and "adm:l:1" in k0 and "adm:l:-1" not in k0
    c1 = FakeCb("adm:l:1", ADMIN, fb)
    await bot.adm(c1)
    k1 = cbs(c1.message.last_kb)
    assert sum(x.startswith("adm:u:") for x in k1) == 2 and "adm:l:0" in k1 and "adm:l:2" not in k1


@check("карточка → «+30 дн · 100 ₽»: выдача, оплата записана, карточка обновилась; быстрый второй тап игнорируется")
async def t_card_grant():
    fb = fresh()
    bot.ensure_user(7001, "alice")
    msg = FakeMsg()
    c = FakeCb("adm:g:7001:100", ADMIN, fb, msg)
    await bot.adm(c)
    assert bot.get_user(7001)["plan"] == 100 and abs(days_left(7001) - 30) < 0.01
    assert pay_count(7001) == (1, 100) and fb.to(7001)
    assert "🟢" in msg.last_text and "adm:x:7001" in cbs(msg.last_kb)
    c2 = FakeCb("adm:g:7001:100", ADMIN, fb, msg)
    await bot.adm(c2)
    assert c2.answers[0][0] == "Уже выполнено"
    assert abs(days_left(7001) - 30) < 0.01 and pay_count(7001) == (1, 100)


@check("«🎁 +7 дней»: без записи оплаты; тариф сохраняется, а для неактивного — минимальный")
async def t_gift():
    fb = fresh()
    mk_user(7101, 200, 5)
    mk_user(7102, 0, 0)
    await bot.adm(FakeCb("adm:d:7101:7", ADMIN, fb))
    await bot.adm(FakeCb("adm:d:7102:7", ADMIN, fb))
    assert bot.get_user(7101)["plan"] == 200 and abs(days_left(7101) - 12) < 0.01
    assert bot.get_user(7102)["plan"] == 100 and abs(days_left(7102) - 7) < 0.01
    assert pay_count(7101) == (0, 0) and pay_count(7102) == (0, 0)


@check("«⛔ Отключить»: сначала подтверждение (ничего не меняется), после «Да» — снят со всех серверов, клиенту сообщение")
async def t_revoke():
    fb = fresh()
    u = mk_user(7201, 100, 10)
    await bot.sync_all()
    assert u["uuid"] in ids_on(0)
    c = FakeCb("adm:x:7201", ADMIN, fb)
    await bot.adm(c)
    assert bot.get_user(7201)["plan"] == 100 and "adm:xy:7201" in cbs(c.message.last_kb)
    assert u["uuid"] in ids_on(0)
    await bot.adm(FakeCb("adm:xy:7201", ADMIN, fb))
    r = bot.get_user(7201)
    assert r["plan"] == 0 and r["expire"] == 0
    assert all(u["uuid"] not in ids_on(i) for i in range(8))
    assert any("отключён" in t for t in fb.to(7201))


@check("«🔑 Сбросить ссылку»: новый UUID и токен, старый убран с серверов, клиент получил новую ссылку")
async def t_reset():
    fb = fresh()
    old = mk_user(7301, 200, 10)
    await bot.sync_all()
    await bot.adm(FakeCb("adm:r:7301", ADMIN, fb))
    new = bot.get_user(7301)
    assert new["uuid"] != old["uuid"] and new["token"] != old["token"]
    for i in range(6):
        assert old["uuid"] not in ids_on(i) and new["uuid"] in ids_on(i), f"сервер {i + 1}"
    assert any("/sub/" + new["token"] in t for t in fb.to(7301))
    async with TestClient(TestServer(bot.make_app())) as cl:
        assert len(base64.b64decode(await (await cl.get(f"/sub/{new['token']}")).text()).decode().split("\n")) == 6
        assert (await cl.get(f"/sub/{old['token']}")).status == 404, "старая ссылка продолжает работать"


@check("«🔄 Синхронизировать»: при сбое — предупреждение с именем сервера, иначе ✅")
async def t_sync_btn():
    fb = fresh()
    mk_user(7401, 100, 10)
    FakeConn.down = {"10.0.0.3"}
    c = FakeCb("adm:sync", ADMIN, fb)
    await bot.adm(c)
    text, alert = c.answers[0]
    assert text.startswith("⚠️") and "Сервер 3" in text and alert is True, c.answers
    FakeConn.down = set()
    bot._recent.clear()
    c2 = FakeCb("adm:sync", ADMIN, fb)
    await bot.adm(c2)
    assert c2.answers[0][0].startswith("✅") and c2.answers[0][1] is False


@check("/user: поиск по ID и по @username (без учёта регистра), неизвестный → подсказка")
async def t_user_cmd():
    fb = fresh()
    bot.ensure_user(7501, "Bob")
    for arg in ("@bob", "7501"):
        m = FakeMessage(ADMIN, fb)
        await bot.cmd_user(m, cmd(arg))
        assert "7501" in m.answers[0][0], arg
    for arg in ("@nobody", "zzz", None):
        m = FakeMessage(ADMIN, fb)
        await bot.cmd_user(m, cmd(arg))
        assert "Формат" in m.answers[0][0], arg


@check("/broadcast: только активным клиентам, HTML экранируется, отчёт «Отправлено N/M»")
async def t_broadcast():
    fb = fresh()
    mk_user(8001, 100, 5)
    mk_user(8002, 0, 0)
    mk_user(8003, 200, -1)
    m = FakeMessage(ADMIN, fb)
    await bot.cmd_broadcast(m, cmd("техработы <b>сегодня</b>"))
    assert fb.to(8001) == ["техработы &lt;b&gt;сегодня&lt;/b&gt;"] and not fb.to(8002) and not fb.to(8003)
    assert m.answers[0][0] == "Отправлено: 1/1"


# =============== ДОСТУП И РЕАЛЬНЫЙ РОУТИНГ AIOGRAM ===============
@check("доступ: админские хендлеры только в admin-роутере с фильтром по ADMIN_ID")
async def t_access_structure():
    assert bot.ADMIN_FILTER.resolve(types.SimpleNamespace(from_user=types.SimpleNamespace(id=ADMIN)))
    assert not bot.ADMIN_FILTER.resolve(types.SimpleNamespace(from_user=types.SimpleNamespace(id=999)))
    admin_cb = {h.callback.__name__ for h in bot.admin.callback_query.handlers}
    admin_msg = {h.callback.__name__ for h in bot.admin.message.handlers}
    user_cb = {h.callback.__name__ for h in bot.router.callback_query.handlers}
    user_msg = {h.callback.__name__ for h in bot.router.message.handlers}
    assert {"admin_ok", "admin_no", "adm"} <= admin_cb, admin_cb
    assert {"cmd_admin", "cmd_grant", "cmd_revoke", "cmd_user", "cmd_broadcast"} <= admin_msg, admin_msg
    assert not ({"admin_ok", "admin_no", "adm"} & user_cb), user_cb
    assert user_msg == {"start"}, user_msg
    assert bot.admin.callback_query._handler.filters, "на роутере нет фильтра callback_query"
    assert bot.admin.message._handler.filters, "на роутере нет фильтра message"


_dp = None


def get_dp():
    # роутер можно подключить только к одному Dispatcher, поэтому он общий
    global _dp
    if _dp is None:
        _dp = Dispatcher()
        _dp.include_router(bot.admin)
        _dp.include_router(bot.router)
    return _dp


class CaptureBot(Bot):
    def __init__(self):
        super().__init__("123456:TESTTOKEN")
        self.calls = []

    async def __call__(self, method, request_timeout=None):
        self.calls.append(method)
        return True


def tg_user(uid, username):
    return {"id": uid, "is_bot": False, "first_name": "N", "username": username}


def upd_msg(uid, text, username="u"):
    ent = [{"type": "bot_command", "offset": 0, "length": len(text.split()[0])}] if text.startswith("/") else []
    return {
        "update_id": 1,
        "message": {
            "message_id": 10, "date": 1700000000, "chat": {"id": uid, "type": "private"},
            "from": tg_user(uid, username), "text": text, "entities": ent,
        },
    }


def upd_cb(uid, data, username="u", text="сообщение"):
    return {
        "update_id": 2,
        "callback_query": {
            "id": "1", "from": tg_user(uid, username), "chat_instance": "x", "data": data,
            "message": {
                "message_id": 10, "date": 1700000000, "chat": {"id": uid, "type": "private"},
                "from": {"id": 5, "is_bot": True, "first_name": "bot"}, "text": text,
            },
        },
    }


async def feed(dp, cb, payload):
    cb.calls.clear()
    await dp.feed_update(cb, Update.model_validate(payload, context={"bot": cb}))
    return list(cb.calls)


@check("реальный aiogram: /admin и кнопки админки доступны админу и молча игнорируются у чужих")
async def t_real_access():
    fresh()
    dp = get_dp()
    cb = CaptureBot()
    try:
        calls = await feed(dp, cb, upd_msg(ADMIN, "/admin", "adm"))
        assert [type(c).__name__ for c in calls] == ["SendMessage"] and "Админ-панель" in calls[0].text, calls
        assert await feed(dp, cb, upd_msg(999, "/admin", "evil")) == [], "чужой получил админку"
        assert await feed(dp, cb, upd_cb(999, "adm:stats", "evil")) == [], "чужой нажал админскую кнопку"
        assert await feed(dp, cb, upd_cb(999, "ok:999:200", "evil")) == [], "чужой подтвердил оплату"
        assert bot.get_user(999) is None or bot.get_user(999)["plan"] == 0
        calls = await feed(dp, cb, upd_cb(ADMIN, "adm:stats", "adm"))
        names = [type(c).__name__ for c in calls]
        assert names == ["EditMessageText", "AnswerCallbackQuery"] and "Статистика" in calls[0].text, calls
    finally:
        await cb.session.close()


@check("реальный aiogram: клиент проходит путь /start → тариф → «Я оплатил», админ подтверждает, клиент получает ссылку")
async def t_real_flow():
    fb = fresh()
    dp = get_dp()
    cb = CaptureBot()
    try:
        calls = await feed(dp, cb, upd_msg(5555, "/start", "client"))
        assert type(calls[0]).__name__ == "SendMessage" and "adm" not in str(cbs(calls[0].reply_markup))
        assert {"buy", "mysub", "help"} <= set(cbs(calls[0].reply_markup))
        calls = await feed(dp, cb, upd_cb(5555, "buy", "client"))
        assert "Выберите тариф" in calls[0].text
        calls = await feed(dp, cb, upd_cb(5555, "plan:100", "client"))
        assert "5555" in calls[0].text and "@mannapov_ulugbek" in calls[0].text
        calls = await feed(dp, cb, upd_cb(5555, "paid:100", "client"))
        send = [c for c in calls if type(c).__name__ == "SendMessage"][0]
        assert send.chat_id == ADMIN and "ok:5555:100" in cbs(send.reply_markup)
        calls = await feed(dp, cb, upd_cb(ADMIN, "ok:5555:100", "adm", text="💰 Заявка на оплату"))
        sends = [c for c in calls if type(c).__name__ == "SendMessage"]
        assert any(c.chat_id == 5555 and "/sub/" in c.text for c in sends), [(c.chat_id, c.text) for c in sends]
        assert bot.get_user(5555)["plan"] == 100
        # /grant через настоящий парсер команд
        calls = await feed(dp, cb, upd_msg(ADMIN, "/grant 6666 200", "adm"))
        assert any(type(c).__name__ == "SendMessage" and c.chat_id == 6666 for c in calls)
        assert bot.get_user(6666)["plan"] == 200 and abs(days_left(6666) - 30) < 0.01
    finally:
        await cb.session.close()


@check("настройки без файлов: SERVERS_JSON (base64 и сырой JSON), понятная ошибка без них")
async def t_servers_env():
    raw = open(os.path.join(work, "servers.json"), encoding="utf-8").read()
    saved = os.environ.get("SERVERS_JSON")
    try:
        os.environ["SERVERS_JSON"] = base64.b64encode(raw.encode()).decode()
        assert len(bot.load_servers()) == 8
        os.environ["SERVERS_JSON"] = raw
        assert len(bot.load_servers()) == 8
        os.environ.pop("SERVERS_JSON")
        old = bot.SERVERS_FILE
        bot.SERVERS_FILE = "/nonexistent/servers.json"
        try:
            bot.load_servers()
            assert False, "должна быть ошибка"
        except SystemExit as e:
            assert "SERVERS_JSON" in str(e)
        finally:
            bot.SERVERS_FILE = old
    finally:
        if saved is None:
            os.environ.pop("SERVERS_JSON", None)
        else:
            os.environ["SERVERS_JSON"] = saved


@check("SSH-ключ из SSH_KEY_B64 пишется в файл с правами 600; нет ключа — понятная ошибка")
async def t_key_env():
    import stat
    real = bot.__dict__.get("_orig_ssh_key_path")
    assert real is None  # подмена из шапки теста не должна влиять на следующую проверку
    src = open(os.path.join(BOTDIR, "bot.py"), encoding="utf-8").read()
    ns = {"os": os, "base64": base64, "_key_cache": {}}
    start = src.index("def ssh_key_path(s):")
    end = src.index("\n\n\ndb = sqlite3.connect(")
    exec(src[start:end], ns)
    fn = ns["ssh_key_path"]
    os.environ.pop("SSH_KEY", None)
    os.environ.pop("SSH_KEY_B64", None)
    try:
        fn({"ssh_key": "/keys/nope"})
        assert False, "должна быть ошибка"
    except FileNotFoundError as e:
        assert "SSH_KEY_B64" in str(e)
    key = "-----BEGIN OPENSSH PRIVATE KEY-----\nAAA\n-----END OPENSSH PRIVATE KEY-----"
    os.environ["SSH_KEY_B64"] = base64.b64encode(key.encode()).decode()
    try:
        p = fn({"ssh_key": "/keys/nope"})
        assert stat.S_IMODE(os.stat(p).st_mode) == 0o600, oct(os.stat(p).st_mode)
        assert open(p).read() == key + "\n"
        existing = os.path.join(work, "k")
        open(existing, "w").write("x")
        assert fn({"ssh_key": existing}) == existing, "существующий файл должен иметь приоритет"
    finally:
        os.environ.pop("SSH_KEY_B64", None)


async def main():
    for t in tests:
        await t()
    print()
    for status, name, err in results:
        print(f"{status}  {name}" + (f"\n      -> {err}" if err else ""))
    fails = sum(1 for s, _, _ in results if s == "FAIL")
    print(f"\nИтого: {len(results) - fails} прошло, {fails} упало (всего {len(results)})")
    sys.exit(1 if fails else 0)


asyncio.run(main())
