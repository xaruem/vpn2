"""Печатает значения для переменных SERVERS_JSON и SSH_KEY_B64 (Railway/Coolify/Dokploy).
Работает на Windows, macOS и Linux (не нужен `base64 -w0`, которого нет на macOS).

    python encode_env.py                      # servers.json + ssh/vpnbot
    python encode_env.py путь/servers.json путь/к/ключу
"""
import base64
import json
import sys

servers_path = sys.argv[1] if len(sys.argv) > 1 else "servers.json"
key_path = sys.argv[2] if len(sys.argv) > 2 else "ssh/vpnbot"


def enc(path):
    with open(path, "rb") as f:
        return f.read()


try:
    raw = enc(servers_path)
    json.loads(raw.decode("utf-8"))
    print(f"SERVERS_JSON ({len(raw)} байт файла) — одной строкой, без пробелов и переносов:")
    print(base64.b64encode(raw).decode(), "\n")
except Exception as e:  # noqa: BLE001
    print(f"! {servers_path}: {e}\n")

try:
    key = enc(key_path)
    text = key.decode("utf-8", "replace")
    if text.startswith("ssh-") or key_path.endswith(".pub"):
        sys.exit("! Это ПУБЛИЧНЫЙ ключ (.pub). Нужен приватный файл vpnbot без расширения.")
    if "PRIVATE KEY" not in text:
        sys.exit(f"! {key_path} не похож на приватный ключ (нет строки BEGIN ... PRIVATE KEY).")
    b64 = base64.b64encode(key).decode()
    print(f"SSH_KEY_B64 ({len(b64)} символов, должно быть 400-700, начало LS0tLS1CRUdJTi):")
    print(b64)
except FileNotFoundError:
    print(f"! файл ключа не найден: {key_path}")
