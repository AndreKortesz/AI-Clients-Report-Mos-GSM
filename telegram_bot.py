import os, requests

TG_TOKEN = os.getenv("TELEGRAM_TOKEN")
TG_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def send_message(text: str):
    if not (TG_TOKEN and TG_CHAT_ID):
        return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML"}, timeout=20)

def format_alerts(alerts):
    if not alerts:
        return "✅ На сейчас тревог нет."
    lines = ["<b>⚠️ Клиенты без ответа в чате и без звонка</b>"]
    for a in alerts[:50]:
        link = f"https://bitrix24.ru/crm/entity/TYPE/{a['owner_type_id']}/ID/{a['owner_id']}"  # при желании подставь свой портал
        line = (f"• Entity {a['owner_type_id']} #{a['owner_id']} — входящее {a['provider_id']} в {a['last_in_created']}"
                f"{' — ' + a['phone'] if a['phone'] else ''}")
        lines.append(line)
    if len(alerts) > 50:
        lines.append(f"... и ещё {len(alerts)-50}")
    return "\n".join(lines)
