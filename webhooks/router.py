from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from typing import Dict, Any, Optional
import json
import logging

router = APIRouter(tags=["webhooks"])

logging.basicConfig(level="INFO", format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("evolution-webhook")

def _pretty(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, indent=2)
    except Exception:
        return str(obj)

async def _extract_json(request: Request) -> Dict[str, Any]:
    ctype = request.headers.get("content-type", "")
    if "application/json" in ctype or "text/json" in ctype:
        return await request.json()

    if "multipart/form-data" in ctype or "application/x-www-form-urlencoded" in ctype:
        form = await request.form()
        if "payload" in form:
            try:
                return json.loads(form["payload"])
            except Exception:
                pass
        return {k: v for k, v in form.items()}

    raw = await request.body()
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return {"raw": raw.decode("utf-8", errors="ignore")}

def _summarize(body: Dict[str, Any], event_name: Optional[str]) -> Dict[str, Any]:
    summary = {"event_path": event_name, "keys": list(body.keys())}
    if isinstance(body.get("event"), str):
        summary["event_field"] = body["event"]
    if isinstance(body.get("messages"), list):
        msgs = body["messages"]
        summary["messages_count"] = len(msgs)
        if msgs:
            m0 = msgs[0]
            summary["first_message_id"] = (m0.get("key") or {}).get("id")
            summary["first_message_from"] = (m0.get("key") or {}).get("remoteJid")
            if isinstance(m0.get("message"), dict):
                summary["first_message_types"] = list(m0["message"].keys())
    if "status" in body:
        summary["status"] = body["status"]
    if "qrcode" in body:
        summary["has_qrcode_base64"] = True
    return summary

@router.get("/webhook/ping")
def webhook_ping():
    log.info("Webhook ping OK")
    return {"ok": True, "msg": "webhook alive"}

@router.post("/webhook")
async def webhook_single(request: Request):
    body = await _extract_json(request)
    log.info("=== Evolution Webhook (single) ===")
    log.info("Headers:\n%s", _pretty(dict(request.headers)))
    log.info("Body:\n%s", _pretty(body))
    log.info("Summary:\n%s", _pretty(_summarize(body, None)))
    return {"ok": True}

# Substitui a rota /webhook/{event_name} por uma flexível
@router.post("/webhook/{tail:path}")
async def webhook_by_events_flex(tail: str, request: Request):
    """
    Aceita caminhos como:
      /webhook/messages-upsert
      /webhook//contacts-update           (com barra dupla)
      /webhook/events/messages-upsert     (se algum proxy inserir prefixos)
    Normaliza para extrair o 'event_name'.
    """
    # normaliza: remove barras duplicadas e leading/trailing
    event_name = "/".join([seg for seg in tail.split("/") if seg]).strip()
    if not event_name:
        event_name = "(empty)"  # muito raro, mas evita string vazia

    body = await _extract_json(request)

    log.info("=== Evolution Webhook (by-events - flex) ===")
    log.info("Event path (raw tail): %s", tail)
    log.info("Event name (normalized): %s", event_name)
    log.info("Headers:\n%s", _pretty(dict(request.headers)))
    log.info("Body:\n%s", _pretty(body))
    log.info("Summary:\n%s", _pretty(_summarize(body, event_name)))

    return {"ok": True, "event": event_name}