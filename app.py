# app.py
from fastapi import FastAPI, Request, Header
from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional, Union
from datetime import datetime

app = FastAPI()

# ---------- MODELOS "INTERNOS" (normalizados) ----------
class InternalMessageQuotedAudio(BaseModel):
    url: str
    mimetype: str
    seconds: Optional[int] = None
    ptt: Optional[bool] = None
    file_length: Optional[int] = None
    file_sha256: Optional[str] = None
    file_enc_sha256: Optional[str] = None
    media_key: Optional[str] = None
    direct_path: Optional[str] = None
    waveform: Optional[str] = None

class InternalMessage(BaseModel):
    event: str = "messages.upsert"
    instance: str
    remote_jid: str
    from_me: bool
    message_id: str
    participant: Optional[str] = None
    push_name: Optional[str] = None
    message_type: str
    text: Optional[str] = None
    timestamp: Optional[int] = None
    source: Optional[str] = None
    status: Optional[str] = None
    # context
    quoted_type: Optional[str] = None
    quoted_audio: Optional[InternalMessageQuotedAudio] = None

class InternalContactUpdate(BaseModel):
    event: str = "contacts.update"
    instance: str
    remote_jid: str
    push_name: Optional[str] = None
    profile_pic_url: Optional[str] = None

class InternalPresenceUpdate(BaseModel):
    event: str = "presence.update"
    instance: str
    chat_id: str
    participant: str
    last_known_presence: str

class InternalChatUpdate(BaseModel):
    event: str
    instance: str
    chat_id: Optional[str] = None
    name: Optional[str] = None
    raw: Dict[str, Any] = Field(default_factory=dict)

# ---------- HELPERS ----------
def _ensure_list(x: Union[List[Any], Dict[str, Any]]) -> List[Dict[str, Any]]:
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]

def _safe_get(d: Dict[str, Any], path: str, default=None):
    cur = d
    for p in path.split("."):
        if not isinstance(cur, dict) or p not in cur:
            return default
        cur = cur[p]
    return cur

def _jid_type(remote_jid: str) -> str:
    # g.us = grupo, s.whatsapp.net = contato normal, status@broadcast etc.
    if remote_jid.endswith("@g.us"):
        return "group"
    if remote_jid.endswith("@s.whatsapp.net"):
        return "user"
    return "other"

# ---------- NORMALIZADORES POR EVENTO ----------
def normalize_messages_upsert(body: Dict[str, Any]) -> List[InternalMessage]:
    out: List[InternalMessage] = []
    instance = body.get("instance")
    data = body.get("data", {})
    remote_jid = _safe_get(data, "key.remoteJid")
    message_id = _safe_get(data, "key.id")
    from_me = bool(_safe_get(data, "key.fromMe", False))
    participant = _safe_get(data, "key.participant")
    push_name = data.get("pushName")
    status = data.get("status")
    message_type = data.get("messageType") or "unknown"
    timestamp = data.get("messageTimestamp")
    source = data.get("source")
    msg_obj = data.get("message") or {}

    # texto simples (conversation)
    text = msg_obj.get("conversation")

    # contexto/quoted
    quoted_audio = None
    quoted_type = None
    q = _safe_get(data, "contextInfo.quotedMessage")
    if isinstance(q, dict) and "audioMessage" in q:
        qa = q["audioMessage"]
        quoted_type = "audioMessage"
        quoted_audio = InternalMessageQuotedAudio(
            url=qa.get("url"),
            mimetype=qa.get("mimetype"),
            seconds=qa.get("seconds"),
            ptt=qa.get("ptt"),
            file_length=int(qa["fileLength"]) if "fileLength" in qa else None,
            file_sha256=qa.get("fileSha256"),
            file_enc_sha256=qa.get("fileEncSha256"),
            media_key=qa.get("mediaKey"),
            direct_path=qa.get("directPath"),
            waveform=qa.get("waveform"),
        )

    out.append(InternalMessage(
        instance=instance,
        remote_jid=remote_jid,
        from_me=from_me,
        message_id=message_id,
        participant=participant,
        push_name=push_name,
        message_type=message_type,
        text=text,
        timestamp=timestamp,
        source=source,
        status=status,
        quoted_type=quoted_type,
        quoted_audio=quoted_audio
    ))
    return out

def normalize_contacts_update(body: Dict[str, Any]) -> List[InternalContactUpdate]:
    out: List[InternalContactUpdate] = []
    instance = body.get("instance")
    for item in _ensure_list(body.get("data")):
        out.append(InternalContactUpdate(
            instance=instance,
            remote_jid=item.get("remoteJid"),
            push_name=item.get("pushName"),
            profile_pic_url=item.get("profilePicUrl"),
        ))
    return out

def normalize_presence_update(body: Dict[str, Any]) -> List[InternalPresenceUpdate]:
    out: List[InternalPresenceUpdate] = []
    instance = body.get("instance")
    data = body.get("data", {})
    chat_id = data.get("id")
    presences = data.get("presences") or {}
    for participant, info in presences.items():
        out.append(InternalPresenceUpdate(
            instance=instance,
            chat_id=chat_id,
            participant=participant,
            last_known_presence=info.get("lastKnownPresence", "unknown")
        ))
    return out

def normalize_chats_update(body: Dict[str, Any]) -> List[InternalChatUpdate]:
    out: List[InternalChatUpdate] = []
    instance = body.get("instance")
    for item in _ensure_list(body.get("data")):
        out.append(InternalChatUpdate(
            event="chats.update",
            instance=instance,
            chat_id=item.get("remoteJid"),
            raw=item
        ))
    return out

def normalize_chats_upsert(body: Dict[str, Any]) -> List[InternalChatUpdate]:
    out: List[InternalChatUpdate] = []
    instance = body.get("instance")
    for item in _ensure_list(body.get("data")):
        out.append(InternalChatUpdate(
            event="chats.upsert",
            instance=instance,
            chat_id=item.get("id"),
            name=item.get("name"),
            raw=item
        ))
    return out

# ---------- ROTEADOR ----------
NORMALIZERS = {
    "messages.upsert": normalize_messages_upsert,
    "contacts.update": normalize_contacts_update,
    "presence.update": normalize_presence_update,
    "chats.update": normalize_chats_update,
    "chats.upsert": normalize_chats_upsert,
}

def normalized_event_name(body: Dict[str, Any], path_tail: Optional[str]) -> str:
    # preferir body["event"]; cair para path_tail se necessário
    evt = body.get("event")
    if isinstance(evt, str):
        return evt
    # logs mostram "Event path (raw tail): presence-update" => já mapeado pelo seu router
    if path_tail:
        return path_tail.replace("-", ".")
    return "unknown"

async def process_internal_events(events: List[BaseModel]):
    """
    AQUI você integra com:
    - fila (e.g., RabbitMQ, Kafka)
    - persistência (PostgreSQL)
    - handlers por tipo
    """
    for e in events:
        # exemplo de roteamento mínimo
        if isinstance(e, InternalMessage):
            # salvar mensagem, indexar mídia citada se houver
            pass
        elif isinstance(e, InternalContactUpdate):
            # upsert contato
            pass
        elif isinstance(e, InternalPresenceUpdate):
            # atualizar presença/última atividade
            pass
        elif isinstance(e, InternalChatUpdate):
            # upsert chat
            pass

# um único endpoint “flex” para /webhook/* (como você já usa)
@app.post("/webhook/{path_tail}")
async def webhook_flex(path_tail: str, request: Request, apikey: Optional[str] = Header(None)):
    body = await request.json()
    # (opcional) validar apikey
    # if apikey != "seu-token-aqui": raise HTTPException(401, "unauthorized")

    event = normalized_event_name(body, path_tail)
    normalizer = NORMALIZERS.get(event)
    if not normalizer:
        return {"ok": True, "ignored_event": event}

    internal_events = normalizer(body)
    await process_internal_events(internal_events)

    # útil para auditoria/observabilidade
    return {
        "ok": True,
        "event": event,
        "received": len(_ensure_list(body.get("data")) or [body.get("data")]),
        "emitted": len(internal_events),
        "ts": datetime.utcnow().isoformat() + "Z"
    }
