# codigos/coopervere/services/evolution_api.py
from __future__ import annotations

import os
import re
import json
from typing import Any, Dict, Optional

import requests
from requests import Response
from dotenv import load_dotenv

load_dotenv()


class EvolutionAPIError(RuntimeError):
    """Erro de alto nível para respostas não-OK da Evolution API."""
    def __init__(self, message: str, status_code: Optional[int] = None, payload: Optional[dict] = None):
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload or {}


def _normalize_number(raw: str) -> str:
    """
    Normaliza números de telefone:
    - Mantém apenas dígitos (ex.: 5541999999999)
    - Aceita formato JID (<número>@s.whatsapp.net)
    """
    if "@" in raw:
        return raw.strip()
    digits = re.sub(r"\D+", "", raw or "")
    if not digits:
        raise ValueError("Número de destino inválido ou vazio.")
    return digits


class EvolutionAPI:
    """
    Cliente para Evolution API, conforme exemplo oficial.

    .env necessários:
        EVO_BASE_URL   -> ex.: https://meu-servidor.com
        EVO_APIKEY     -> apikey entregue pelo Evolution
        EVO_INSTANCE   -> nome/id da instância (ex.: whats_ricardopart)
    """

    def __init__(self) -> None:
        self.base_url = os.getenv("EVO_BASE_URL", "http://localhost:8080").rstrip("/")
        self.apikey = os.getenv("EVO_APIKEY")
        self.instance = os.getenv("EVO_INSTANCE")
        self.timeout = int(os.getenv("EVO_TIMEOUT_S", "30"))

        if not self.apikey:
            raise RuntimeError("EVO_APIKEY não definido no .env")
        if not self.instance:
            raise RuntimeError("EVO_INSTANCE não definido no .env")

        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "apikey": self.apikey,
        })

    # ==========================================================
    # Envio de mensagens
    # ==========================================================
    def send_text(
        self,
        phone: str,
        text: str,
        *,
        delay: int = 1000,
        link_preview: bool = False,
        mentions_everyone: bool = False,
        mentioned: Optional[list[str]] = None,
        quoted_id: Optional[str] = None,
        quoted_text: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Envia uma mensagem de texto conforme exemplo oficial da Evolution API.

        Endpoint:
            POST /message/sendText/{instance}

        Exemplo de payload:
            {
                "number": "<string>",
                "text": "<string>",
                "delay": 123,
                "linkPreview": True,
                "mentionsEveryOne": True,
                "mentioned": ["{{remoteJID}}"],
                "quoted": {
                    "key": { "id": "<string>" },
                    "message": { "conversation": "<string>" }
                }
            }
        """

        number = _normalize_number(phone)
        payload: Dict[str, Any] = {
            "number": number,
            "text": text,
            "delay": delay,
            "linkPreview": link_preview,
            "mentionsEveryOne": mentions_everyone,
        }

        if mentioned:
            payload["mentioned"] = mentioned

        if quoted_id or quoted_text:
            payload["quoted"] = {
                "key": {"id": quoted_id or ""},
                "message": {"conversation": quoted_text or ""},
            }

        url = f"{self.base_url}/message/sendText/{self.instance}"

        resp = self.session.post(url, json=payload, timeout=self.timeout)
        return self._handle_response(resp)
    
    # ==========================================================
    # Envio de mídia (imagem, vídeo ou documento)
    # ==========================================================

    def send_media(
        self,
        phone: str,
        mediatype: str,
        mimetype: str,
        caption: str,
        media: str,
        file_name: str,
        *,
        delay: int = 1000,
        link_preview: bool = False,
        mentions_everyone: bool = False,
        mentioned: Optional[list[str]] = None,
        quoted_id: Optional[str] = None,
        quoted_text: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Envia mídia (imagem, vídeo ou documento) via Evolution API.

        Exemplo:
            send_media(
                phone="5541999999999",
                mediatype="document",
                mimetype="application/pdf",
                caption="Relatório semanal",
                media="<base64 ou URL>",
                file_name="relatorio.pdf"
            )
        """

        number = _normalize_number(phone)
        payload: Dict[str, Any] = {
            "number": number,
            "mediatype": mediatype,
            "mimetype": mimetype,
            "caption": caption,
            "media": media,
            "fileName": file_name,
            "delay": delay,
            "linkPreview": link_preview,
            "mentionsEveryOne": mentions_everyone,
        }

        if mentioned:
            payload["mentioned"] = mentioned

        if quoted_id or quoted_text:
            payload["quoted"] = {
                "key": {"id": quoted_id or ""},
                "message": {"conversation": quoted_text or ""},
            }

        url = f"{self.base_url}/message/sendMedia/{self.instance}"
        resp = self.session.post(url, json=payload, timeout=self.timeout)
        return self._handle_response(resp)



    # ==========================================================
    # Verificação de status (opcional)
    # ==========================================================
    def health(self) -> Dict[str, Any]:
        """Verifica se o servidor Evolution está respondendo."""
        url = f"{self.base_url}/health"
        resp = self.session.get(url, timeout=self.timeout)
        return self._handle_response(resp)

    # ==========================================================
    # Tratamento de resposta
    # ==========================================================
    def _handle_response(self, resp: Response) -> Dict[str, Any]:
        try:
            resp.raise_for_status()
        except requests.HTTPError as http_err:
            try:
                payload = resp.json()
            except Exception:
                payload = {"text": resp.text}
            raise EvolutionAPIError(
                f"Evolution API HTTP {resp.status_code}",
                status_code=resp.status_code,
                payload=payload,
            ) from http_err

        try:
            return resp.json()
        except ValueError:
            return {"raw": resp.text}


# ==========================================================
# Execução direta para teste rápido
# ==========================================================
if __name__ == "__main__":
    api = EvolutionAPI()

    try:
        print("[Health check]", api.health())
    except Exception as e:
        print("[Health ERROR]", e)

    # Teste de envio simples
    print(api.send_text("5546999111465", "Mensagem de teste via Evolution API ✅"))
