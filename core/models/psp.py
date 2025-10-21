# core/models/psp.py
from pydantic import BaseModel, Field
from typing import Any, Dict, List, Literal, Optional
from datetime import datetime, timezone
import uuid


def new_message_id():
    return f"msg_{uuid.uuid4()}"


class Envelope(BaseModel):
    """
    Определяет структуру "конверта" протокола PSP v1.0.
    """

    psp_version: str = "1.0"
    message_id: str = Field(default_factory=new_message_id)
    session_id: Optional[str] = (
        None  # Может быть не у всех сообщений (например, регистрация)
    )
    type: Literal[
        "command",
        "event",
        "response",
        "handshake",
        "secrets",
        "error",
        "handshake_confirmed",
        "router_registration",  # Добавляем служебные типы
        "module_registration",
        "error_manifest_registration",
    ]
    return_from: str
    return_to: Optional[str] = None
    request_id: Optional[str] = None
    trigger_event_id: Optional[str] = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    payload: Dict[str, Any]
