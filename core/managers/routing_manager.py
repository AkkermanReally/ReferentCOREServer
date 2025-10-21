# core/managers/routing_manager.py
import logging
from ..models.psp import Envelope

# ИЗМЕНЕНИЕ: Импортируем абстракцию, а не реализацию
from ..state.base_storage import BaseStorage


logger = logging.getLogger(__name__)


class RoutingManager:
    """Маршрутизирует сообщения типа 'command' и 'response'."""

    # ИЗМЕНЕНИЕ: Указываем в зависимостях базовый класс
    def __init__(self, storage: BaseStorage):
        self.storage = storage

    async def route(self, envelope: Envelope):
        target_name = envelope.return_to
        if not target_name:
            logger.warning(
                f"Невозможно маршрутизировать сообщение {envelope.message_id}: отсутствует 'return_to'."
            )
            return

        target_ws = await self.storage.get_websocket(target_name)
        if target_ws:
            try:
                await target_ws.send(envelope.model_dump_json())
                logger.info(
                    f"Сообщение {envelope.message_id} от '{envelope.return_from}' успешно отправлено к '{target_name}'."
                )
            except Exception as e:
                logger.error(f"Ошибка при отправке сообщения к '{target_name}': {e}")
        else:
            logger.warning(
                f"Не удалось отправить сообщение {envelope.message_id}: получатель '{target_name}' не найден."
            )
