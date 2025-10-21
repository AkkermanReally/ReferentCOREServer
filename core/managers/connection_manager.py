# core/managers/connection_manager.py
import logging
from typing import Any, TYPE_CHECKING

from ..state.base_storage import BaseStorage
from ..utils.redis_client import get_redis_client
from ..models.psp import Envelope  # <-- Добавим на всякий случай, если понадобится

if TYPE_CHECKING:
    from .flow_control_manager import FlowControlManager

logger = logging.getLogger(__name__)


class ConnectionManager:
    """Управляет жизненным циклом WebSocket-соединений."""

    def __init__(
        self, storage: BaseStorage, flow_control_manager_ref: "FlowControlManager"
    ):
        self.storage = storage
        self.redis = get_redis_client()
        self.flow_control_manager_ref = flow_control_manager_ref

        if not self.redis:
            logger.error(
                "КРИТИЧЕСКАЯ ОШИБКА: Redis клиент не доступен в ConnectionManager!"
            )

    # --- ВОССТАНОВЛЕННЫЙ МЕТОД ---
    async def handle_new_connection(self, websocket: Any):
        """Обрабатывает новое, еще не идентифицированное подключение."""
        pass

    # --- ВОССТАНОВЛЕННЫЙ МЕТОД (который вызывал ошибку) ---
    async def handle_module_registration(self, websocket: Any, module_name: str):
        """Регистрирует идентифицированный модуль."""
        await self.storage.register_module(module_name, websocket)
        await self.redis.sadd("system:active_connections", module_name)

    # --- Наш обновленный метод (остается без изменений) ---
    async def handle_disconnect(self, websocket: Any):
        """Обрабатывает отключение модуля."""
        module_name = await self.storage.get_module_name(websocket)
        if module_name:
            await self.storage.unregister_module(module_name)
            await self.redis.srem("system:active_connections", module_name)

            logger.warning(
                f"Компонент '{module_name}' отключился. Проваливаем все ожидающие запросы."
            )
            await self.flow_control_manager_ref.handle_component_disconnect(module_name)

        logger.info(f"Соединение с '{module_name or 'Неизвестный'}' разорвано.")
