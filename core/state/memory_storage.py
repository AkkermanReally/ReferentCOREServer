# core/state/memory_storage.py
import logging
from typing import Dict, Any

# ИЗМЕНЕНИЕ: Импортируем базовый класс
from .base_storage import BaseStorage

logger = logging.getLogger(__name__)


# ИЗМЕНЕНИЕ: Наследуемся от BaseStorage
class MemoryStorage(BaseStorage):
    """
    Простое In-Memory хранилище для состояния сервера.
    Хранит реестр подключенных модулей.
    """

    def __init__(self):
        self.connected_modules: Dict[str, Any] = {}
        logger.info("In-memory хранилище инициализировано.")

    async def register_module(self, module_name: str, websocket: Any):
        self.connected_modules[module_name] = websocket
        logger.info(f"Модуль '{module_name}' зарегистрирован в хранилище.")

    async def unregister_module(self, module_name: str):
        if module_name in self.connected_modules:
            del self.connected_modules[module_name]
            logger.info(f"Модуль '{module_name}' разрегистрирован из хранилища.")

    async def get_websocket(self, module_name: str) -> Any | None:
        return self.connected_modules.get(module_name)

    async def get_module_name(self, websocket: Any) -> str | None:
        # Это медленно, но для in-memory и отладки нормально
        for name, ws in self.connected_modules.items():
            if ws == websocket:
                return name
        return None
