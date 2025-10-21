# RCs/core/state/base_storage.py
from abc import ABC, abstractmethod
from typing import Any


class BaseStorage(ABC):
    """
    Абстрактный базовый класс для всех реализаций хранилища состояния.
    Определяет контракт, которому должны следовать хранилища.
    """

    @abstractmethod
    async def register_module(self, module_name: str, websocket: Any):
        """Регистрирует новый модуль."""
        pass

    @abstractmethod
    async def unregister_module(self, module_name: str):
        """Удаляет модуль из реестра."""
        pass

    @abstractmethod
    async def get_websocket(self, module_name: str) -> Any | None:
        """Возвращает WebSocket-объект по имени модуля."""
        pass

    @abstractmethod
    async def get_module_name(self, websocket: Any) -> str | None:
        """Возвращает имя модуля по его WebSocket-объекту."""
        pass
