# core/managers/flow_control_manager.py
import asyncio
import logging
from collections import deque
from typing import Dict, Any

from ..models.psp import Envelope
from .routing_manager import RoutingManager

# Удаляем импорты, связанные со статической конфигурацией
from .config_manager import ConfigManager  # <--- НОВАЯ ЗАВИСИМОСТЬ

logger = logging.getLogger(__name__)


class FlowControlManager:
    """
    Управляет потоком запросов к компонентам, получая лимиты
    от ConfigManager и обновляя их в реальном времени.
    """

    def __init__(self, config_manager: ConfigManager, routing_manager: RoutingManager):
        # --- ИЗМЕНЕНИЕ: Зависимость от ConfigManager, а не от словаря ---
        self.config_manager = config_manager
        self.routing_manager = routing_manager

        self.queues: Dict[str, deque] = {}
        self.active_requests: Dict[str, int] = {}
        self.limits: Dict[str, float] = {}  # Используем float для поддержки 'inf'
        self.paused_queues: Dict[str, bool] = {}
        self.in_flight_requests: Dict[str, Envelope] = {}

        self._initialize_from_live_config()
        logger.info(
            "FlowControlManager инициализирован и синхронизирован с ConfigManager."
        )

    def _initialize_from_live_config(self):
        """Инициализирует очереди и лимиты на основе 'живой' конфигурации от ConfigManager."""
        all_components = self.config_manager.get_all_components()
        for name, config in all_components.items():
            self.queues[name] = deque()
            self.active_requests[name] = 0
            self.paused_queues[name] = False

            # Устанавливаем начальные лимиты
            limit_str = config.get("max_concurrent_requests")
            self._set_limit_for(name, limit_str)

        logger.info(f"Начальные лимиты FlowControlManager: {self.limits}")

    def _set_limit_for(self, component_name: str, limit_str: str | None):
        """Внутренний метод для безопасной установки лимита."""
        if limit_str and limit_str.isdigit():
            self.limits[component_name] = int(limit_str)
        else:
            # Если значение отсутствует, не является числом или равно None, лимит - бесконечность
            self.limits[component_name] = float("inf")

    def update_limits(self, component_name: str, new_limit_str: str | None):
        """
        Публичный метод для 'горячего' обновления лимитов.
        Вызывается из ConfigManager.
        """
        old_limit = self.limits.get(component_name)
        self._set_limit_for(component_name, new_limit_str)
        new_limit = self.limits.get(component_name)

        logger.info(
            f"Лимит для '{component_name}' обновлен: {old_limit} -> {new_limit}."
        )

        # Важно: после обновления лимита (особенно если он увеличился),
        # нужно немедленно попытаться обработать очередь.
        asyncio.create_task(self._process_queue(component_name))

    async def enqueue(self, envelope: Envelope):
        """Добавляет входящий запрос в очередь и запускает обработку."""
        target = envelope.return_to
        if target in self.queues:
            self.queues[target].append(envelope)
            logger.info(
                f"Запрос {envelope.request_id} добавлен в очередь для '{target}'. Размер очереди: {len(self.queues[target])}"
            )
            asyncio.create_task(self._process_queue(target))
        else:
            logger.warning(
                f"Получен запрос для компонента '{target}', для которого не инициализирована очередь."
            )

    async def _process_queue(self, component_name: str):
        """Обрабатывает очередь для указанного компонента, если есть свободные слоты."""
        if self.paused_queues.get(component_name):
            return

        # --- ИЗМЕНЕНИЕ: Сравнение идет с self.limits, который теперь 'живой' ---
        while self.active_requests.get(component_name, 0) < self.limits.get(
            component_name, float("inf")
        ) and self.queues.get(component_name):
            envelope = self.queues[component_name].popleft()
            self.active_requests[component_name] += 1
            self.in_flight_requests[envelope.request_id] = envelope

            logger.info(
                f"Отправка запроса {envelope.request_id} к '{component_name}'. Активных: {self.active_requests[component_name]}/{self.limits[component_name]}"
            )
            # --- ИЗМЕНЕНИЕ: Используем routing_manager для отправки ---
            await self.routing_manager.route(envelope)

    async def handle_completion(self, envelope: Envelope):
        """
        Обрабатывает завершение запроса (успешный ответ или ошибка).
        Освобождает слот и запускает обработку очереди.
        """
        request_id = envelope.request_id
        if request_id not in self.in_flight_requests:
            return

        original_request = self.in_flight_requests.pop(request_id)
        component_name = original_request.return_to

        if self.active_requests.get(component_name, 0) > 0:
            self.active_requests[component_name] -= 1

        logger.info(
            f"Запрос {request_id} для '{component_name}' завершен. Активных: {self.active_requests.get(component_name, 'N/A')}/{self.limits.get(component_name, 'N/A')}"
        )
        asyncio.create_task(self._process_queue(component_name))

    # Методы requeue_and_pause и _resume_after_delay остаются без изменений
    async def requeue_and_pause(
        self, request_id: str, component_name: str, cooldown: int
    ):
        original_request = self.in_flight_requests.get(request_id)
        # Этот метод вызывается ДО handle_completion для ошибки, поэтому сначала нужно "вернуть" слот
        if original_request:
            if self.active_requests.get(component_name, 0) > 0:
                self.active_requests[component_name] -= 1
            # Возвращаем в начало очереди и удаляем из "улетевших"
            self.queues[component_name].appendleft(original_request)
            del self.in_flight_requests[request_id]
            logger.info(
                f"Запрос {request_id} возвращен в очередь для '{component_name}'."
            )

        self.paused_queues[component_name] = True
        logger.warning(
            f"Очередь для '{component_name}' поставлена на паузу на {cooldown} секунд."
        )
        asyncio.create_task(self._resume_after_delay(component_name, cooldown))

    async def _resume_after_delay(self, component_name: str, delay: int):
        await asyncio.sleep(delay)
        self.paused_queues[component_name] = False
        logger.info(
            f"Пауза снята с очереди для '{component_name}'. Возобновление обработки."
        )
        asyncio.create_task(self._process_queue(component_name))

    async def handle_component_disconnect(self, component_name: str):
        """
        Вызывается из ConnectionManager, когда компонент отключается.
        Находит все "зависшие" запросы к этому компоненту и возвращает
        на них ошибку.
        """
        # Создаем копию ключей, так как словарь будет меняться во время итерации
        in_flight_ids = list(self.in_flight_requests.keys())

        for request_id in in_flight_ids:
            request_envelope = self.in_flight_requests.get(request_id)
            if request_envelope and request_envelope.return_to == component_name:
                logger.error(
                    f"Проваливаем запрос {request_id}, т.к. компонент '{component_name}' отключился."
                )

                # Имитируем получение ошибки от компонента
                error_payload = {
                    "error_message": f"Component '{component_name}' disconnected during processing.",
                    "error_type": "ConnectionLostError",
                }
                error_envelope = Envelope(
                    session_id=request_envelope.session_id,
                    type="error",
                    return_from=component_name,  # Притворяемся, что ответил компонент
                    return_to=request_envelope.return_from,  # Адресуем оригинальному клиенту
                    request_id=request_id,
                    payload=error_payload,
                )

                # Обрабатываем "завершение" этого запроса, чтобы освободить слот
                await self.handle_completion(error_envelope)
                # И отправляем ошибку клиенту
                await self.routing_manager.route(error_envelope)
