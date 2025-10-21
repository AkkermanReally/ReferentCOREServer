# core/managers/handshake_manager.py
import asyncio
import json
import logging
from typing import Dict, Any
import websockets
from websockets.exceptions import ConnectionClosed
import uuid

from .config_manager import ConfigManager
from .psp_dispatcher import PSP_Dispatcher
from ..models.psp import Envelope

logger = logging.getLogger(__name__)


class HandshakeManager:
    """
    Динамически управляет жизненным циклом соединений с компонентами.
    Запускает, останавливает и перезапускает соединения на основе 'живой'
    конфигурации из ConfigManager.
    """

    def __init__(self, config_manager: ConfigManager, dispatcher: PSP_Dispatcher):
        self.config_manager = config_manager
        self.dispatcher = dispatcher
        self.secrets_vault = {}
        # Словарь для отслеживания активных задач по управлению соединениями
        self.connection_tasks: Dict[str, asyncio.Task] = {}
        logger.info("HandshakeManager (dynamic) инициализирован.")

    def load_secrets_vault(self, secrets: Dict):
        """Загружает хранилище секретов, необходимое для хендшейка."""
        self.secrets_vault = secrets
        logger.info(f"Хранилище секретов загружено в HandshakeManager.")

    async def manage_all_connections(self):
        """
        Главная точка входа. Запускает задачи для всех включенных
        компонентов при старте RCs.
        """
        all_components = self.config_manager.get_all_components()
        for name, config in all_components.items():
            if config.get("disabled") == "false":
                self._start_connection_task_for(name)

        # Эта корутина просто ждет вечно. Вся работа происходит в фоновых задачах.
        await asyncio.Future()

    async def update_component_state(self, name: str, new_config: Dict):
        """
        Публичный метод, вызываемый ConfigManager'ом при изменении статуса
        компонента (включен/выключен).
        """
        is_enabled = new_config.get("disabled") == "false"
        task_exists = name in self.connection_tasks

        if is_enabled and not task_exists:
            logger.info(
                f"Компонент '{name}' был включен. Запуск задачи на подключение."
            )
            self._start_connection_task_for(name)
        elif not is_enabled and task_exists:
            logger.info(
                f"Компонент '{name}' был выключен. Остановка задачи на подключение."
            )
            await self._stop_connection_task_for(name)

    def _start_connection_task_for(self, name: str):
        """Создает и запускает фоновую задачу для управления одним соединением."""
        if name in self.connection_tasks:
            return

        task = asyncio.create_task(self._manage_connection_loop(name))
        self.connection_tasks[name] = task
        # Убираем задачу из словаря, когда она завершается (например, при отмене)
        task.add_done_callback(lambda _: self.connection_tasks.pop(name, None))

    async def _stop_connection_task_for(self, name: str):
        """Безопасно останавливает и отменяет работающую задачу соединения."""
        if name in self.connection_tasks:
            task = self.connection_tasks[name]
            task.cancel()
            try:
                await task  # Ожидаем, пока задача обработает отмену
            except asyncio.CancelledError:
                logger.info(f"Задача на подключение для '{name}' успешно отменена.")

    async def _manage_connection_loop(self, component_name: str):
        """
        Бесконечный цикл, который пытается подключиться к компоненту,
        выполнить хендшейк и слушать сообщения. Работает, пока его не отменят.
        """
        while True:
            try:
                # На каждой итерации получаем свежий конфиг
                config = self.config_manager.get_component_config(component_name)
                if not config or config.get("disabled") == "true":
                    logger.warning(
                        f"Цикл подключения для '{component_name}' остановлен, т.к. компонент выключен."
                    )
                    break

                uri = config.get("uri")
                auth_token = config.get("auth_token")

                async with websockets.connect(uri) as websocket:
                    logger.info(f"[{component_name}] Соединение установлено.")
                    is_successful = await self._perform_handshake(
                        websocket, component_name, auth_token
                    )

                    if is_successful:
                        logger.info(
                            f"[{component_name}] Рукопожатие успешно. Прослушивание сообщений..."
                        )
                        async for message in websocket:
                            await self.dispatcher.dispatch(websocket, message)

            except asyncio.CancelledError:
                logger.info(f"Цикл подключения для '{component_name}' отменяется.")
                break
            except (ConnectionRefusedError, ConnectionClosed, OSError) as e:
                logger.warning(
                    f"[{component_name}] Ошибка соединения: {type(e).__name__}. Повторная попытка через 10 секунд."
                )
            except Exception as e:
                logger.error(
                    f"[{component_name}] Неожиданная ошибка в цикле подключения: {e}",
                    exc_info=True,
                )

            await asyncio.sleep(10)
        logger.info(f"Цикл управления соединением для '{component_name}' завершен.")

    async def _perform_handshake(self, websocket, component_name, auth_token) -> bool:
        # Эта логика остается такой же, как и была, она не зависит от новой архитектуры
        try:
            handshake_msg = Envelope(
                type="handshake",
                return_from="RefferentCORE",
                payload={"auth_token": auth_token},
            )
            await websocket.send(handshake_msg.model_dump_json(by_alias=True))
            response_raw = await websocket.recv()
            response = Envelope.model_validate(json.loads(response_raw))

            if response.type == "secrets":
                required = response.payload.get("required_secrets", [])
                secrets_to_send = {
                    key: self.secrets_vault.get(key)
                    for key in required
                    if key in self.secrets_vault
                }
                secrets_msg = Envelope(
                    type="secrets",
                    return_from="RefferentCORE",
                    request_id=response.request_id,
                    payload=secrets_to_send,
                )
                await websocket.send(secrets_msg.model_dump_json(by_alias=True))
                response_raw = await websocket.recv()
                response = Envelope.model_validate(json.loads(response_raw))

            if response.type == "handshake_confirmed":
                await self.dispatcher.dispatch(websocket, response_raw)
                return True
            return False
        except Exception as e:
            logger.error(
                f"[{component_name}] Ошибка во время рукопожатия: {e}", exc_info=True
            )
            return False
