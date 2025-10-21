# main.py
import asyncio
import json
import logging
from websockets import serve
from websockets.exceptions import ConnectionClosed
from dotenv import load_dotenv

from core.utils.redis_client import init_redis_pool
from core.state.memory_storage import MemoryStorage
from core.managers.connection_manager import ConnectionManager
from core.managers.routing_manager import RoutingManager
from core.managers.psp_dispatcher import PSP_Dispatcher
from core.managers.handshake_manager import HandshakeManager
from core.managers.flow_control_manager import FlowControlManager
from core.managers.error_handler_manager import ErrorHandlerManager
from core.managers.config_manager import ConfigManager

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] (RC) %(message)s"
)
load_dotenv()


class CoreServer:
    def __init__(self):
        # --- ИЗМЕНЕНИЕ: Конструктор теперь почти пустой. ---
        # Вся инициализация переезжает в асинхронный метод.
        self.storage = MemoryStorage()
        logging.info("CoreServer 'скелет' создан. Ожидание инициализации менеджеров.")

    async def initialize_managers(self):
        """
        Инициализирует все менеджеры в правильном порядке ПОСЛЕ подключения к Redis.
        """
        # --- ИЗМЕНЕНИЕ: Четкая последовательность создания ---

        # 1. Сначала создаем менеджеры, от которых зависят другие
        self.routing_manager = RoutingManager(self.storage)
        self.config_manager = ConfigManager()
        await self.config_manager.sync_on_startup()
        self.flow_control_manager = FlowControlManager(
            self.config_manager, self.routing_manager
        )

        # 2. Теперь создаем ConnectionManager, передавая ему нужную ссылку
        self.connection_manager = ConnectionManager(
            self.storage, self.flow_control_manager
        )

        # 3. Создаем остальные менеджеры, которые зависят от предыдущих
        try:
            with open("secrets_vault.json", "r") as f:
                secrets_vault = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            logger.warning(
                "Файл 'secrets_vault.json' не найден или поврежден. Работа без секретов."
            )
            secrets_vault = {}

        self.error_handler_manager = ErrorHandlerManager(self.flow_control_manager)

        self.dispatcher = PSP_Dispatcher(
            self.connection_manager,
            self.routing_manager,
            self.flow_control_manager,
            self.error_handler_manager,
        )

        self.handshake_manager = HandshakeManager(self.config_manager, self.dispatcher)
        self.handshake_manager.load_secrets_vault(secrets_vault)

        self.config_manager.set_manager_references(
            self.handshake_manager, self.flow_control_manager
        )
        logging.info("RefferentCOREServer и все менеджеры успешно инициализированы.")

    async def handler(self, websocket):
        await self.connection_manager.handle_new_connection(websocket)
        try:
            async for message in websocket:
                await self.dispatcher.dispatch(websocket, message)
        except ConnectionClosed:
            pass
        finally:
            await self.connection_manager.handle_disconnect(websocket)

    async def start(self):
        # --- ИЗМЕНЕНИЕ: Четкая и правильная последовательность ---
        # 1. Сначала подключаемся к Redis.
        await init_redis_pool()
        # 2. Только потом создаем все остальные объекты.
        await self.initialize_managers()

        port = 8080
        host = "0.0.0.0"
        logging.info(f"Запуск RefferentCOREServer на {host}:{port}")

        config_listener_task = asyncio.create_task(
            self.config_manager.listen_for_updates()
        )
        connection_manager_task = asyncio.create_task(
            self.handshake_manager.manage_all_connections()
        )

        async with serve(self.handler, host, port, max_size=None):
            await asyncio.gather(config_listener_task, connection_manager_task)


async def main():
    server = CoreServer()
    await server.start()


if __name__ == "__main__":
    asyncio.run(main())
