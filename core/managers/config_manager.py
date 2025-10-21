# core/managers/config_manager.py
import asyncio
import json
import logging
from typing import Dict, Any

from ..utils.redis_client import get_redis_client

logger = logging.getLogger(__name__)


class ConfigManager:
    """
    Управляет динамической конфигурацией компонентов, используя Redis как источник правды.
    Является центральным узлом для получения и обновления операционных параметров.
    """

    def __init__(self):
        self.redis = get_redis_client()
        # Внутренний кэш конфигураций для быстрого доступа
        self.configs: Dict[str, Dict] = {}

        # Ссылки на другие менеджеры для отправки им команд об обновлении.
        # Они будут установлены после инициализации всех объектов в main.py.
        self.handshake_manager_ref = None
        self.flow_control_manager_ref = None
        logger.info("ConfigManager инициализирован.")

    def set_manager_references(self, handshake_manager, flow_control_manager):
        """
        Внедряет зависимости (другие менеджеры) после их создания.
        Это позволяет избежать проблемы циклических импортов.
        """
        self.handshake_manager_ref = handshake_manager
        self.flow_control_manager_ref = flow_control_manager

    async def sync_on_startup(self):
        """
        Выполняет первоначальную синхронизацию.
        Читает bootstrap-файл. Если для компонента нет записи в Redis,
        СОЗДАЕТ ЕЕ, КОПИРУЯ ВСЕ ДАННЫЕ ИЗ ФАЙЛА.
        Если запись уже есть, файл игнорируется.
        """
        logger.info("Запуск первоначальной синхронизации конфигурации с Redis...")
        try:
            with open("remote_components.json", "r") as f:
                bootstrap_configs = json.load(f)
        except FileNotFoundError:
            logger.error(
                "КРИТИЧЕСКАЯ ОШИБКА: Файл 'remote_components.json' не найден! Завершение работы."
            )
            exit(1)

        for name, data in bootstrap_configs.items():
            key = f"component:{name}"

            # --- ИЗМЕНЕНИЕ: Новая, более умная логика инициализации ---
            if not await self.redis.exists(key):
                logger.warning(
                    f"Конфигурация для '{name}' не найдена в Redis. Создание из remote_components.json..."
                )

                # Преобразуем все значения в строки, так как Redis HSET работает с ними
                initial_config = {k: str(v) for k, v in data.items()}

                # Гарантируем наличие обязательных полей, если их вдруг нет в файле
                if "disabled" not in initial_config:
                    initial_config["disabled"] = "true"
                if "max_concurrent_requests" not in initial_config:
                    initial_config["max_concurrent_requests"] = "5"

                await self.redis.hset(key, mapping=initial_config)

            # Загружаем актуальную конфигурацию из Redis в память (в кэш)
            self.configs[name] = await self.redis.hgetall(key)

        logger.info(
            f"Синхронизация завершена. Загружено {len(self.configs)} конфигураций."
        )

    def get_all_components(self) -> Dict[str, Dict]:
        """Возвращает кэш всех конфигураций."""
        return self.configs

    def get_component_config(self, name: str) -> Dict | None:
        """Возвращает кэш конфигурации для одного компонента."""
        return self.configs.get(name)

    async def listen_for_updates(self):
        """
        Фоновая задача, которая подписывается на канал Redis и слушает
        уведомления об обновлении конфигураций для их 'горячей' перезагрузки.
        """
        pubsub = self.redis.pubsub()
        await pubsub.subscribe("system:config_updates")
        logger.info(
            "ConfigManager подписался на канал 'system:config_updates' для прослушивания обновлений."
        )

        while True:
            try:
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=60
                )
                if message and message.get("type") == "message":
                    component_name = message["data"]
                    logger.info(
                        f"Получено уведомление об обновлении конфигурации для '{component_name}'."
                    )
                    await self._reload_config_for(component_name)
            except Exception as e:
                logger.error(
                    f"Ошибка в прослушивателе обновлений конфигурации: {e}",
                    exc_info=True,
                )
                # В случае ошибки ждем и пытаемся переподписаться
                await asyncio.sleep(5)

    async def _reload_config_for(self, name: str):
        """
        Перезагружает конфигурацию для одного компонента из Redis,
        обновляет внутренний кэш и уведомляет зависимые менеджеры об изменениях.
        """
        if name not in self.configs:
            logger.warning(
                f"Получено обновление для неизвестного компонента '{name}'. Игнорируется."
            )
            return

        old_config = self.configs.get(name, {})
        new_config = await self.redis.hgetall(f"component:{name}")
        self.configs[name] = new_config

        logger.info(f"Конфигурация для '{name}' обновлена в памяти.")

        # === Уведомление других менеджеров ===

        # 1. FlowControlManager: обновить лимиты
        if old_config.get("max_concurrent_requests") != new_config.get(
            "max_concurrent_requests"
        ):
            new_limit = new_config.get("max_concurrent_requests")
            self.flow_control_manager_ref.update_limits(name, new_limit)

        # 2. HandshakeManager: проверить, не изменился ли статус (вкл/выкл)
        if old_config.get("disabled") != new_config.get("disabled"):
            await self.handshake_manager_ref.update_component_state(name, new_config)
