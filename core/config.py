# core/config.py

import os
import json
import logging
from pydantic_settings import BaseSettings
from pydantic import Field
from dotenv import load_dotenv
from typing import Optional, Dict, Any  # <-- Импортируем Optional

# Инициализируем логгер
logger = logging.getLogger(__name__)
load_dotenv()


class RemoteComponentConfig(BaseSettings):
    uri: str
    auth_token: str
    disabled: bool
    max_concurrent_requests: Optional[int] = None


class Settings(BaseSettings):
    rc_port: int = 8765
    remote_components: dict[str, RemoteComponentConfig] = Field(default_factory=dict)
    secrets_vault: dict[str, RemoteComponentConfig] = Field(default_factory=dict)

    def load_remote_components(self, path: Optional[str]):
        if path and os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    config_data = json.load(f)
                    for name, data in config_data.items():
                        self.remote_components[name] = RemoteComponentConfig(**data)
                logger.info(
                    f"Конфигурация удаленных компонентов успешно загружена из {path}"
                )
            except Exception as e:
                logger.error(
                    f"Не удалось загрузить или распарсить файл конфигурации {path}: {e}"
                )
        elif path:
            logger.warning(
                f"Файл конфигурации удаленных компонентов не найден по пути: {path}"
            )
        else:
            logger.info(
                "Путь к конфигурации удаленных компонентов не указан. Запуск без удаленных компонентов."
            )

    def load_secrets_vault(self, path: Optional[str]):
        """Загружает структурированные секреты из JSON-файла."""
        if path and os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    self.secrets_vault = json.load(f)
                logger.info(f"Сейф Секретов успешно загружен из {path}")
            except Exception as e:
                logger.error(
                    f"Не удалось загрузить или распарсить Сейф Секретов {path}: {e}"
                )
        else:
            logger.warning(
                "Путь к Сейфу Секретов не указан или файл не найден. Сервер не сможет предоставлять секреты компонентам."
            )


settings = Settings()

settings.load_remote_components(os.getenv("REMOTE_COMPONENTS_CONFIG_PATH"))

settings.load_secrets_vault(os.getenv("SECRETS_VAULT_PATH"))
