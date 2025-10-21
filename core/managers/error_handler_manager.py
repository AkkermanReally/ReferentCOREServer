# core/managers/error_handler_manager.py
import logging
from typing import Dict, Any

from ..models.psp import Envelope

# ВАЖНО: импортируем FlowControlManager для тайп-хинтинга и избежания циклического импорта
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .flow_control_manager import FlowControlManager

logger = logging.getLogger(__name__)


class ErrorHandlerManager:
    """
    Централизованно обрабатывает ошибки от компонентов, используя
    предварительно зарегистрированные "манифесты ошибок".
    """

    def __init__(self, flow_control_manager: "FlowControlManager"):
        self.flow_control_manager = flow_control_manager
        self.manifests: Dict[str, Dict] = {}  # component_name -> manifest
        logger.info("ErrorHandlerManager инициализирован.")

    def register_manifest(self, manifest: Dict[str, Any]):
        """Регистрирует манифест ошибок для компонента."""
        component_name = manifest.get("component_name")
        if component_name:
            self.manifests[component_name] = manifest.get("error_codes", {})
            logger.info(
                f"Манифест ошибок для '{component_name}' успешно зарегистрирован."
            )
        else:
            logger.warning("Попытка зарегистрировать манифест без имени компонента.")

    async def process_error(self, error_envelope: Envelope) -> bool:
        """
        Обрабатывает ошибку. Возвращает True, если ошибка была обработана
        (и не должна отправляться клиенту), и False в противном случае.
        """
        component_name = error_envelope.return_from
        error_code = error_envelope.payload.get("error_code")
        request_id = error_envelope.request_id

        if not component_name or not error_code:
            return False  # Не можем обработать, нет данных

        component_manifest = self.manifests.get(component_name)
        if not component_manifest or error_code not in component_manifest:
            logger.warning(
                f"Для '{component_name}' не найдено правило обработки ошибки '{error_code}'."
            )
            return False  # Нет правила, пробрасываем ошибку дальше

        rule = component_manifest[error_code]
        action = rule.get("action")

        logger.info(
            f"Для ошибки '{error_code}' от '{component_name}' найдено правило: '{action}'."
        )

        if action == "pause_and_retry":
            cooldown = rule.get("cooldown_seconds", 60)
            await self.flow_control_manager.requeue_and_pause(
                request_id, component_name, cooldown
            )
            return True  # Ошибка обработана, клиенту ничего не отправляем

        # Другие действия (retry_immediate, etc.) можно добавить здесь

        # Если действие не подразумевает скрытие ошибки от клиента
        return False
