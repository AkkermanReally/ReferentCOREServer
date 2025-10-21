# core/managers/psp_dispatcher.py
import json
import logging
from typing import Any
from ..models.psp import Envelope
from .connection_manager import ConnectionManager
from .routing_manager import RoutingManager
from .flow_control_manager import FlowControlManager
from .error_handler_manager import ErrorHandlerManager

logger = logging.getLogger(__name__)


class PSP_Dispatcher:
    """Направляет входящие сообщения в соответствующие менеджеры."""

    def __init__(
        self,
        connection_manager: ConnectionManager,
        routing_manager: RoutingManager,
        flow_control_manager: FlowControlManager,
        error_handler_manager: ErrorHandlerManager,
    ):
        self.connection_manager = connection_manager
        self.routing_manager = routing_manager
        self.flow_control_manager = flow_control_manager
        self.error_handler_manager = error_handler_manager

    async def dispatch(self, websocket: Any, raw_message: str):
        try:
            data = json.loads(raw_message)
            envelope = Envelope.model_validate(data)

            # --- Новая логика маршрутизации ---

            if envelope.type == "command":
                await self.flow_control_manager.enqueue(envelope)

            elif envelope.type == "error":
                # Сначала ошибка идет в кризис-менеджер
                is_handled = await self.error_handler_manager.process_error(envelope)
                # Сообщаем FlowControl, что запрос завершился (даже если с ошибкой)
                await self.flow_control_manager.handle_completion(envelope)
                # Если ошибка НЕ была обработана (т.е. она фатальная), пробрасываем ее клиенту
                if not is_handled:
                    await self.routing_manager.route(envelope)

            elif envelope.type == "response":
                await self.flow_control_manager.handle_completion(envelope)
                await self.routing_manager.route(envelope)

            elif envelope.type == "error_manifest_registration":
                self.error_handler_manager.register_manifest(envelope.payload)

            elif envelope.type in ["handshake_confirmed", "module_registration"]:
                module_name = envelope.payload.get("entity") or envelope.payload.get(
                    "module"
                )
                if module_name:
                    await self.connection_manager.handle_module_registration(
                        websocket, module_name
                    )
            else:
                logger.info(
                    f"Получено сообщение типа '{envelope.type}', специальная обработка не требуется."
                )

        except Exception as e:
            logger.error(f"Критическая ошибка при диспетчеризации: {e}", exc_info=True)
