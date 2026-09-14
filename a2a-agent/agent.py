"""A public A2A research-response agent for Internet Court workflows."""

import asyncio

from starlette.applications import Starlette

from a2a.helpers.proto_helpers import new_text_status_update_event
from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.events.event_queue_v2 import EventQueue
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.routes import create_agent_card_routes, create_jsonrpc_routes
from a2a.server.tasks import InMemoryTaskStore
from a2a.server.tasks.task_manager import TaskManager
from a2a.types.a2a_pb2 import (
    AgentCapabilities,
    AgentCard,
    AgentInterface,
    AgentSkill,
    TaskState,
    Task,
)

LOCAL_BASE_URL = "https://laughing-fishstick-76p9gj5gx9p3pg66-8000.app.github.dev"

agent_card = AgentCard(
    name="My Internet Court Agent",
    description=(
        "A public A2A agent that returns short research-style responses "
        "to supplied text. It does not access wallets, payment rails, or "
        "external counterparties."
    ),
    supported_interfaces=[
        AgentInterface(
            url=LOCAL_BASE_URL,
            protocol_binding="JSONRPC",
            protocol_version="1.0",
        )
    ],
    version="0.1.0",
    capabilities=AgentCapabilities(
        streaming=True,
        push_notifications=False,
    ),
    default_input_modes=["text/plain"],
    default_output_modes=["text/plain"],
    skills=[
        AgentSkill(
            id="local-research-response",
            name="Local research response",
            description="Returns a short research-style response to supplied text.",
            tags=["research", "local", "safe"],
            examples=["Summarize why local testing is useful."],
            input_modes=["text/plain"],
            output_modes=["text/plain"],
        )
    ],
)


class LocalResearchExecutor(AgentExecutor):
    async def execute(
        self,
        context: RequestContext,
        event_queue: EventQueue,
    ) -> None:
        task_id = context.task_id
        context_id = context.context_id

        if not task_id or not context_id:
            raise ValueError("A2A task and context identifiers are required.")

        try:
            request_text = context.get_user_input().strip()

            if not request_text:
                raise ValueError(
                    "Please provide text for the local research response."
                )

            task = Task(
                id=task_id,
                context_id=context_id,
            )

            await event_queue.enqueue_event(task)

            response = (
                "Research note: the submitted topic is "
                f"'{request_text}'. A useful next step is to define the claim, "
                "identify local evidence to examine, and record the result with "
                "its assumptions."
            )

            await event_queue.enqueue_event(
                new_text_status_update_event(
                    task_id,
                    context_id,
                    TaskState.TASK_STATE_COMPLETED,
                    response,
                )
            )

        except asyncio.CancelledError:
            raise

        except Exception as error:
            await event_queue.enqueue_event(
                new_text_status_update_event(
                    task_id,
                    context_id,
                    TaskState.TASK_STATE_FAILED,
                    f"Local request failed: {error}",
                )
            )

    async def cancel(
        self,
        context: RequestContext,
        event_queue: EventQueue,
    ) -> None:
        task_id = context.task_id
        context_id = context.context_id

        if task_id and context_id:
            await event_queue.enqueue_event(
                new_text_status_update_event(
                    task_id,
                    context_id,
                    TaskState.TASK_STATE_CANCELED,
                    "The local research task was canceled.",
                )
            )


agent_executor = LocalResearchExecutor()

task_store = InMemoryTaskStore()

request_handler = DefaultRequestHandler(
    agent_executor=agent_executor,
    task_store=task_store,
    agent_card=agent_card,
)

app = Starlette(
    routes=(
        create_agent_card_routes(
            agent_card,
            card_url="/.well-known/agent.json",
        )
        + create_jsonrpc_routes(
            request_handler,
            rpc_url="/",
        )
    )
)


if __name__ == "__main__":
    raise SystemExit(
        "Run this application with an ASGI server such as uvicorn."
    )
