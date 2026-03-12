import asyncio
from types import SimpleNamespace

from azure.ai.projects.aio import AIProjectClient
from azure.core.exceptions import ResourceExistsError
from agent_framework.azure import AzureAIAgentClient
from auth import get_azure_credential
try:
    # Older agent-framework builds expose Agent at top-level.
    from agent_framework import Agent
except ImportError:
    # Newer builds expose ChatAgent instead.
    from agent_framework import ChatAgent as Agent

from config import (
    AI_FOUNDRY_PROJECT_ENDPOINT,
    MODEL_DEPLOYMENT_NAME,
    CORE_ORDERING_AGENT_ID,
)
from agents.tools.core_ordering_tools import (
    run_planning_tools_readonly_query,
)
from agents.instructions import AGENT_INSTRUCTIONS


def _extract_obj_value(obj: object, key: str) -> object:
    if isinstance(obj, dict):
        return obj.get(key)
    getter = getattr(obj, "get", None)
    if callable(getter):
        try:
            return getter(key)
        except Exception:
            pass
    return getattr(obj, key, None)


async def _resolve_agent_id_if_needed(raw_agent_ref: str, credential: object) -> str:
    """
    Accept either an `asst_...` id or an agent name.
    If a name is provided, resolve it to id from Foundry project.
    """
    ref = (raw_agent_ref or "").strip()
    if ref.startswith("asst_"):
        return ref

    async with AIProjectClient(endpoint=AI_FOUNDRY_PROJECT_ENDPOINT, credential=credential) as project_client:
        candidates = []
        try:
            if hasattr(project_client.agents, "list"):
                pager = project_client.agents.list(limit=200)
                async for a in pager:
                    candidates.append(a)
            elif hasattr(project_client.agents, "list_agents"):
                pager = project_client.agents.list_agents(limit=200)
                async for a in pager:
                    candidates.append(a)
        except Exception:
            candidates = []

        # Try exact name match first.
        for a in candidates:
            name = _extract_obj_value(a, "name")
            aid = _extract_obj_value(a, "id")
            if str(name or "").strip() == ref and str(aid or "").startswith("asst_"):
                return str(aid)

        # Fallback to direct getter by provided ref.
        fetched = None
        try:
            if hasattr(project_client.agents, "get_agent"):
                fetched = await project_client.agents.get_agent(ref)
            else:
                fetched = await project_client.agents.get(ref)
        except Exception:
            fetched = None

        resolved = _extract_obj_value(fetched, "id") if fetched is not None else None
        if resolved and str(resolved).startswith("asst_"):
            return str(resolved)

        raise RuntimeError(
            "CORE_ORDERING_AGENT_ID must be an 'asst_' id (or a resolvable agent name). "
            f"Could not resolve '{ref}' to a valid assistant id."
        )


def _make_agent_definition(model: str, instructions: str) -> object:
    """
    Build an agent definition compatible with multiple azure-ai-projects SDK versions.
    """
    try:
        from azure.ai.projects.models import PromptAgentDefinition  # type: ignore
        return PromptAgentDefinition(model=model, instructions=instructions)
    except Exception:
        # Older/newer SDK variants may accept plain JSON body shape.
        return {
            "model": model,
            "instructions": instructions,
        }


def _patch_ai_projects_agents_compat() -> None:
    """
    Bridge API differences between azure-ai-projects SDK variants.
    Some agent_framework_azure_ai builds expect agents.{get_agent,create_agent,delete_agent}
    while newer azure-ai-projects exposes agents.{get,create,delete}.
    """
    try:
        from azure.ai.projects.aio.operations._operations import AgentsOperations
    except Exception:
        return

    if not hasattr(AgentsOperations, "get_agent") and hasattr(AgentsOperations, "get"):
        async def _get_agent(self, agent_name: str, **kwargs: object) -> object:
            return await self.get(agent_name, **kwargs)
        setattr(AgentsOperations, "get_agent", _get_agent)

    if not hasattr(AgentsOperations, "delete_agent") and hasattr(AgentsOperations, "delete"):
        async def _delete_agent(self, agent_name: str, **kwargs: object) -> object:
            return await self.delete(agent_name, **kwargs)
        setattr(AgentsOperations, "delete_agent", _delete_agent)

    if not hasattr(AgentsOperations, "create_agent") and hasattr(AgentsOperations, "create"):
        async def _create_agent(self, **kwargs: object) -> object:
            name = str(kwargs.pop("name", "CoreOrderingAgent"))
            definition = kwargs.pop("definition", None)
            if definition is None:
                model = kwargs.pop("model", None)
                instructions = kwargs.pop("instructions", None)
                if model is None:
                    raise TypeError("create_agent compatibility wrapper requires model or definition.")
                definition = _make_agent_definition(str(model), str(instructions or ""))
            return await self.create(name=name, definition=definition, **kwargs)
        setattr(AgentsOperations, "create_agent", _create_agent)


class CompatAzureAIAgentClient(AzureAIAgentClient):
    """
    Compatibility wrapper for mixed azure-ai-projects/agent-framework versions.
    Some SDK combinations return AgentObject shapes without `tools` / `instructions`
    attributes that agent_framework_azure_ai expects.
    """

    async def _load_agent_definition_if_needed(self):  # type: ignore[override]
        try:
            definition = await super()._load_agent_definition_if_needed()
        except Exception:
            return None
        if definition is None:
            return None

        if hasattr(definition, "tools") and hasattr(definition, "instructions"):
            return definition

        # Coerce dict-like AgentObject to an object with expected attrs.
        get_value = getattr(definition, "get", None)
        if callable(get_value):
            tools = get_value("tools", []) or []
            instructions = get_value("instructions")
            tool_resources = get_value("tool_resources")
            coerced = SimpleNamespace(
                tools=tools,
                instructions=instructions,
                tool_resources=tool_resources,
            )
            self._agent_definition = coerced
            return coerced

        # If shape is unknown, skip loading definition instead of failing requests.
        return None


async def get_core_ordering_agent() -> Agent:
    if not CORE_ORDERING_AGENT_ID:
        raise RuntimeError("Missing CORE_ORDERING_AGENT_ID in .env (create the agent once first).")

    _patch_ai_projects_agents_compat()
    async with get_azure_credential() as credential:
        resolved_agent_id = await _resolve_agent_id_if_needed(CORE_ORDERING_AGENT_ID, credential)
        chat_client = CompatAzureAIAgentClient(
            project_endpoint=AI_FOUNDRY_PROJECT_ENDPOINT,
            credential=credential,
            agent_id=resolved_agent_id,
            model_deployment_name=MODEL_DEPLOYMENT_NAME,
        )
        tools = [
            run_planning_tools_readonly_query,
        ]
        return Agent(
            client=chat_client,
            tools=tools,
        )


async def create_core_ordering_agent() -> str:
    _patch_ai_projects_agents_compat()
    async with get_azure_credential() as credential:
        async with AIProjectClient(endpoint=AI_FOUNDRY_PROJECT_ENDPOINT, credential=credential) as project_client:
            instructions = AGENT_INSTRUCTIONS
            definition = _make_agent_definition(
                model=MODEL_DEPLOYMENT_NAME,
                instructions=instructions,
            )

            try:
                if hasattr(project_client.agents, "create_agent"):
                    created = await project_client.agents.create_agent(
                        name="CoreOrderingAgent",
                        definition=definition,
                    )
                elif hasattr(project_client.agents, "create"):
                    created = await project_client.agents.create(
                        name="CoreOrderingAgent",
                        definition=definition,
                    )
                else:
                    # Compatibility fallback for SDK versions exposing a private create method.
                    created = await project_client.agents._create_agent(
                        name="CoreOrderingAgent",
                        definition=definition,
                    )
            except ResourceExistsError:
                # Idempotent behavior: if the named agent exists, return its id.
                if hasattr(project_client.agents, "get_agent"):
                    created = await project_client.agents.get_agent("CoreOrderingAgent")
                else:
                    created = await project_client.agents.get("CoreOrderingAgent")
            return created.id


if __name__ == "__main__":
    agent_id = asyncio.run(create_core_ordering_agent())
    print("Created agent id:", agent_id)
    print("Add to .env: CORE_ORDERING_AGENT_ID=" + agent_id)
