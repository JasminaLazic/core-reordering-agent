import sys
import os
import asyncio

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from azure.ai.projects.aio import AIProjectClient

from auth import get_azure_credential
from config import AI_FOUNDRY_PROJECT_ENDPOINT, MODEL_DEPLOYMENT_NAME, CORE_ORDERING_AGENT_ID
from agents.instructions import AGENT_INSTRUCTIONS
from agents.core_ordering_agent import _make_agent_definition

AGENT_NAME = "CoreOrderingAgent"


async def main() -> None:
    async with get_azure_credential() as credential:
        async with AIProjectClient(endpoint=AI_FOUNDRY_PROJECT_ENDPOINT, credential=credential) as client:
            definition = _make_agent_definition(
                model=MODEL_DEPLOYMENT_NAME,
                instructions=AGENT_INSTRUCTIONS,
            )
            updated = await client.agents._update_agent(
                AGENT_NAME,
                definition=definition,
            )
            print(f"updated_agent id={updated.id} name={updated.name}")
            print(f"instructions length: {len(AGENT_INSTRUCTIONS)} chars")


if __name__ == "__main__":
    asyncio.run(main())
