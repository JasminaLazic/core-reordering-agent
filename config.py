import os
from dotenv import load_dotenv

_ROOT_ENV_PATH = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path=_ROOT_ENV_PATH, override=True)

AI_FOUNDRY_PROJECT_ENDPOINT = os.environ["AI_FOUNDRY_PROJECT_ENDPOINT"]
MODEL_DEPLOYMENT_NAME = os.environ["MODEL_DEPLOYMENT_NAME"]
CORE_ORDERING_AGENT_ID = os.environ.get("CORE_ORDERING_AGENT_ID")
