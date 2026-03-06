import os
import shutil

from azure.identity.aio import AzureCliCredential, DefaultAzureCredential


def get_azure_credential():
    """
    Prefer Azure CLI when available to keep local-dev auth deterministic.
    Set AZURE_AUTH_USE_CLI=false to force non-CLI auth.
    """
    env = os.environ.get("AZURE_AUTH_USE_CLI", "").strip().lower()
    if env in {"0", "false", "no"}:
        return DefaultAzureCredential(exclude_cli_credential=True)

    if shutil.which("az"):
        return AzureCliCredential()

    if env in {"1", "true", "yes"}:
        raise RuntimeError("AZURE_AUTH_USE_CLI=true but Azure CLI (`az`) was not found on PATH.")

    return DefaultAzureCredential(exclude_cli_credential=True)
