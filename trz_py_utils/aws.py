from typing import Any
import json
import logging


log = logging.getLogger(__name__)


def get_secret(secret_name: str, sm_client):
    """Retrieve and `json.loads()` a Secret from SecretsManager

    Args:
        secret_name (str): name fo the ssecret
        sm_client (SecretsManagerClient): _description_

    Returns:
        dict[str, Any]: secret object

    Example:
        >>> from trz_py_utils.aws import get_secret
        >>> import boto3
        >>> sm_client = boto3.client("secretsmanager")
        >>> get_secret("trz-docs-test", sm_client)
        {'key1': 'value1'}
    """
    log.info(f"retrieving secret '{secret_name}'...")
    response = sm_client.get_secret_value(SecretId=secret_name)

    secret: dict[str, Any] = json.loads(response['SecretString'])
    log.info("secret retrieved successfully.")

    return secret
