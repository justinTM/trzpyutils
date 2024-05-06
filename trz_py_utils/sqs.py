from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.utilities.data_classes import (
    APIGatewayProxyEventV2,
)
from boto3 import client
from mypy_boto3_sqs import SQSClient
from typing import Any
import json
import logging as log


SQS_CLIENT: SQSClient = client("sqs")

SQS_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "$id": "https://example.com/object1660222326.json",
    "type": "object",
    "title": "Object detection input schema",
    "description": "The root schema comprises the entire JSON document.",
    "required": ["Records"],
    "properties": {
        "Records": {
            "$id": "#root/Records",
            "title": "each record corresponds to an individual SQS message",
            "type": "array",
            "items": {
                "type": "object",
                "required": ["body"],
                "properties": {
                    "body": {
                        "$id": "#root/body",
                        "title": "body of the request, alongside context and headers",  # noqa
                        "type": "object",
                    }
                },
            },
        },
    },
}


def make_queue_url_from_sqs_event(event: dict[str, Any]):
    """From SQS Lambda event, construct the queue URL (eg. message deletion)

    Args:
        event (dict[str, Any]): Lambda event passed to handler

    Returns:
        str: SQS queue URL, useful for message deletion upon success

    Example:
        >>> from trz_py_utils.sqs import make_queue_url_from_sqs_event
        >>> url = make_queue_url_from_sqs_event({})
        Traceback (most recent call last):
          ...
        NotImplementedError: no SQS 'Records' in event dict

    Example:
        >>> from trz_py_utils.sqs import make_queue_url_from_sqs_event
        >>> make_queue_url_from_sqs_event({"Records": [{
        ...     "eventSourceARN": "arn:aws:sqs:us-west-2:123456789012:MyQueue"
        ... }]})
        'https://sqs.us-west-2.amazonaws.com/123456789012/MyQueue'
    """
    if "Records" not in event:
        raise NotImplementedError("no SQS 'Records' in event dict")
    elif len(event["Records"]) > 1:
        raise NotImplementedError("can't handle multiple SQS records")
    arn_parts = event["Records"][0]["eventSourceARN"].split(":")
    region = arn_parts[3]
    account_id = arn_parts[4]
    queue_name = arn_parts[5]

    return f"https://sqs.{region}.amazonaws.com/{account_id}/{queue_name}"


def parse_sqs_body(event: dict[str, Any],
                   context: LambdaContext) -> tuple[Exception | None, str]:
    """_summary_

    Args:
        event (dict[str, Any]): _description_
        context (LambdaContext): _description_

    Raises:
        NotImplementedError: can only handle events with a single Record

    Returns:
        tuple[Exception | None, str]: _description_

    Example:
        >>> from aws_lambda_powertools.utilities.validation import validator
        >>> from trz_py_utils.sqs import parse_sqs_body
        >>> error, body = parse_sqs_body({})
        Traceback (most recent call last):
          ...
        TypeError: parse_sqs_body() missing 1 required positional argument: 'context'

    Example:
        >>> from aws_lambda_powertools.utilities.validation import validator
        >>> from trz_py_utils.sqs import parse_sqs_body
        >>> error, body = parse_sqs_body(event={"Records": [{
        ...     "body": [1]
        ... }]}, context={})
        >>> body[0]
        1
    """  # noqa
    try:
        log.debug(json.dumps(event, indent=4))
        records = APIGatewayProxyEventV2(event).get("Records")
        if len(records) > 1:
            raise NotImplementedError("can't handle multiple SQS records")
        return None, records[0]["body"]
    except Exception as e:
        return e, None
