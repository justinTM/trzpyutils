import logging as log
import json
from http.client import responses
from typing import Any
from socket import gethostname

from aws_lambda_powertools.utilities.validation import validator
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.utilities.validation.exceptions import (
    SchemaValidationError
)
from aws_lambda_powertools.utilities.data_classes import (
    APIGatewayProxyEventV2,
)

from botocore.response import StreamingBody
from mypy_boto3_lambda.client import LambdaClient

from trz_py_utils.format import dumps


# log = logging.getLogger(__name__)


def error_response(code: int, exception: Exception, **kwargs):
    """Craft a JSON object for lambda function response from Exception.
    Error message will be looked-up based on status code.

    Args:
        code (int): status code (eg. 500, 404).
        exception (Exception): error type and message will be extracted.

    Returns:
        dict[str, Any]: reponse JSON object.

    Example:
        >>> from trz_py_utils.lambda_func import error_response
        >>> res = error_response(500, ValueError("failed to call function"))
        >>> res.get("statusCode")
        500
        >>> res.get("isBase64Encoded")
        False
        >>> isinstance(res.get("body"), str)
        True

    Example:
        >>> from trz_py_utils.lambda_func import error_response
        >>> err = ValueError("failed to call function")
        >>> res = error_response(500, err, results=[1])
        >>> res.get("statusCode")
        500
        >>> res.get("isBase64Encoded")
        False
        >>> isinstance(res.get("body"), str)
        True
    """
    log.info("returning error response...")
    try:
        message = responses[code]
    except KeyError:
        code = 500
        message = "Internal Server Error"
        log.warn(f"couldn't find response for code '{code}', using 500...")
    response = {
        "statusCode": code,
        "headers": {
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "*",
        },
        "isBase64Encoded": False,
        "body": {
            "error": message,
            "message": f"{type(exception).__name__}: {exception}",
        }
    }
    response.get("body").update(kwargs)
    response["body"] = dumps(response["body"])
    log.info(dumps(response))
    return response


def success_response(message: str, **kwargs):
    """Craft a 200 'OK' JSON object for lambda function HTTP request response.

    Args:
        message (str): Custom message for response body.

    Returns:
        dict[str, Any]: response JSON object.

    Example:
        >>> from trz_py_utils.lambda_func import success_response
        >>> res = success_response("called lambda function successfully")
        >>> res.get("statusCode")
        200
        >>> res.get("isBase64Encoded")
        False
        >>> isinstance(res.get("body"), str)
        True

    Example:
        >>> from trz_py_utils.lambda_func import success_response
        >>> msg = "called lambda function successfully"
        >>> res = success_response(msg, results=[1])
        >>> res.get("statusCode")
        200
        >>> res.get("isBase64Encoded")
        False
        >>> isinstance(res.get("body"), str)
        True
    """
    log.info("returning success reponse...")
    response = {
        "statusCode": 200,
        "body": {
            "message": message,
        },
        "headers": {
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "*",
        },
        "isBase64Encoded": False,
    }
    response.get("body").update(kwargs)
    response["body"] = dumps(response["body"])
    log.info(dumps(response))
    return response


def parse_event(event: APIGatewayProxyEventV2,
                context: LambdaContext, schema: dict):
    """Extracts body from lambda event while validating against schema.

    Args:
        event (APIGatewayProxyEventV2): https://docs.aws.amazon.com/apigateway/latest/developerguide/http-api-develop-integrations-lambda.html
        context (LambdaContext): _description_

    Returns:
        tuple[Exception, str]: error (or None) and event body string

    Example:
        >>> from trz_py_utils.lambda_func import parse_event
        >>> error, body = parse_event({}, {}, None)
        >>> error
        >>> body
        {}

    Example:
        >>> from trz_py_utils.lambda_func import parse_event, error_response
        >>> from aws_lambda_powertools.utilities.validation import validator
        >>> REQUEST_SCHEMA = {
        ...     "$schema": "http://json-schema.org/draft-07/schema#",
        ...     "$id": "https://example.com/object1660222326.json",
        ...     "type": "object",
        ...     "title": "Event schema for payload passed to lambda function",
        ...     "description": "Root schema comprises entire JSON document",
        ...     "examples": [
        ...         {
        ...             "body": { "regex": ".*" }
        ...         },
        ...     ],
        ...     "required": ["body"],
        ...     "properties": {
        ...         "body": {
        ...             "$id": "#root/body",
        ...             "title": "Root",
        ...             "type": "object",
        ...         },
        ...     },
        ... }
        >>> EVENT = {
        ...     "resource": "/",
        ...     "path": "/",
        ...     "httpMethod": "GET",
        ...     "requestContext": {
        ...         "resourcePath": "/",
        ...         "httpMethod": "GET",
        ...         "path": "/Prod/",
        ...     },
        ...     "headers": {
        ...         "accept": "text/html",
        ...         "accept-encoding": "gzip, deflate, br",
        ...         "Host": "xxx.us-east-2.amazonaws.com",
        ...         "User-Agent": "Mozilla/5.0",
        ...     },
        ...     "multiValueHeaders": {
        ...         "accept": [
        ...             "text/html"
        ...         ],
        ...         "accept-encoding": [
        ...             "gzip, deflate, br"
        ...         ],
        ...     },
        ...     "queryStringParameters": {
        ...         "postcode": 12345
        ...         },
        ...     "multiValueQueryStringParameters": None,
        ...     "pathParameters": None,
        ...     "stageVariables": None,
        ...     "body": {"regex": ".*"},
        ...     "isBase64Encoded": False
        ... }
        >>> error, body = parse_event(EVENT, {}, REQUEST_SCHEMA)
        >>> body
        {'regex': '.*'}

    Example:
        >>> from trz_py_utils.lambda_func import parse_event, error_response
        >>> from aws_lambda_powertools.utilities.validation import validator
        >>> from aws_lambda_powertools.utilities.validation.exceptions import (
        ...     SchemaValidationError
        ... )
        >>> REQUEST_SCHEMA = {
        ...     "type": "object",
        ...     "required": ["body"],
        ...     "properties": {
        ...         "body": {
        ...             "type": "object",
        ...         },
        ...     },
        ... }
        >>> EVENT = {
        ...     "body": ["val1"],
        ... }
        >>> error, body = parse_event(EVENT, {}, REQUEST_SCHEMA)
        >>> error
        SchemaValidationError("Failed schema validation. Error: data.body must be object, Path: ['data', 'body'], Data: ['val1']")
    """  # noqa
    body = {}
    error = None
    # allow non-lambda executions to skip schema validation
    if not event and not context:
        return error, body

    @validator(inbound_schema=schema)
    def parse(event, context: LambdaContext) -> tuple[Exception, str]:
        try:
            body = APIGatewayProxyEventV2(event).body
            log.info("got body:")
            log.info(json.dumps(body, indent=4))
            return None, body
        except Exception as e:
            log.error("ERROR bad request format:")
            log.info(json.dumps(event, indent=4))
            return e, None

    try:
        if isinstance(event.get("body", {}), str):
            event["body"] = json.loads(event["body"])
        return parse(event, context)
    except SchemaValidationError as e:
        return e, {}


def get_or_make_request_id(event: APIGatewayProxyEventV2):
    """Parse requestId or craft one if running locally.

    Args:
        event (APIGatewayProxyEventV2): _description_

    Returns:
        str: request id

    Example:
        >>> from trz_py_utils.lambda_func import get_or_make_request_id
        >>> from socket import gethostname
        >>> host = gethostname()
        >>> host == get_or_make_request_id({})
        True

    Example:
        >>> from trz_py_utils.lambda_func import get_or_make_request_id
        >>> get_or_make_request_id({"requestContext": {"requestId": "asd"}})
        'asd'
    """
    try:
        request_id: str = event.get("requestContext").get("requestId")
    except AttributeError as e:
        if "NoneType" in str(e):
            request_id = gethostname()
        else:
            raise

    return request_id


def parse_invoke_response(response: dict[str, Any | StreamingBody]):
    """
    Decode Payload from invocation response object. Payload
    is of type StreamingBody. See docs for more:
    - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/lambda/client/invoke.html#
    - https://botocore.amazonaws.com/v1/documentation/api/latest/reference/response.html

    Example:
        >>> from trz_py_utils.lambda_func import parse_invoke_response
        >>> import io; stream = io.BytesIO(b'{"body": "example"}')
        >>> response = {"Payload": stream}
        >>> parse_invoke_response(response)
        {'body': 'example'}
    """  # noqa
    streaming_body: StreamingBody = response["Payload"]
    payload_str = streaming_body.read().decode("utf-8")
    response: dict = json.loads(payload_str)
    if not isinstance(response, dict):
        log.warn(f"expected response type dict, but got {type(response)}!")

    return response


def handle_lambda_response(name: str,
                           response_raw: dict[str, Any | bytes]):
    """Parses lambda response and crafts response JSON accordingly.

    Args:
        name (str): Name of the lambda (for logging purposes)
        response_raw (dict[str, Any]):

    Returns:
        tuple(Exception|None, dict): error (or None) and response

    Example:
        >>> from trz_py_utils.lambda_func import handle_lambda_response
        >>> from io import BytesIO
        >>> response = {
        ...     "Payload": BytesIO(b'{"statusCode": 200, "body": "example"}')}
        >>> error, response = handle_lambda_response("func1", response)
        >>> response
        {'statusCode': 200, 'body': 'example'}

    Example:
        >>> from trz_py_utils.lambda_func import handle_lambda_response
        >>> from io import BytesIO
        >>> response = {"Payload": BytesIO(b'{"body": "example"}')}
        >>> error, response = handle_lambda_response("func1", response)
        >>> error
        ValueError("ERROR lambda 'func1' response not OK (500): {'body': 'example'}")
    """  # noqa
    # parse and print the response
    error: ValueError = None
    response = parse_invoke_response(response_raw)

    status = response.get("statusCode", 500)
    if status == 200:
        log.info(f"200 OK response from '{name}' lambda")
        log.info(response)
    else:
        msg = f"ERROR lambda '{name}' response not OK ({status}): {response}"
        log.error(msg)
        error = ValueError(msg)

    return error, response


def call_lambda(name: str, payload: Any, lambda_client: LambdaClient):
    """Invokes a lambda function by name, with a payload (usually dict)
    and a client like `boto3.client('lambda')`. See docs for more:
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/lambda/client/invoke.html#

    Args:
        name (str): Name of the lambda function to invoke.
        payload (Any): Object (usualy dict) to pass as request for function.
        lambda_client (LambdaClient): Low-level client representing AWS Lambda.

    Returns:
        tuple[Exception, dict]: error (or None) and response object
    """
    log.info(f"invoking lambda '{name}'...")
    log.info(f"with payload:\n {json.dumps(payload, indent=4)}")

    # Invoke the target Lambda function
    response_raw = lambda_client.invoke(
        FunctionName=name,
        InvocationType="RequestResponse",  # Use "Event" for async invoke
        LogType="Tail",  # Include logs in the response
        Payload=json.dumps(payload)
    )

    return handle_lambda_response(name, response_raw)
