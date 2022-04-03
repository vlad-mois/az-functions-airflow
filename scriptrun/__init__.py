import ast
import fastjsonschema
import json
import logging
import os
import parsedatetime
import time
from datetime import datetime
from enum import IntEnum, unique
from string import Template
from typing import Any, Dict, Optional, Tuple

import azure.functions as func
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.fileshare import ShareFileClient


@unique
class ErrorCode(IntEnum):
    PARSE_ERROR = -32700
    INVALID_REQUEST = -32600
    METHOD_NOT_FOUND = -32601
    INVALID_PARAMS = -32602
    INTERNAL_ERROR = -32603


def parse_timedelta(interval: str) -> int:
    """Parse timedelta in string format.

    Args:
        interval: Timedelta in string format. Supported suffixes: w, d, h, m, s.

    Returns:
        Timedelta length in seconds.

    Examples:
        Parse string time interval.
        >>> parse_timedelta('1h 23m:45s')
        5025

        Parse date interval.
        >>> parse_timedelta('2w 3d')
        1468800

        Use seconds by default.
        >>> parse_timedelta('123')
        123
    """
    try:
        return int(interval)
    except ValueError:
        pass
    cal = parsedatetime.Calendar()
    ts_current = time.localtime()
    ts_parsed, ctx = cal.parse(interval, ts_current, version=parsedatetime.VERSION_CONTEXT_STYLE)
    if not ctx.dateTimeFlag:
        raise ValueError(f'Cant parse {interval} as a timedelta')
    dt_current = datetime.utcfromtimestamp(time.mktime(ts_current))
    dt_parsed = datetime.utcfromtimestamp(time.mktime(ts_parsed))
    timedelta = dt_parsed - dt_current
    return int(timedelta.total_seconds())


def _read_text_file(connection_string: str, share: str, path: str) -> str:
    """Read text file from given Azure File share.

    Args:
        connection_string: Connection string for the given Azure Storage account.
        share: Name of file share storage.
        path: Path at the storage.

    Returns:
        String content of the file.
    """
    client = ShareFileClient.from_connection_string(connection_string, share, path)
    return client.download_file().content_as_text()


def _write_text_file(
    connection_string: str,
    share: str,
    path: str,
    content: str,
    metadata: Optional[Dict[str, str]] = None,
) -> None:
    """Write given text content to the file at Aure File share. Overwrite existing content.

    Args:
        connection_string: Connection string for the given Azure Storage account.
        share: Name of file share storage.
        path: Path at the storage. File may exist.
    """
    client = ShareFileClient.from_connection_string(connection_string, share, path)
    kwargs = {'metadata': metadata} if metadata else {}
    client.upload_file(content.encode(), **kwargs)


def _exists(connection_string: str, share: str, path: str) -> bool:
    """Check if file exists in Azure File storage.

    Args:
        connection_string: Connection string for the given Azure Storage account.
        share: Name of file share storage.
        path: Path at the storage.

    Returns:
        True if file exists at this path, else otherwise.
    """
    client = ShareFileClient.from_connection_string(connection_string, share, path)
    try:
        client.get_file_properties()
        return True
    except ResourceNotFoundError:
        return False


def _make_error(message: str, error_code: ErrorCode, data: Optional[Any] = None) -> Dict[str, Any]:
    """Prepare error response body."""
    error = {'code': error_code.value, 'message': message}
    if data:
        try:
            json.dumps(data)
        except Exception:
            data = repr(data)
        error['data'] = data
    return error


PARAMS_JSONSCHEMA = {
    "type": "object",
    "properties": {
        "project": {"type": "string", "pattern": "^[0-9A-Za-z_]*$"},
        "script": {"type": "string", "pattern": "^[0-9A-Za-z_][0-9A-Za-z_.]*$"},
        "interval": {"type": "string", "pattern": "[0-9][0-9wdhms:]*"}
    },
    "required": ["project", "script"],
    "additionalProperties": True
}
PARAMS_JSONSCHEMA_COMPILED = fastjsonschema.compile(PARAMS_JSONSCHEMA)

MIN_INTERVAL = 60
DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'
SHARE_DAGS = 'dags'
SHARE_SCRIPTS = 'scripts'
HEALTHCHECK = 'healthcheck'


def process_request(
    template_dag_path: str,
    connection_string: str,
    headers: Dict[str, str],
    params: Dict[str, str],
    body: bytes,
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], int]:

    if params.get(HEALTHCHECK):
        return {HEALTHCHECK: True}, None, 200

    params = dict(params)
    if body:
        try:
            params.update(json.loads(body.decode()))
        except Exception as exc:
            return None, _make_error('Parse error', ErrorCode.PARSE_ERROR, exc), 400

    try:
        PARAMS_JSONSCHEMA_COMPILED(params)
    except Exception:
        return None, _make_error('Invalid params', ErrorCode.INVALID_PARAMS, {'expected': PARAMS_JSONSCHEMA}), 400

    try:
        interval_seconds: int = parse_timedelta(params.get('interval', '1d'))
    except ValueError:
        return None, _make_error('Invalid interval', ErrorCode.INVALID_PARAMS, {'interval': params['interval']}), 400

    if not _exists(connection_string, SHARE_SCRIPTS, params['script']):
        return None, _make_error('No such script', ErrorCode.INVALID_PARAMS, {'script': params['script']}), 400

    dag_name = f"dag_{params['project']}"
    ts = int(time.time())
    attributes = {
        'TEMPLATE': template_dag_path,
        'DAG_NAME': dag_name,
        'PROJECT': params['project'],
        'SCRIPT': params['script'],
        'UPDATE_TS': ts,
        'UPDATE_DT_UTC': datetime.utcfromtimestamp(ts).strftime(DATETIME_FORMAT),
        'INTERVAL_SECONDS': max(interval_seconds, MIN_INTERVAL),
        'X_CALLER_CONTEXT': headers.get('X-Caller-Context'),
        'USER_AGENT': headers.get('User-Agent'),
    }
    logging.info('Attributes to substitute: %s', attributes)

    template: str = _read_text_file(connection_string, SHARE_DAGS, template_dag_path)
    dag_config: str = Template(template).safe_substitute(
        {'ATTRIBUTES_JSON': json.dumps(attributes, indent=4, ensure_ascii=False)})

    try:
        ast.parse(dag_config)
    except SyntaxError:
        return None, _make_error('Invalid DAG config produced', ErrorCode.INTERNAL_ERROR), 500

    dag_path = f'{dag_name}.py'
    is_overwritten: bool = _exists(connection_string, SHARE_DAGS, dag_path)
    _write_text_file(connection_string, SHARE_DAGS, dag_path, dag_config)

    result = {'dag_path': dag_path, 'dag_config_len': len(dag_config), 'interval_seconds': interval_seconds}
    if is_overwritten:
        result['is_overwritten'] = True

    return result, None, 200


def main(req: func.HttpRequest) -> func.HttpResponse:
    result, error, status_code = process_request(
        os.environ['TEMPLATE_DAG_PATH'],
        os.environ['FILESHARE_CONNECTION_STRING'],
        req.headers,
        req.params,
        req.get_body(),
    )
    if result and error or not result and not error:
        raise ValueError(f'Either result or error should persist. Got: ({result}, {error}, {status_code})')

    response_body = {'result': result} if result else {'error': error}
    return func.HttpResponse(json.dumps(response_body, ensure_ascii=False), status_code=status_code)
