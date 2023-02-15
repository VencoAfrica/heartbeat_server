from asyncio.log import logger
import asyncio
from uuid import uuid4
import redis
import json
import h11
import time
import requests
from .db import Db
from asyncio.streams import StreamReader, StreamWriter
from h11 import Request
from urllib.parse import urlparse
from logging import Logger


class HTTPRequest:

    def __init__(self, data):
        self.data = data

def is_url(url: str):
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False

class AddMeterRequestPayload:
    '''
    A remote request

        {
            "ccu": "{ccu_no}",
            "meters": "{comma separated set of meters}",
            "callback_url": "https://fcb3-105-160-5-15.ngrok.io"
        }
    '''

    @property
    def data(self):
        return self._data

    @property
    def request(self):
        return self._request

    @property
    def ccu(self):
        return self._ccu

    @property
    def meters(self):
        return self._meters

    @property
    def callback_url(self):
        return self._callback_url

    def __init__(self, data: bytearray):
        self._data = data
        self._request = json.loads(self._data.decode('utf-8'))
        self.validate()
        self.populate()
        
    def validate(self):
        self.validate_ccu(self._request)
        self.validate_meters(self._request)
        self.validate_callback_url(self._request)


    def validate_ccu(self, request):
        if not 'ccu' in request:
            raise Exception('Missing field ccu')

    def validate_meters(self, request):
        if not 'meters' in request:
            raise Exception('Missing field meters')
        meters = request['meters']
        if not isinstance(meters, list):
            raise Exception('Meters must be a list')
 
    def validate_callback_url(self, request):
        if 'callback_url' in request:
            callback_url = request['callback_url']
            if not is_url(callback_url):
                raise Exception(f'Invalid callback url {callback_url}')
    
    def populate(self):
        self._ccu = self._request['ccu']
        self._meters = self._request['meters']
        self._callback_url = self._request.get('callback_url', None)

    
class RemoteRequestPayload:
    """
    A remote request denotes a POST request to read
    or write a specific OBIS code instruction to
    a meter.

    It is a json payload with the following format:

    {
        action: "write",
        meter: "MTRK179000989931",
        command: "Get Meters Count",
        code: "96.51.91.1",
        value: "12344665363643",
        "callback_url": "https://callback.url"
    }

    To note, authentication/authorization for the
    callback URL is not supported. In addition, if no
    `callback_url` is provided, then no callback will
    be issued.

    Returns:
        {
            "status": <status_code>,
            "message": "<transaction queue message",
            "transaction_id": "<hex_string>",
            "timestamp": "<timestamp>"
        }

        status:
            200 - if queued, 0 - if error
        message:
            Message related to status above
        transaction_id:
            A hex string that can be used by the
            requesting service to match requests or
            to validate the status of a transaction.
        timestamp:
            Timestamp as epoch time e.g. 1331856000000

    """

    @property
    def data(self):
        return self._data

    @property
    def request(self):
        return self._request

    @property
    def action(self):
        return self._action

    @property
    def meter(self):
        return self._meter

    @property
    def command(self):
        return self._command

    @property
    def code(self):
        return self._code

    @property
    def value(self):
        return self._value

    @property
    def callback_url(self):
        return self._callback_url

    def __init__(self, data: bytearray):
        self._data = data
        self._request = json.loads(self._data.decode('utf-8'))
        self.validate()
        self.populate()

    def validate(self):
        self.validate_action(self._request)
        self.validate_meter(self._request)
        self.validate_command(self._request)
        self.validate_code(self._request)
        self.validate_value(self._request)
        self.validate_callback_url(self._request)

    def validate_action(self, request):
        if 'action' in request:
            action = request['action']
            if not any(action.upper() == val for val in ['WRITE', 'READ']):
                raise Exception(f'Invalid method {action}')
            self._action = action
            return
        raise Exception('Missing field action')

    def validate_meter(self, request):
        if not 'meter' in request:
            raise Exception('Missing field meter')

    def validate_command(self, request):
        if not 'command' in request:
            raise Exception('Missing field command')

    def validate_code(self, request):
        if not 'code' in request:
            raise Exception('Missing field code')

    def validate_value(self, request):
        if request['action'].upper() == 'WRITE':
            if not 'value' in request:
                raise Exception('Missing field value')

    def validate_callback_url(self, request):
        if 'callback_url' in request:
            callback_url = request['callback_url']
            if not is_url(callback_url):
                raise Exception(f'Invalid callback url {callback_url}')

    def populate(self):
        self._action = self.request['action']
        self._meter = self.request['meter']
        self._command = self.request['command']
        self._code = self.request['code']
        self._callback_url = self.request['callback_url']

        if self._action.upper() == 'WRITE':
            self._value = self.request['value']


def is_supported_http_request(data: bytearray, logger: Logger):
    try:
        supported_verbs = ['GET', 'POST', 'PUT', 'PATCH', 'DELETE']
        if any(data.decode('utf-8').upper().startswith(verb)
               for verb in supported_verbs):
            if data.decode('utf-8').upper() \
                    .startswith(('GET', 'PUT', 'PATCH', 'DELETE')):
                raise Exception(f'Unsupported method')
            return True
    except Exception as exec:
        logger.error(f'Exception is_supported_http_request: {exec}')
        return False

def inspect_http_content_type(body):
    request = json.loads(body.data.decode('utf-8'))
    if 'command' in request:
        return 'remote'
    if 'meters' in request:
        return 'ccu'
    if 'command' not in request and 'meters' not in request:
        raise Exception('Invalid request')

def get_request_path(url):
    parsed_url = urlparse(url)
    path = parsed_url.path
    resource = path.split('/')[-1]
    return resource

async def process_http_request(request: HTTPRequest,
                               reader: StreamReader,
                               auth_token: str,
                               redis_params: dict,
                               db_params:dict,
                               writer: StreamWriter,
                               logger: Logger):
    conn = h11.Connection(h11.SERVER)
    conn.receive_data(request.data)
    while True:
        event = conn.next_event()
        if event is h11.NEED_DATA:
            conn.receive_data(await reader.read(255))
        elif isinstance(event, Request):
            method = event.method.decode('utf-8')
            request_url = event.target.decode('utf-8')
            if method == 'POST':
                token = extract('Authorization', event.headers)
                authenticate(token, auth_token)
                body = conn.next_event()
                content_type = inspect_http_content_type(body)
                request_path = get_request_path(request_url)
                if content_type == 'remote':
                    await queue(body.data, redis_params, writer)
                elif request_path == 'ccu_add_meter':
                    await add_meter_queue(body.data, redis_params, writer, db_params, auth_token, logger)
            else:
                raise Exception('Unsupported HTTP method')


def extract(desired, headers):
    for header in headers:
        if header[0].decode('utf-8').upper() \
                == desired.upper():
            return header[1].decode('utf-8')
    raise Exception(f'{desired} header not found!')


def authenticate(token, auth_token):
    if token.split('Bearer')[1].strip() != auth_token:
        raise Exception('Invalid auth token')

async def process_add_meter_command(ccu_no, meters,
                                    callback_url,
                                    db_params, redis_params, logger):
    for meter in meters:
        remote_meter_add_payload = {
            "action": "read",
            "meter": '{}-{}'.format(ccu_no, meter),
            "command": "Add Meter",
            "code": "96.51.90.255",
            "callback_url": callback_url if callback_url else ''
        }
        payload = json.dumps(remote_meter_add_payload)
        remote_meter_add = bytearray(payload, 'utf-8')
        logger.info(remote_meter_add)
        payload = RemoteRequestPayload(remote_meter_add)
        redis_command_id = redis_write(payload, redis_params)
        logger.info(f"process_add_meter queued {redis_command_id}")

async def queue(data: bytearray, redis_params: dict,
          writer: StreamWriter):
    remote_request = RemoteRequestPayload(data)
    request_id = redis_write(remote_request, redis_params)
    message = f'Scheduled {remote_request.action} for {remote_request.meter}'
    await send_response(writer, request_id, 200, message)

async def add_meter_queue(data: bytearray, redis_params: dict,
                          writer: StreamWriter, db_params: dict,
                          auth_token:str, logger):
    request_id = str(uuid4())                    
    try:
        remote_add = AddMeterRequestPayload(data)
        message = f'Scheduled meter addition for {remote_add.ccu}'
        await send_response(writer, request_id, 200, message)
        await process_add_meter_command(remote_add.ccu,
                                        remote_add.meters,
                                        remote_add.callback_url,
                                        db_params, redis_params, logger)
    except Exception as e:
        await send_response(writer, request_id, 500, str(e))
        logger.info(e)


def redis_write(remote_request: RemoteRequestPayload,
                redis_params: dict) -> int:
    _d = '*|'
    r = redis.Redis(host=redis_params.get('host', '0.0.0.0'),
                    port=redis_params.get('port', 6379),
                    db=redis_params.get('db', 0))
    value = None
    curr_timestamp = (round(time.time() * 1000))

    if remote_request.action.upper() == 'WRITE':
        value = f'W{_d}{remote_request.meter}' \
                f'{_d}{remote_request.command}' \
                f'{_d}{remote_request.code}' \
                f'{_d}{remote_request.value}' \
                f'{_d}{remote_request.callback_url}'
    elif remote_request.action.upper() == 'READ':
        value = f'R{_d}{remote_request.meter}' \
                f'{_d}{remote_request.command}' \
                f'{_d}{remote_request.code}' \
                f'{_d}{remote_request.callback_url}'

    if value:
        r.set(f'*|{curr_timestamp}', value)

    return curr_timestamp


def send_callback(data: dict,
                  callback_url: str,
                  logger: Logger):
    meter_no = data['meter_no']
    logger.info(f'Sending callback for {meter_no} to {callback_url}')
    resp = requests.post(url=callback_url, data=json.dumps(data))
    logger.info(f'Callback result: status_code {resp.status_code} reason {resp.reason}')


async def send_response(writer: StreamWriter,
                        request_id: int,
                        status: int,
                        message: str):
    response = json.dumps({
                            'status': status,
                            'request_id': request_id,
                            'message': message
                        }).encode()
    headers = [
        ('Host', 'Heartbeat server'),
        ('Content-length', str(len(response))),
        ('Content-type', 'application/json')
    ]
    conn = h11.Connection(h11.SERVER)
    output = conn.send(h11.Response(
        status_code=status,
        headers=headers
    ))
    output += conn.send(h11.Data(data=response))
    output += conn.send(h11.EndOfMessage())
    writer.write(output)
