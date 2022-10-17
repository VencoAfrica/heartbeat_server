import redis
import json
import h11
import time
import requests
import sqlite3
from asyncio.streams import StreamReader, StreamWriter
from h11 import Request
from urllib.parse import urlparse
from logging import Logger


class HTTPRequest:

    def __init__(self, data):
        self.data = data

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
        self._request = json.loads(self._data.data.decode('utf-8'))
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
        if not 'meter' in request:
            raise Exception('Missing field meter')

    def validate_callback_url(self, request):
        if 'callback_url' in request:
            callback_url = request['callback_url']
            if not self.is_url(callback_url):
                raise Exception(f'Invalid callback url {callback_url}')

    def is_url(self, url: str):
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except ValueError:
            return False
    
    def populate(self):
        self._meter = self.request['meter']
        self._ccu = self.request['ccu']
        self._callback_url = self.request['callback_url']

    
    
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
        self._request = json.loads(self._data.data.decode('utf-8'))
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
            if not self.is_url(callback_url):
                raise Exception(f'Invalid callback url {callback_url}')

    def is_url(self, url: str):
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except ValueError:
            return False

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
    try:
        request = json.loads(body.decode('utf-8'))
        if 'command' in request:
            return 'remote'
        if 'ccu' in request:
            return 'add_meter'
    except Exception as exec:
        logger.error(f'Exception inspect_http_content_type: {exec}')
        return None

async def process_http_request(request: HTTPRequest,
                               reader: StreamReader,
                               auth_token: str,
                               redis_params: dict,
                               writer: StreamWriter):
    conn = h11.Connection(h11.SERVER)
    conn.receive_data(request.data)
    while True:
        event = conn.next_event()
        if event is h11.NEED_DATA:
            conn.receive_data(await reader.read(255))
        elif isinstance(event, Request):
            method = event.method.decode('utf-8')
            if method == 'POST':
                token = extract('Authorization', event.headers)
                authenticate(token, auth_token)
                body = conn.next_event()
                content_type = inspect_http_content_type(body.data)
                if content_type == 'remote':
                    await queue(body, redis_params, writer)
                elif content_type == 'add_meter':
                    request = json.loads(body.decode('utf-8'))
                    # add to sqlite db'
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

async def add_meter_queue(data: bytearray, redis_params: dict, writer: StreamWriter):
    remote_add = AddMeterRequestPayload(data)
    # add to sqlite db

    # add to redis queue
    # send response

async def queue(data: bytearray, redis_params: dict,
          writer: StreamWriter):
    remote_request = RemoteRequestPayload(data)
    request_id = redis_write(remote_request, redis_params)
    message = f'Scheduled {remote_request.action} for {remote_request.meter}'
    await send_response(writer, request_id, 200, message)


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

