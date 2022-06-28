import json
import redis

from aiohttp import web

redis_host = '0.0.0.0'
redis_port = 6379
redis_db = 0


def run_bulk_requests_handler(logger,
                                    bwr_params,
                                    redis_params):
    bwr_host = bwr_params.get('host', '0.0.0.0')
    bwr_port = bwr_params.get('port', '18902')
    redis_host = redis_params.get('host', '0.0.0.0')
    redis_port = redis_params.get('port', 6379)

    if logger:
        logger.info(f'Running bulk requests '
                    f'handler at {bwr_host}:{bwr_port}')

    web.run_app(init_app(), host=bwr_host, port=bwr_port)


async def init_app() -> web.Application:
    app = web.Application()
    app.router.add_post('/bulk_requests',
                        handler=bulk_requests_handler)
    return app


async def bulk_requests_handler(request: web.Request) -> web.Response:
    meters = request.query['meters']
    command = request.query['command']
    value = request.query['value']

    if not meters or not command \
            or not value:
        response = {'status': 'Invalid Bulk Requests',
                    'code': 403}
    else:
        await schedule(meters, command, value)
        response = {'status': 'Read Requests scheduled',
                    'code': 200}

    return web.Response(
        text=json.dumps(response['status']),
        status=response['code'])


async def schedule(meters, command, value):
    for meter in meters:
        r = redis.Redis(host=redis_host,
                        port=redis_port,
                        db=redis_db)
        r.set(meter, f'{command}:{value}')
