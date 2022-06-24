import json
import redis

from aiohttp import web


def run_bulk_requests_handler(config=None):
    app = web.application()
    app.router.add_post('/bulk_requests',
                        bulk_requests_handler)
    web.run_app(app)


def bulk_requests_handler(request):
    meters = request.query['meters']
    command = request.query['command']
    value = request.query['value']

    if not meters or not command \
            or not value:
        response_err = {'status': 'Invalid Bulk Requests'}
        return web.Response(
            text=json.dumps(response_err),
            status=403)

    schedule(meters, command, value)


def schedule(meters, command, value):
    for meter in meters:
        r = redis.Redis(host='localhost',
                        port=6379,
                        db=0)
        r.set(meter, f'{command}:{value}')
