"""Route the existing relay and the reconnect protocol without mixing rooms."""
import asyncio
import os
from http import HTTPStatus
from urllib.parse import urlsplit

import websockets

RECONNECT_PATH = '/reconnect-v1'


def build_router(legacy_handler, reconnect_handler):
    async def process_request(connection, request):
        path = urlsplit(request.path).path
        if path == RECONNECT_PATH:
            # Apply the same settings as the tested reconnect listener, before
            # websockets starts its keepalive task. Legacy settings stay at defaults.
            connection.ping_interval = 5
            connection.ping_timeout = 5
            connection.close_timeout = 2
            connection.protocol.max_size = 4096
        elif path != '/':
            return connection.respond(HTTPStatus.NOT_FOUND, 'Unknown WebSocket path\n')
        return None

    async def handler(connection):
        if urlsplit(connection.request.path).path == RECONNECT_PATH:
            await reconnect_handler(connection)
        else:
            await legacy_handler(connection)

    return handler, process_request


async def serve_combined(legacy_handler):
    from normal_reconnect_server import Service
    import logging
    logging.basicConfig(level=logging.INFO)
    service = Service()
    handler, process_request = build_router(legacy_handler, service.handler)
    host = os.environ.get('NORMAL_DUAL_HOST', '0.0.0.0')
    port = int(os.environ.get('PORT', '10000'))
    async with websockets.serve(handler, host, port, process_request=process_request):
        print(f'Normal dual server: {host}:{port} | legacy / | reconnect {RECONNECT_PATH}', flush=True)
        await service.monitor()
