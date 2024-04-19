import os
import httpx

_client = None
_aclient = None

# TODO: handle closing clients

headers = {}
if 'SUPERPIPE_API_KEY' in os.environ:
    headers['X_API_KEY_HEADER'] = os.environ['SUPERPIPE_API_KEY']


def get_client():
    global _client
    if _client is None:
        _client = httpx.Client(headers=headers)
    return _client


def get_async_client():
    global _aclient
    if _aclient is None:
        _aclient = httpx.AsyncClient(headers=headers)
    return _aclient
