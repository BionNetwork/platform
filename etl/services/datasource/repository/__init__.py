import redis
from django.conf import settings
from redis.connection import Connection
from core.helpers import check_redis_lock


class LockRetryConnection(Connection):

    @check_redis_lock
    def send_command(self, *args):
        super(LockRetryConnection, self).send_command(*args)


class LockRetryStrictRedis(redis.StrictRedis):
    def __init__(self, host='localhost', port=6379,
                 db=0, password=None, socket_timeout=None,
                 socket_connect_timeout=None,
                 socket_keepalive=None, socket_keepalive_options=None,
                 connection_pool=None, unix_socket_path=None,
                 encoding='utf-8', encoding_errors='strict',
                 charset=None, errors=None,
                 decode_responses=False, retry_on_timeout=False,
                 ssl=False, ssl_keyfile=None, ssl_certfile=None,
                 ssl_cert_reqs=None, ssl_ca_certs=None):
        super(LockRetryStrictRedis, self).__init__(
            host=host, port=port, db=db, password=password,
            socket_timeout=socket_timeout,
            socket_connect_timeout=socket_connect_timeout,
            socket_keepalive=socket_keepalive,
            socket_keepalive_options=socket_keepalive_options,
            connection_pool=connection_pool, unix_socket_path=unix_socket_path,
            encoding=encoding, encoding_errors=encoding_errors,
            charset=charset, errors=errors, decode_responses=decode_responses,
            retry_on_timeout=retry_on_timeout, ssl=ssl, ssl_keyfile=ssl_keyfile,
            ssl_certfile=ssl_certfile, ssl_cert_reqs=ssl_cert_reqs,
            ssl_ca_certs=ssl_ca_certs)

        self.connection_pool.connection_class = LockRetryConnection


r_server = LockRetryStrictRedis(
    host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=settings.REDIS_DB)
