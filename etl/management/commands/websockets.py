# coding: utf-8


import brukva

from django.conf import settings
from django.core.management.base import BaseCommand

from tornado import ioloop
from tornado.web import Application
from tornado.websocket import WebSocketHandler

class Command(BaseCommand):

    def handle(self, *args, **options):
        class SocketHandler(WebSocketHandler):

            def check_origin(self, origin):
                """ Проверяются хосты, например
                    127.0.0.1:8000 и 127.0.0.1:8888
                    чтоб работало перекрываем метод
                """
                host = self.request.headers.get("Host")
                settings_host = (settings.SOCKET_HOST +
                                 (settings.SOCKET_PORT and (':' + settings.SOCKET_PORT)))

                return host == settings_host

            def open(self, user_id, task_id):
                # создание канала
                self.client = brukva.Client(host=settings.REDIS_HOST,
                                            port=int(settings.REDIS_PORT),
                                            selected_db=settings.REDIS_DB)
                self.client.connect()
                # подписка на канал
                self.channel = 'jobs:etl:extract:{0}:{1}'.format(user_id, task_id)
                self.client.subscribe(self.channel)
                # прослушка
                self.client.listen(self.on_messages_published)

            def on_messages_published(self, message):
                self.write_message(message.body)

            def on_close(self):
                self.client.unsubscribe(self.channel)


        application = Application([
            (r"/socket/user/(\d+)/task/(\d+)", SocketHandler),
        ], {})

        # if __name__ == "__main__":
        print 'Tornado is started'
        application.listen(int(settings.SOCKET_PORT))
        ioloop.IOLoop.instance().start()
