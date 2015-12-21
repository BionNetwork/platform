# coding: utf-8

import json
import txredisapi

from django.conf import settings
from django.core.management.base import BaseCommand

from twisted.internet.defer import inlineCallbacks
from twisted.internet.protocol import ClientCreator
from twisted.internet import reactor
from autobahn.twisted.wamp import ApplicationSession, ApplicationRunner


class Command(BaseCommand):

    def handle(self, *args, **options):

        class Component(ApplicationSession):
            """
                WAMP-server
            """
            @inlineCallbacks
            def onJoin(self, details):
                print("ApplicationSession is started")

                class MyRedisProtocol(txredisapi.SubscriberProtocol):

                    def connectionMade(redis_self):
                        self.protocol_ins = redis_self

                    def messageReceived(redis_self, pattern, channel, message):

                        mess = json.loads(message)
                        if mess['event'] == 'finish':
                            redis_self.unsubscribe(channel)

                        self.publish(channel, message)

                ClientCreator(reactor, MyRedisProtocol).connectTCP(
                    settings.REDIS_HOST, int(settings.REDIS_PORT))

                def channel_to_publish(new_channel):
                    print new_channel
                    self.protocol_ins.subscribe(new_channel)
                    return "It's ok"

                yield self.register(channel_to_publish, u'set_publish_channel')

        runner = ApplicationRunner(
            u"ws://{0}:{1}/ws".format(settings.SOCKET_HOST, settings.SOCKET_PORT),
            u"realm1",
        )
        runner.run(Component)

        # class SocketHandler(WebSocketHandler):
        #
        #     def check_origin(self, origin):
        #         """ Проверяются хосты, например
        #             127.0.0.1:8000 и 127.0.0.1:8888
        #             чтоб работало перекрываем метод
        #         """
        #         host = self.request.headers.get("Host")
        #         settings_host = (settings.SOCKET_HOST +
        #                          (settings.SOCKET_PORT and (':' + settings.SOCKET_PORT)))
        #
        #         return host == settings_host
        #
        #     def open(self, channel):
        #         # создание канала
        #         self.client = brukva.Client(host=settings.REDIS_HOST,
        #                                     port=int(settings.REDIS_PORT),
        #                                     selected_db=settings.REDIS_DB)
        #         self.client.connect()
        #         # подписка на канал
        #         self.channel = channel
        #         self.client.subscribe(self.channel)
        #         # прослушка
        #         self.client.listen(self.on_messages_published)
        #
        #     def on_messages_published(self, message):
        #         self.write_message(message.body)
        #
        #     def on_close(self):
        #         self.client.unsubscribe(self.channel)
        #
        # application = Application([
        #     (r"/socket/channel/(.*)", SocketHandler),
        # ], {}, debug=settings.DEBUG)
        #
        # # if __name__ == "__main__":
        # print 'Tornado is started'
        # application.listen(int(settings.SOCKET_PORT))
        # ioloop.IOLoop.instance().start()
