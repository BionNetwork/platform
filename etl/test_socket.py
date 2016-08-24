# coding: utf-8

from __future__ import unicode_literals

import SocketServer
from django.conf import settings
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
os.environ["DJANGO_SETTINGS_MODULE"] = "config.settings.production"


class MyTCPHandler(SocketServer.BaseRequestHandler):
    """
    The request handler class for our server.

    It is instantiated once per connection to the server, and must
    override the handle() method to implement communication to the
    client.
    """

    def handle(self):
        # self.request is the TCP socket connected to the client
        data = self.request.recv(1024).strip()
        print "{0}:{1} wrote:".format(self.client_address[0],
                                      self.client_address[1])
        print data
        # just send back the same data, but upper-cased
        self.request.sendall(data.upper())

if __name__ == "__main__":

    HOST, PORT = settings.PHP_HOST, settings.PHP_PORT  # (localhost, 8070)
    print HOST, PORT

    server = SocketServer.TCPServer((HOST, PORT), MyTCPHandler)

    print 'Started!'
    server.serve_forever()
