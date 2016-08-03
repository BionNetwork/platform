# coding: utf-8

from __future__ import unicode_literals, division

import sys
import os
import re
import requests
import operator
import numpy
import socket
import codecs

from urlparse import urlparse

from pandas import DataFrame

from io import StringIO
from lxml import etree


# при необходимости использовать список ненеужных тэгов
# например при наследовании,
# данный момент используются регул выражения
EXCLUDE_TAGS = ["script", "noscript", "style", "time", ]

ALLOW_PROTOCOLS = ["http", "https", ]

CONTENT_TAGS = ["p", "span", "h3", "h4", "h5", "h6", "h7", ]

DEFAULT_PORT = 80

P_TAG = "p"
A_TAG = "a"
GARBAGE_TAGS = ["span", ]
SPAN_MIN_TEXT = 15

HEADER_TAGS = ["h1", "h2", ]


# избавление от пустоты(\n\t\r\f\v)
text_ptrn = re.compile(r'\s+$')
# избавление от тэгов
tag_ptrn = re.compile(r'<cyfunction|script|noscript|style|time')

head_ptrn = re.compile(r'h\d')

BIG_FOOT = 10
CONTENT_STEP = 5
ARTICLE_MIN_SIZE = 400


def exit(msg):
    # выход
    print msg
    sys.exit()


class UrlValidator(object):
    """
    """

    def __init__(self, url):
        self.connector = urlparse(url)

    def validate(self):
        """
        """
        connector = self.connector
        if connector.hostname is None:
            exit("ERROR: INVALID URL! SET PROTOCOL AND HOSTNAME")

        if connector.scheme not in ALLOW_PROTOCOLS:
            exit("ERROR: PROTOCOL IS NOT SUPPORTED!")

        self.tcp_connect()

    def tcp_connect(self):
        """
        """
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(10)
        except socket.error as err:
            exit('Failed to create socket! Error: {0}'.format(err.message))

        connector = self.connector
        try:
            ip = socket.gethostbyname(connector.hostname)
        except socket.gaierror:
            exit('Hostname could not be resolved!')

        try:
            s.connect((ip, connector.port or DEFAULT_PORT))
        except socket.error as err:
            exit('Failed to create socket! Error: {0}'.format(err.message))


class ArgsValidator(object):
    """
    Валидатор входных параметров
    """
    @classmethod
    def validate(cls, args):
        """

        """
        if len(args) == 1:
            raise Exception('Please enter site url as second argument!')

        url_checker = UrlValidator(url=args[1])
        url_checker.validate()


class ContentTree(object):
    """

    """
    def __init__(self, url):
        """

        """
        self.url = url
        self.tree = None
        self.data = []

    def build_tree(self):
        """

        """
        resp = requests.get(self.url)
        parser_ = etree.HTMLParser()
        self.tree = etree.parse(StringIO(resp.text), parser_)

    def get_tree_content(self):
        """
        """
        tree = self.tree
        data = self.data

        if tree is None:
            raise Exception("Build tree firstly!")

        body = tree.getroot().find('body')
        iter_ = body.getiterator()

        ind = 0

        for el in iter_:
            if (re.match(text_ptrn, el.text or ' ') is None and
                    re.match(tag_ptrn, str(el.tag)) is None):
                data.append((ind, len(el.text), el.tag, el.text))
                ind += 1

    def get_headers(self):
        """
        """
        return list(self.tree.iter(HEADER_TAGS))

    def headers_below_content(self, headers):
        """
        """
        for head in headers:
            for parent in head.iterancestors():
                contents = parent.iter(P_TAG)
                length_ = len(reduce(
                    operator.add, parent.itertext(CONTENT_TAGS), ''))
                if length_ > ARTICLE_MIN_SIZE:
                    return list(contents)

        exit("Something went wrong!")

    def nearest_parent(self, content):
        """
        """
        l = len(content)
        middle = content[int(l/2)]

        par = middle.getparent()
        childs = list(par.iter())

        while len(childs)/l < 0.7:
            par = par.getparent()
            childs = list(par.iter())

        return par

    def get_content(self, element):
        """
        """
        return list(element.iter(CONTENT_TAGS))


class FileBuilder(object):
    """

    """
    def __init__(self, url):
        """
        """
        self.url = url

    @staticmethod
    def new_line(text):
        """
        """
        return "{0}{1}".format(text, "\n")

    @staticmethod
    def get_tail(element):
        """
        """
        print 'get_tail', element, element.text
        s = ''
        for a in list(element.iter(A_TAG)):

            print a.tag
            print a.text

            s += "{0}{1}{2}".format(a.text or '', '[]', a.tail or '')
        print s
        return s

    @staticmethod
    def filter(element):
        """
        """

        # print 'fi'
        # print element.tag
        # print element.text
        # print 'sdfg'

        text = element.text or ''

        if element.tag in GARBAGE_TAGS and len(text) < SPAN_MIN_TEXT:
            return False
        return True

    def write_content(self, headers, contents):
        """
        """
        l = self.new_line
        fi = self.filter
        t = self.get_tail

        with codecs.open('file.txt', 'w', 'utf-8') as f:
            for h in headers:
                f.write(l(h.text))

            for elem in contents:
                print 80*'-'
                print elem.text
                print 80*'-'
                # print bool(fi(elem))
                # t(elem)
                # ases = list(elem.iter(A_TAG))
                # print ases
                # for a in ases:
                #     print a.tag
                f.write(
                    l("{0}{1}".format(elem.text or '', t(elem)))
                    if fi(elem) else ''
                )


class DataMiner(object):
    """
    Добыватель данных с сайтов
    """
    def __init__(self, url):
        """
        """
        self.headers = None
        self.content = None
        self.tree_builder = ContentTree(url)
        self.file_builder = FileBuilder(url)

    def prepare(self):
        """
        """
        t_builder = self.tree_builder
        t_builder.build_tree()
        t_builder.get_tree_content()

    def mine_by_tree(self):
        """
        """
        tree_manager = self.tree_builder

        self.headers = tree_manager.get_headers()

        below_header = tree_manager.headers_below_content(
            self.headers)

        parent = tree_manager.nearest_parent(below_header)

        self.content = tree_manager.get_content(parent)

    def mine_by_df(self):
        """
        """
        df = DataFrame(self.tree_builder.data,
                       columns=['ind', 'len', 'tag', 'text'])

        headers = df[df['tag'].isin(HEADER_TAGS)]

        # заголовка нет
        if headers.empty:
            # ищем медиану, она же центр статьи
            content = df[df['tag'].isin(CONTENT_TAGS)]
            biggest = content.sort_values(
                by='len', ascending=False)[:BIG_FOOT]

            # если статья имеет вес, то медиана правильна
            median = int(biggest['ind'].median())

        else:
            head = headers['ind'].iloc[0]
            print head
            indexes = df[head:][df['tag'].isin(CONTENT_TAGS)]['ind'].tolist()
            print indexes
            edge = self.get_edge(indexes)

            if edge is None:
                exit("Content is empty! Change CONTENT_TAGS!")

            print head, edge
            # print df[head:edge+1]
            print df[df['tag'].isin(CONTENT_TAGS)]

    def mine(self):
        """
        """

    @staticmethod
    def get_edge(indexes):
        """
        """
        edge = indexes[0] if indexes else None

        for i, next_ in enumerate(indexes[1:]):
            if edge + CONTENT_STEP <= next_:
                return edge
            edge = next_

        return edge

    def output(self):
        """
        """
        self.file_builder.write_content(
            self.headers, self.content)


if __name__ == '__main__':

    ArgsValidator.validate(sys.argv)

    url_ = sys.argv[1]

    miner = DataMiner(url_)
    miner.prepare()
    miner.mine_by_tree()
    miner.output()

    # print miner.content
    # for el in miner.content:
    #     print el.tag, el.text
    #     for a in el.iter(A_TAG):
    #         print a.tag, a.text, a.tail

    # tree = miner.tree_builder.tree
    # r = tree.getroot()
    # print dir(r)
    # headers = list(tree.iter(["h1", "h2"]))
    # print headers

    # for head in headers:
    #     for parent in head.iterancestors():
    #         contents = parent.iter(CONTENT_TAGS)
    #         texts = parent.itertext(CONTENT_TAGS)
    #
    #         l = len(reduce(operator.add, texts, ''))
    #
    #         if l > ARTICLE_MIN_SIZE:
    #             print texts
