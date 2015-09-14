# coding: utf-8
from __future__ import unicode_literals

import psycopg2
import MySQLdb

from core.models import ConnectionChoices


def check_connection(post):
    conn_info = {
        'host': post.get('host', ''),
        'user': post.get('login', ''),
        'passwd': post.get('password', ''),
        'db': post.get('name', ''),
        'port': int(post.get('port', ''))
    }

    conn_type = int(post.get('conn_type', ''))

    if conn_type == ConnectionChoices.POSTGRESQL:
        try:
            conn_str = ("host='{host}' dbname='{db}' user='{user}' "
                        "password='{passwd}' port={port}").format(**conn_info)
            psycopg2.connect(conn_str)
        except psycopg2.OperationalError:
            return False
    elif conn_type == ConnectionChoices.MYSQL:
        try:
            MySQLdb.connect(**conn_info)
        except MySQLdb.OperationalError:
            return False

    return True
