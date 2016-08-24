# coding: utf-8
from __future__ import unicode_literals, division

import os
import sys
import time


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
os.environ["DJANGO_SETTINGS_MODULE"] = "config.settings.production"

from celery import Celery, chord, chain
from bisect import bisect_left
import pandas as pd
from collections import defaultdict

from core.models import Datasource, ConnectionChoices as CC

from etl.constants import *
from etl.services.queue.base import *
from etl.services.middleware.base import EtlEncoder
from etl.services.datasource.base import DataSourceService
from etl.services.db.factory import LocalDatabaseService


logger = logging.getLogger(__name__)

ASC = 1


# FIXME chord на amqp бомбашится, паэтаму redis
app = Celery('multi', backend='redis://localhost:6379/0',
             broker='redis://localhost:6379/0')


@app.task(name=CREATE_DATASET_MULTI)
def create_dataset_multi(task_id, channel, context):
    return CreateDatasetMulti(task_id, channel, context).load_data()


@app.task(name=MONGODB_DATA_LOAD_MULTI)
def load_to_db_multi(task_id, channel, context):
    return LoadDbMulti(task_id, channel, context).load_data()


class CreateDatasetMulti(TaskProcessing):
    """
    Создание Dataset
    """
    def processing(self):
        self.next_task_params = (load_to_db_multi, self.context)


class LoadDbMulti(TaskProcessing):
    """
    Создание Dataset
    """
    def processing(self):

        context = self.context
        pusher = Pusher(context['card_id'])
        sub_trees = context['sub_trees']

        # FIXME проверить работу таблицы дат
        # создание таблицы дат
        local_db_service = DataSourceService.get_local_instance()
        local_db_service.create_date_tables(
            "time_table_name", sub_trees, False)

        # параллель из последований, в конце колбэк
        # chord(
        #     chain(
        #         load_to_mongo.subtask((sub_tree, )),
        #         # получает, то что вернул load_to_mongo
        #         create_foreign_table.subtask(),
        #         create_view.subtask()
        #     )
        #     for sub_tree in sub_trees)(
        #         mongo_callback.subtask(self.context))

        for sub_tree in sub_trees:
            create_foreign_table(sub_tree, is_mongodb=False)
            pusher.push_foreign_table(sub_tree['val'])
            create_view(sub_tree)
            pusher.push_view(sub_tree['val'])

        # mongo_callback(self.context)
        ClickHouseLoad(self.context).run()
        pusher.push_dim_meas()

        resp = get_responce(self.context)
        pusher.push_final(resp)


def get_responce(context):
    """
    Итоговый ответ о загрузках, включает ориг/хэш названия таблиц,
    ориг/новые названия колонок
    """
    sources_info = {
        "main_table": CLICK_TABLE.format(context['card_id']),
        "sources": defaultdict(list),
    }
    sources = sources_info["sources"]

    sub_trees = context['sub_trees']

    for sub_tree in sub_trees:

        table_hash = sub_tree["collection_hash"]

        sub_info = {
            "orig_table": sub_tree["val"],
            "hash_table": table_hash,
            "columns": [
                {
                    "orig_column": column["name"],
                    "click_column": CLICK_COLUMN.format(column['hash']),
                    "type": column["type"],
                } for column in sub_tree['columns']
            ],
        }

        sources[sub_tree["sid"]].append(sub_info)

    return sources_info


# FIXME remove?
def create_schema(tree):
    """
    Создание схемы
    Args:
        tree:

    Returns:

    """
    service = LocalDatabaseService()
    service.create_schema('schema_17')


@app.task(name=MONGODB_DATA_LOAD_MONO)
def load_to_mongo(sub_tree):
    """
    """
    t1 = time.time()

    limit = settings.ETL_COLLECTION_LOAD_ROWS_LIMIT

    page = 1
    sid = sub_tree['sid']

    source = Datasource.objects.get(id=sid)
    source_service = DataSourceService.get_source_service(source)

    col_names = ['_id', '_state', ]
    col_names += sub_tree["joined_columns"]

    key = sub_tree["collection_hash"]

    _ID, _STATE = '_id', '_state'

    # создаем коллекцию и индексы в Mongodb
    collection = MongodbConnection(
        '{0}_{1}'.format(STTM, key),
        indexes=[(_ID, ASC), (_STATE, ASC), ]
        ).collection

    loaded_count = 0

    columns = sub_tree['columns']

    while True:
        rows = source_service.get_source_rows(
            sub_tree, cols=columns, limit=limit, offset=(page-1)*limit)

        if not rows:
            break

        keys = []
        key_records = {}

        for ind, record in enumerate(rows, start=1):
            row_key = simple_key_for_row(record, (page - 1) * limit + ind)
            keys.append(row_key)

            record_normalized = (
                [row_key, TRSE.NEW, ] +
                [EtlEncoder.encode(rec_field) for rec_field in record])

            key_records[row_key] = dict(izip(col_names, record_normalized))

        exist_docs = collection.find({_ID: {'$in': keys}}, {_ID: 1})
        exist_ids = map(lambda x: x[_ID], exist_docs)
        exists_len = len(exist_ids)

        # при докачке, есть совпадения
        if exist_ids:
            # fixme либо сеты, либо бинари сёрч
            # на 5000 элементах вроде скорость одинакова
            # если что потом проверить и заменить
            # not_exist = set(keys) - set(exist_ids)
            # data_to_insert = [key_records[id_] for id_ in not_exist]

            data_to_insert = []
            exist_ids.sort()

            for id_ in keys:
                ind = bisect_left(exist_ids, id_)
                if ind == exists_len or not exist_ids[ind] == id_:
                    data_to_insert.append(key_records[id_])

            # смена статусов предыдущих на NEW
            collection.update_many(
                {_ID: {'$in': exist_ids}}, {'$set': {_STATE: TRSE.NEW}, }
            )

        # вся пачка новых
        else:
            data_to_insert = [key_records[id_] for id_ in keys]

        if data_to_insert:
            try:
                collection.insert_many(data_to_insert, ordered=False)
                loaded_count += ind
                print 'inserted %d rows to mongodb. Total inserted %s/%s.' % (
                    ind, loaded_count, 'rows_count')
            except Exception as e:
                print 'Exception', loaded_count, loaded_count + ind, e.message

        page += 1

        # FIXME у файлов прогон 1 раз
        # Fixme not true
        # Fixme подумать над пагинацией
        if sub_tree['type'] == 'file':
            break

    t2 = time.time()
    print 'xrange', t2 - t1

    # удаление всех со статусом PREV
    collection.delete_many({_STATE: TRSE.PREV},)

    # проставление всем статуса PREV
    collection.update_many({}, {'$set': {_STATE: TRSE.PREV}, })

    t3 = time.time()
    print 'xrange2', t3 - t2
    print 'xrange3', t3 - t1

    return sub_tree


@app.task(name=PSQL_FOREIGN_TABLE)
def create_foreign_table(sub_tree, is_mongodb=True):
    """
    Создание Удаленной таблицы
    """
    source = Datasource.objects.get(id=int(sub_tree['sid']))
    source_type = source.get_source_type()

    if source_type == CC.values.get(CC.EXCEL):
        fdw = XlsForeignTable(tree=sub_tree)
    elif source_type == CC.values.get(CC.CSV):
        fdw = CsvForeignTable(tree=sub_tree)
    else:
        fdw = RdbmsForeignTable(tree=sub_tree)
    fdw.create()


@app.task(name=PSQL_VIEW)
def create_view(sub_tree):
    """
    Создание View для Foreign Table со ссылкой на Дату
    Args:
        sub_tree(): Описать

    Returns:
    """
    local_service = DataSourceService.get_local_instance()

    local_service.create_foreign_view(sub_tree)


@app.task(name=MONGODB_DATA_LOAD_CALLBACK)
def mongo_callback(context):
    """
    Работа после создания
    """
    # fixme needs to check status of all subtasks
    # если какой нить таск упал, то сюда не дойдет
    # нужны декораторы на обработку ошибок
    local_service = DataSourceService.get_local_instance()
    cube_key = context["cube_key"]
    local_service.create_materialized_view(
        DIMENSIONS_MV.format(cube_key),
        MEASURES_MV.format(cube_key),
        context['relations']
    )


class BaseForeignTable(object):
    """
    Базовый класс для создания "удаленной таблицы"
    """

    def __init__(self, tree):
        """
        Args:
            tree(dict): Метаинформация о создаваемой таблице
        """
        self.tree = tree
        self.name = '{0}{1}'.format(STTM, self.tree["collection_hash"])
        self.service = LocalDatabaseService()

    @property
    def server_name(self):
        raise NotImplementedError

    @property
    def source_url(self):
        raise NotImplementedError

    @property
    def options(self):
        raise NotImplementedError

    def create(self):
        self.service.create_foreign_table(
            self.name, self.server_name, self.options, self.tree['columns'])


class RdbmsForeignTable(BaseForeignTable):
    """
    Создание "удаленной таблицы" для РСУБД (Postgresql, MySQL, Oracle...)
    """

    @property
    def server_name(self):
        return RDBMS_SERVER

    @property
    def source_url(self):
        sid = int(self.tree['sid'])
        source = Datasource.objects.get(id=sid)
        return '{db_type}://{login}:{password}@{host}:{port}/{db}'.format(
            db_type='postgresql',  # FIXME: Доделать для остальных типов баз данных
            login=source.login,
            password=source.password,
            host=source.host,
            port=source.port,
            db=source.db,
        )

    @property
    def options(self):
        return {
            # 'schema': 'mgd',
            'tablename': self.tree['val'],
            'db_url': self.source_url,
        }

    def update(self):
        """
        При работе с РСУБД реализация обновления не нужна
        Returns:
        """
        pass


class CsvForeignTable(BaseForeignTable):

    @property
    def server_name(self):
        return CSV_SERVER

    @property
    def source_url(self):
        sid = int(self.tree['sid'])
        source = Datasource.objects.get(id=sid)
        return source.get_file_path()

    @property
    def options(self):
        """
        Returns: dict
        """
        return {
            'filename': self.source_url,
            'skip_header': '1',
            'delimiter': ','
        }


class XlsForeignTable(CsvForeignTable):
    """
    Создание "удаленной таблицы" для файлов типа csv
    """

    def _xls_convert(self):
        """
        Преобразует excel лист в csv
        Returns:
            str: Название csv-файла
        """
        indexes = [x['order'] for x in self.tree['columns']]
        sheet_name = abs(HashEncoder.encode(self.tree['val']))

        csv_file_name = '{file_name}_{sheet_name}.csv'.format(
            file_name=os.path.splitext(self.source_url)[0],
            sheet_name=sheet_name)

        data_xls = pd.read_excel(
            self.source_url, self.tree['val'],
            parse_cols=indexes, index_col=False)
        data_xls.to_csv(
            csv_file_name, header=indexes, encoding='utf-8', index=None)

        return csv_file_name

    @property
    def options(self):
        """
        Делаем конвертацию xls -> csv. В дальнейшем работаем с csv
        Returns:
        """
        csv_file_name = self._xls_convert()

        return {
            'filename': csv_file_name,
            'skip_header': '1',
            'delimiter': ','
        }

    def update(self):
        pass


class MongoForeignTable(BaseForeignTable):
    """
    Создание "удаленной таблицы" для Mongodb
    """


class ClickHouseLoad(object):
    """
    Загрузка в ClickHouse
    """

    field_map = {
        'text': 'String',
        'integer': 'Int16',
        'datetime': 'DateTime',
        'date': 'Date',
    }

    def __init__(self, context, file_path='/tmp/',
                 db_url='http://localhost:8123/'):
        self.context = context
        self.db_url = db_url
        self.file_path = file_path
        self.table_name = self.context["cube_key"]

    def create_csv(self):
        """
        Создание csv-файла из запроса в Postgres
        """
        file_name = self.table_name
        local_service = DataSourceService.get_local_instance()
        local_service.create_sttm_select_query(
            self.file_path, file_name, self.context['relations'])

    def create_table(self):
        """
        Запрос на создание таблицы в Сlickhouse
        """
        col_types = []

        for tree in self.context['sub_trees']:
            for col in tree['columns']:
                col_types.append(u'{0} {1}'.format(
                    CLICK_COLUMN.format(col['hash']), self.field_map[col['type']]))

        drop_query = """DROP TABLE IF EXISTS t_{table_name}""".format(
            table_name=self.table_name)

        create_query = """CREATE TABLE {table_name} ({columns}) engine = Log
            """.format(
            table_name=CLICK_TABLE.format(self.table_name),
            columns=','.join(col_types))

        self._send([drop_query, create_query])

    def load_csv(self):
        """
        Загрузка данных из csv в Clickhouse
        """
        os.system(
            """
            cat /tmp/{file}.csv |
            clickhouse-client --query="INSERT INTO t_{table} FORMAT CSV"
            """.format(
                file=self.table_name, table=self.table_name))

    def _send(self, data, settings=None, stream=False):
        """
        """
        for query in data:
            r = requests.post(self.db_url, data=query, stream=stream)
            if r.status_code != 200:
                raise Exception(r.text)

    def run(self):
        self.create_csv()
        self.create_table()
        self.load_csv()


# write in console: python manage.py celery -A etl.multitask worker
#                   --loglevel=info --concurrency=10
#                   (--concurrency=1000 -P (eventlet, gevent)
