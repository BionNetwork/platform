# coding: utf-8
from __future__ import unicode_literals, division

import os
import sys
import time

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

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
os.environ["DJANGO_SETTINGS_MODULE"] = "config.settings.production"

logger = logging.getLogger(__name__)

ASC = 1

# FIXME chord на amqp не работает, поэтому redis
app = Celery('multi', backend='redis://localhost:6379/0',
             broker='redis://localhost:6379/0')


def create_dataset_multi(task_id, channel, context):
    return load_data(context)


def load_data(context):
    context = context
    card_id = context['card_id']
    pusher = Pusher(card_id)
    sub_trees = context['sub_trees']

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
        create_foreign_table(card_id, sub_tree, pusher)
        create_view(card_id, sub_tree, pusher)

    warehouse_load(card_id, context, pusher)


# FIXME: реализовать через EtlBase
@app.task(name=CREATE_DATE_TABLE)
def create_date_tables(sub_trees):
    """
    Создание таблицы дат
    Args:
        sub_trees:
    Returns:
    """
    local_db_service = DataSourceService.get_local_instance()
    local_db_service.create_date_tables(
        "time_table_name", sub_trees, False)


@app.task(name=PSQL_FOREIGN_TABLE)
def create_foreign_table(card_id, sub_tree, pusher):
    """
    Создание Обертки над внешней таблицей
    Args:
        pusher(Pusher): Отправитель информации на клиент
        card_id(int): id карточки
        sub_tree(dict): Описать
    """
    CreateForeignTable(card_id=card_id, context=sub_tree, pusher=pusher).run()


@app.task(name=PSQL_VIEW)
def create_view(card_id, sub_tree, pusher):
    """
    Создание View для Foreign Table для внешнего источника
    Args:
        pusher(Pusher): Отправитель информации на клиент
        card_id(int): id карточки
        sub_tree(dict): Данные о таблице источника
    """
    CreateView(card_id=card_id, context=sub_tree, pusher=pusher).run()


@app.task(name=WAREHOUSE_LOAD)
def warehouse_load(card_id, context, pusher):
    """
    Загрузка данных в Хранилище данных
    Args:
        pusher(Pusher): Отправитель информации на клиент
        card_id(int): id карточки
        context(dict): Данные о загружаемых таблицах всех источников
    """
    LoadWarehouse(card_id=card_id, context=context, pusher=pusher).run()


# ------ Tasks ------------

class EtlBaseTask(object):
    """
    Основной класс задач ETL-процесса
    """
    task_name = None

    def __init__(self, card_id, context, pusher=None):
        """
        Args:
            card_id(int): id карточки
            context(dict): контекст задачи
            pusher(Pusher): Отправитель сообщений на клиент
        """
        self.task_id = TaskService(self.task_name).add_task(arguments=context)
        self.context = context
        self.pusher = pusher or Pusher(card_id)

    def pre(self):
        """
        Подготовка данных в рамках задачи
        """
        pass

    def post(self):
        """
        Постобработка
        """
        pass

    def run(self):
        try:
            self.pre()
            self.process()
            self.post()
        except:
            raise Exception

    def process(self):
        """
        Основное тело задачи
        """
        raise NotImplementedError


class CreateForeignTable(EtlBaseTask):
    """
    Создание обертки над подключенными внешними таблицами
    """

    task_name = PSQL_FOREIGN_TABLE

    def process(self):
        source = Datasource.objects.get(id=int(self.context['sid']))
        source_type = source.get_source_type()
    
        if source_type == CC.values.get(CC.EXCEL):
            fdw = XlsForeignTable(tree=self.context)
        elif source_type == CC.values.get(CC.CSV):
            fdw = CsvForeignTable(tree=self.context)
        else:
            fdw = RdbmsForeignTable(tree=self.context)
        fdw.create()

    def post(self):
        self.pusher.push_view(self.context['val'])


class CreateView(EtlBaseTask):
    """
    Создание представления над foreign table
    """

    task_name = PSQL_VIEW

    def process(self):
        local_service = DataSourceService.get_local_instance()
        local_service.create_foreign_view(self.context)

    def post(self):
        self.pusher.push_view(self.context['val'])


class LoadWarehouse(EtlBaseTask):
    """
    Загрузка данных в конечное хранилище
    """

    task_name = WAREHOUSE_LOAD

    def process(self):
        """
        Загрузка в Clickhouse. Возможно следует реальзовать и вариант с созданием
        материализованных представлений для мер и размерностей в Postgres
        """
        ClickHouse(context=self.context).run()

    def get_response(self):
        """
        Итоговый ответ о загрузках, включает ориг/хэш названия таблиц,
        ориг/новые названия колонок
        """
        sources_info = {
            "main_table": CLICK_TABLE.format(self.context['card_id']),
            "sources": defaultdict(list),
        }
        sources = sources_info["sources"]

        sub_trees = self.context['sub_trees']

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

    def post(self):
        self.pusher.push_final(data=self.get_response())


class LoadMongoDB(EtlBaseTask):
    """
    Загрузка в MongoDB
    """

    task_name = MONGODB_DATA_LOAD_MULTI

    def process(self):

        limit = settings.ETL_COLLECTION_LOAD_ROWS_LIMIT

        page = 1
        sid = self.context['sid']

        source = Datasource.objects.get(id=sid)
        source_service = DataSourceService.get_source_service(source)

        col_names = ['_id', '_state', ]
        col_names += self.context["joined_columns"]

        key = self.context["collection_hash"]

        _ID, _STATE = '_id', '_state'

        # создаем коллекцию и индексы в Mongodb
        collection = MongodbConnection(
            '{0}_{1}'.format(STTM, key),
            indexes=[(_ID, ASC), (_STATE, ASC), ]
        ).collection

        loaded_count = 0

        columns = self.context['columns']

        while True:
            rows = source_service.get_source_rows(
                self.context, cols=columns, limit=limit, offset=(page - 1) * limit)

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
                    {_ID: {'$in': exist_ids}}, {'$set': {_STATE: TRSE.NEW},}
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
            if self.context['type'] == 'file':
                break

        # удаление всех со статусом PREV
        collection.delete_many({_STATE: TRSE.PREV}, )

        # проставление всем статуса PREV
        collection.update_many({}, {'$set': {_STATE: TRSE.PREV},})

        return self.context


# ------ !Tasks ------------


# ------ Foreign Tables ------------
class BaseForeignTable(object):
    """
    Базовый класс обертки над внешними источниками
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

    def update(self):
        pass


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
    def server_name(self):
        return RDBMS_SERVER

    def create(self):
        table_options = {
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
    """
    Обертка над csv-файлами
    """

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


# ------ !Foreign Tables ----------


# ------ Warehouse Load ----------

class WareHouse(object):
    """
    Базовый класс, описывающий хранилище данных
    """

    def __init__(self, context):
        """
        Args:
            context(dict): Контекст выполнения
        """
        self.context = context

    def run(self):
        raise NotImplementedError


class ClickHouse(WareHouse):
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
        super(ClickHouse, self).__init__(context=context)

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


class MaterializedView(WareHouse):
    """
    Класс описывает материализованное представление.

    !!Загрузка в материализованное представление на данный момент является
    альтернативой загрузки в clickhouse
    """

    def run(self):
        # fixme needs to check status of all subtasks
        # если какой нить таск упал, то сюда не дойдет
        # нужны декораторы на обработку ошибок
        local_service = DataSourceService.get_local_instance()
        cube_key = self.context["cube_key"]
        local_service.create_materialized_view(
            DIMENSIONS_MV.format(cube_key),
            MEASURES_MV.format(cube_key),
            self.context['relations']
        )

    print 'MEASURES AND DIMENSIONS ARE MADE!'

# ------ Warehouse Load ----------


# write in console: python manage.py celery -A etl.multitask worker
#                   --loglevel=info --concurrency=10
#                   (--concurrency=1000 -P (eventlet, gevent)
