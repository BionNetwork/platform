# coding: utf-8
from __future__ import unicode_literals, division

import os
import sys
import time
from core.models import Datasource


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
os.environ["DJANGO_SETTINGS_MODULE"] = "config.settings.production"

from celery import Celery, chord, chain
from bisect import bisect_left
import pandas as pd

from etl.constants import *
from etl.services.queue.base import *
from etl.services.middleware.base import EtlEncoder
from etl.services.datasource.base import DataSourceService
from etl.services.db.factory import LocalDatabaseService


logger = logging.getLogger(__name__)

ASC = 1


# FIXME chord на amqp не работает, поэтому redis
app = Celery('multi', backend='redis://localhost:6379/0',
             broker='redis://localhost:6379/0')


@app.task(name=CREATE_DATASET_MULTI)
def create_dataset_multi(task_id, channel, context):
    return CreateDatasetMulti(task_id, channel, context).load_data()


@app.task(name=MONGODB_DATA_LOAD_MULTI)
def load_mongo_db_multi(task_id, channel, context):
    return LoadMongodbMulti(task_id, channel, context).load_data()


class CreateDatasetMulti(TaskProcessing):
    """
    Создание Dataset
    """
    def processing(self):
        self.next_task_params = (load_mongo_db_multi, self.context)


class LoadMongodbMulti(TaskProcessing):
    """
    Создание Dataset
    """
    def processing(self):

        context = self.context
        card_id = context['card_id']
        pusher = Pusher(context['card_id'])
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
            create_foreign_table(sub_tree, is_mongodb=False)
            pusher.foreign_table_created(sub_tree['val'])
            create_view(sub_tree)
            pusher.view_created(sub_tree['val'])

        # mongo_callback(self.context)
        ClickHouse(self.context).run()
        pusher.warehouse_create()


@app.tasks(name=CREATE_DATE_TABLE)
def create_date_tables(card_id, sub_trees):
    #FIXME: реализовать через EtlBase
        local_db_service = DataSourceService.get_local_instance()
        local_db_service.create_date_tables(
            "time_table_name", sub_trees, False)


@app.task(name=PSQL_FOREIGN_TABLE)
def create_foreign_table(card_id, sub_tree, is_mongodb=True):
    """
    Создание Обертку над внешней таблицей
    Args:
        card_id(int): id карточки
        sub_tree(dict): Описать
    """
    CreateForeignTable(task_id=2, card_id=card_id, context=sub_tree)


@app.task(name=PSQL_VIEW)
def create_view(card_id, sub_tree):
    """
    Создание View для Foreign Table для внешнего источника
    Args:
        card_id(int): id карточки
        sub_tree(dict): Данные о таблице источника
    """
    CreateView(task_id=4, card_id=card_id, context=sub_tree).run()


@app.task(name=WAREHOUSE_LOAD)
def warehouse_load(card_id, context):
    """
    Загрузка данных в Хранилище данных
    Args:
        card_id(int): id карточки
        context(dict): Данные о загружаемых таблицах всех источников
    """
    LoadWarehouse(task_id=3, card_id=card_id, context=context).run()


# ------ Tasks ------------

class EtlBaseTask(object):
    """
    Основной класс задач ETL-процесса
    """

    def __init__(self, task_id, card_id, context, pusher=None):
        """
        Args:
            task_id(int): id Задачи
            context(dict): контекст задачи
            pusher(Pusher): Отправитель сообщений на клиент
        """
        self.task_id = task_id
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
    def process(self):
        source = Datasource.objects.get(id=int(self.context['sid']))
        if source.get_source_type() in ['Excel', 'Csv']:
            fdw = XlsForeignTable(tree=self.context)
        else:
            fdw = RdbmsForeignTable(tree=self.context)
        fdw.create()

    def post(self):
        self.pusher.push_view(self.context['val'])


class CreateView(EtlBaseTask):
    """
    Создание представления над foreign table
    """

    def process(self):
        local_service = DataSourceService.get_local_instance()
        local_service.create_foreign_view(self.context)

    def post(self):
        self.pusher.push_view(self.context['val'])


class LoadWarehouse(EtlBaseTask):
    """
    Загрузка данных в конечное хранилище
    """

    def process(self):
        """
        Загрузка в Clickhouse. Возможно следует реальзовать и вариант с созданием
        материализованных представлений для мер и размерностей в Postgres
        """
        ClickHouse(context=self.context).run()

    def post(self):
        self.pusher.warehouse_create()


class LoadMongoDB(EtlBaseTask):
    """
    Загрузка в MongoDB
    """

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
                self.context, cols=columns, limit=limit, offset=(page-1)*limit)

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
            if self.context['type'] == 'file':
                break

        # удаление всех со статусом PREV
        collection.delete_many({_STATE: TRSE.PREV},)

        # проставление всем статуса PREV
        collection.update_many({}, {'$set': {_STATE: TRSE.PREV}, })

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
    def db_url(self):

        raise NotImplementedError

    def create(self):
        """
        Создание ForeignTable в зависимости от типа источника
        """
        raise NotImplementedError

    def update(self):
        pass


class RdbmsForeignTable(BaseForeignTable):
    """
    Создание "удаленной таблицы" для РСУБД (Postgresql, MySQL, Oracle...)
    """

    @property
    def db_url(self):
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
            'db_url': self.db_url
        }

        self.service.create_foreign_table(self.name,
            self.server_name, table_options, self.tree['columns'])

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
    # FIXME: Требует реализации

    @property
    def server_name(self):
        return CSV_SERVER

    @property
    def db_url(self):
        """
        В качестве адреса базы данных полный путь до файла
        Return:
            str: Путь до файла
        """
        return ''

    def create(self):
        pass


class XlsForeignTable(CsvForeignTable):
    """
    Создание "удаленной таблицы" для файлов типа csv
    """

    @property
    def server_name(self):
        return CSV_SERVER

    @property
    def db_url(self):
        sid = int(self.tree['sid'])
        source = Datasource.objects.get(id=sid)
        return source.file.path

    def _xls_convert(self):
        """
        Преобразует excel лист в csv
        Returns:
            str: Название csv-файла

        """
        indexes = [x['order'] for x in self.tree['columns']]
        sheet_name = HashEncoder.encode(self.tree['val'])
        csv_file_name = '{file_name}__{sheet_name}.csv'.format(
            file_name=os.path.splitext(self.db_url)[0], sheet_name=sheet_name)
        data_xls = pd.read_excel(
            self.db_url, self.tree['val'], parse_cols=indexes, index_col=False)
        data_xls.to_csv(
            csv_file_name, header=indexes, encoding='utf-8', index=None)
        return csv_file_name

    def create(self):
        """
        Делаем конвертацию xls -> csv. В дальнейшем работаем с csv
        Returns:

        """

        csv_file_name = self._xls_convert()

        table_options = {
            'filename': csv_file_name,
            'skip_header': '1',
            'delimiter': ','
        }

        self.service.create_foreign_table(self.name,
            self.server_name, table_options, self.tree['columns'])

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

    def __init__(self, context, file_path='/tmp/', db_url='http://localhost:8123/'):
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
        local_service.create_sttm_select_query(self.file_path, file_name, self.context['relations'])

    def create_table(self):
        """
        Запрос на создание таблицы в Сlickhouse
        """
        col_types = []
        columns = []
        for tree in self.context['sub_trees']:
            for col in tree['columns']:
                col['name'] += tree['collection_hash']
                columns.append(col)
        for field in columns:
            col_types.append(u'{0} {1}'.format(
                field['name'], self.field_map[field['type']]))

        drop_query = """DROP TABLE IF EXISTS t_{table_name}""".format(
            table_name=self.table_name)

        create_query = """CREATE TABLE t_{table_name} ({columns}) engine = Log
            """.format(
            table_name=self.table_name, columns=','.join(col_types))

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
