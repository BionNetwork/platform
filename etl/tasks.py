# coding: utf-8


import os
import sys

from celery import Celery, chord, chain
from bisect import bisect_left
from collections import defaultdict

from core.models import (
    Datasource, Dataset, DatasetStateChoices,
    ConnectionChoices as CC, Columns, ColumnTypeChoices as CTC)

from etl.constants import *
from etl.services.queue.base import *
from etl.helpers import EtlEncoder
from etl.services.datasource.base import DataSourceService

from etl.foreign_tables import (
    RdbmsForeignTable, CsvForeignTable, XlsForeignTable)
from etl.warehouse import ClickHouse, PostgresWarehouse

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
os.environ["DJANGO_SETTINGS_MODULE"] = "config.settings.production"

logger = logging.getLogger(__name__)

ASC = 1

# FIXME chord на amqp не работает, поэтому redis
app = Celery('multi', backend='redis://localhost:6379/0',
             broker='redis://localhost:6379/0')


def load_data(context):
    """
    Сценарий загрузки данных
    Args:
        context:

    Returns:

    """
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

    # create_dataset(card_id, sub_trees, pusher)

    for sub_tree in sub_trees:
        create_foreign_table(card_id, sub_tree, pusher)
        create_view(card_id, sub_tree, pusher)

    warehouse_load(card_id, context, pusher)

    return LoadWarehouse(
        card_id=card_id, context=context, pusher=pusher).get_response()


def update_data(context):
    """
    Сценарий обновления данных
    Args:
        context:

    Returns:

    """
    pass


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


@app.task(name=CREATE_DATASET)
def create_dataset(card_id, sub_tree, pusher):
    """
    Создание хранилища
    """
    CreateDataset(card_id, sub_tree, pusher).run()


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


@app.task(name=META_INFO_SAVE)
def meta_info_save(card_id, context, pusher):
    """
    Загрузка данных в Хранилище данных
    Args:
        pusher(Pusher): Отправитель информации на клиент
        card_id(int): id карточки
        context(dict): Данные о загружаемых таблицах всех источников
    """
    MetaInfoSave(card_id=card_id, context=context, pusher=pusher).run()


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
        self.card_id = card_id
        # TODO save context and rewrite TaskService and Queue models
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
        except Exception as e:
            print(e.message)
            raise Exception

    def process(self):
        """
        Основное тело задачи
        """
        raise NotImplementedError


class CreateDataset(EtlBaseTask):
    """
    Создание основного датасета
    """

    task_name = CREATE_DATASET

    def process(self):
        dataset, created = Dataset.objects.get_or_create(key=self.card_id)

        # меняем статус dataset
        Dataset.update_state(dataset.id, DatasetStateChoices.IDLE)


class CreateForeignTable(EtlBaseTask):
    """
    Создание обертки над подключенными внешними таблицами
    """

    task_name = PSQL_FOREIGN_TABLE

    def pre(self):
        """

        """
        dataset = Dataset.objects.get(key=self.card_id)
        dataset.state = DatasetStateChoices.DIMCR
        dataset.save()

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

    def pre(self):
        """

        """
        # FIXME: Обновить Dataset

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

    def pre(self):
        """

        """
        # FIXME: Обновить Dataset

    def process(self):
        """
        Загрузка в Clickhouse. Возможно следует реальзовать и вариант с созданием
        материализованных представлений для мер и размерностей в Postgres
        """
        PostgresWarehouse(context=self.context).run()

    def get_response(self):
        """
        Итоговый ответ о загрузках, включает ориг/хэш названия таблиц,
        ориг/новые названия колонок
        """
        sources_info = {
            "main_table": self.context['warehouse'],
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
                        "click_column": column["click_column"],
                        "type": column["type"],
                    } for column in sub_tree['columns']
                ],
            }

            sources[sub_tree["sid"]].append(sub_info)

        return sources_info

    def post(self):
        meta_info_save(self.card_id, self.context, self.pusher)
        self.pusher.push_final(data=self.get_response())


class MetaInfoSave(EtlBaseTask):
    """
    Сохранение в базе информации о колонках куба
    Сохранение в базе метаданных о колонках источников
    """

    task_name = META_INFO_SAVE

    def process(self):

        dataset_id = self.card_id

        sub_trees = self.context['sub_trees']

        # FIXME temporary delete all old meta columns info
        Columns.objects.filter(dataset__key=dataset_id).delete()

        dataset = Dataset.objects.get(key=dataset_id)

        for sub_tree in sub_trees:
            orig_table = sub_tree['val']
            source_id = sub_tree['sid']
            for column in sub_tree['columns']:
                Columns.objects.create(
                    dataset=dataset,
                    original_table=orig_table,
                    original_name=column['name'],
                    name=CLICK_COLUMN.format(column['hash']),
                    # name=column['name'],
                    source_id=source_id,
                    type=CTC.get_type(column['type']),
                )


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

                key_records[row_key] = dict(zip(col_names, record_normalized))

            exist_docs = collection.find({_ID: {'$in': keys}}, {_ID: 1})
            exist_ids = [x[_ID] for x in exist_docs]
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
                    print('inserted %d rows to mongodb. Total inserted %s/%s.' % (
                        ind, loaded_count, 'rows_count'))
                except Exception as e:
                    print('Exception', loaded_count, loaded_count + ind, e.message)

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


# write in console: python manage.py celery -A etl.multitask worker
#                   --loglevel=info --concurrency=10
#                   (--concurrency=1000 -P (eventlet, gevent)
