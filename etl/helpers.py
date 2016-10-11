# coding: utf-8

from collections import defaultdict
import decimal
import datetime


def group_by_source(columns_info):
    """
    Группировка по соурсам, на всякий пожарный перед загрузкой
    """
    sid_grouped = defaultdict(dict)

    for sid, tables in columns_info.items():
        sid_grouped[str(sid)].update(tables)

    return dict(sid_grouped)


def extract_tables_info(columns):

    tables_dict = {}

    for sid, tables in columns.items():
        tables_dict[sid] = list(tables.keys())
    return tables_dict


class EtlEncoder:
    @staticmethod
    def encode(obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%d.%m.%Y')
        elif isinstance(obj, datetime.date):
            return obj.strftime('%d.%m.%Y')
        elif isinstance(obj, decimal.Decimal):
            return float(obj)
        return obj


class HashEncoder(object):
    """
    Базовый класс для хэширования данных
    """

    @staticmethod
    def encode(data):
        """
        Кодирование данных
        Args:
            data(object): list, dict, str данные для кодирования
        Returns:
            object(int): integer представление
        """
        return hash(data)


def datetime_now_str():
    """
    Нынешнее время в строковой форме
    """
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class DatasetContext(object):
    """
    Контекст выполениния задач
    """

    def __init__(self, cube_id, is_update=False, sources_info=None):
        """

        Args:
            cube_id(int): id Карточки
            is_update(bool): Обновление?
            sources_info(dict): Данные загрузки
            ::
            ['<source_1>':
                {
                    "shops": ['<column_1>, <column_2>, <column_3>]
                },
            '<source_2>':
                {
                    "<table_1>": [<column_1>, <column_2>],
                    "<table_2": [<column_1>, <column_2>]
                }
            ...

        ]
        """
        self.cube_id = cube_id
        self.is_update = is_update
        self.sources_info = sources_info
        self.cube_service = DataCubeService(cube_id=cube_id)

        self.dataset, self.is_new = Dataset.objects.get_or_create(key=cube_id)

    @property
    def context(self):
        """
        Контекста выполнения задач

        Returns:
            dict:
        """
        # if not self.dataset.context:
        sub_trees = self.cube_service.prepare_sub_trees(self.sources_info)
        for sub_tree in sub_trees:
            sub_tree.update(
                view_name='{type}{view_hash}'.format(
                    type=VIEW, view_hash=sub_tree['collection_hash']),
                table_name='{type}{view_hash}'.format(
                    type=STTM, view_hash=sub_tree['collection_hash']))
            for column in sub_tree['columns']:
                column.update(click_column=CLICK_COLUMN.format(column['hash']))

        relations = self.cube_service.prepare_relations(sub_trees)
        self.is_new = False
        return {
            'warehouse': CLICK_TABLE.format(self.cube_id),
            'cube_id': self.cube_id,
            'is_update': False,
            'sub_trees': sub_trees,
            "relations": relations,
        }
        # else:
        #     return self.dataset.context

    def create_dataset(self):
        if self.is_new:
            self.dataset.key = self.cube_id
            self.dataset.context = self.context
            self.dataset.state = DatasetStateChoices.IDLE
            self.dataset.save()
        else:
            self.dataset.context = self.context
            self.dataset.save()
            # raise ContextError('Dataset already exist')

    @property
    def state(self):
        return self.dataset.state

    @state.setter
    def state(self, value):
        self.dataset.state = value
        self.dataset.save()


class ContextError(Exception):
    pass

