# coding: utf-8
from __future__ import unicode_literals, division

import os
import pandas as pd

from core.models import Datasource

from etl.constants import *
from etl.services.middleware.base import HashEncoder
from etl.services.db.factory import LocalDatabaseService


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

        # FIXME temporary drop all empty values
        # dropna_df = data_xls.dropna()

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
