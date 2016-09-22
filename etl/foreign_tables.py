# coding: utf-8


import os

import pandas as pd

from core.models import Datasource
from etl.constants import *
from etl.helpers import HashEncoder
from etl.services.datasource.db.factory import LocalDatabaseService


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

    @property
    def cols_list(self):
        """
        список колонок с типами для Foreign Table
        """
        raise NotImplementedError

    def create(self):

        select_cols = ','.join(self.cols_list)

        self.service.create_foreign_table(
            self.name, self.server_name, self.options, select_cols)

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

    @property
    def cols_list(self):

        columns = self.tree['columns']
        result = []

        for field in columns:
            result.append('"{0}" {1}'.format(
                field['name'], field['type']))
        return result

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

    DATES = {
        # FIXME потом '0000-00-00 00:00:00' засунуть for timestamp & datetime
        "timestamp": {'val': '0000-00-00'},
        "datetime": {'val': '0000-00-00'},
        "date": {'val': '0000-00-00'},
    }
    DATE_DEFAULT = {'val': '0000-00-00'}

    NUMERIC = {
        "integer": {'val': 0, 'type': int},
        "double precision": {'val': 0, 'type': float},
    }
    NUMERIC_DEFAULT = {'val': 0, 'type': int}

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

    # FIXME for files set text type all columns
    @property
    def cols_list(self):

        columns = self.tree['columns']
        result = []

        for field in columns:
            result.append('"{0}" {1}'.format(
                field['name'], 'text'))
        return result


class XlsForeignTable(CsvForeignTable):
    """
    Создание "удаленной таблицы" для файлов типа csv
    """
    def read_excel_necols(self, *args, **kwargs):
        """
        Открытие файла без пустых колонок(если без title)
        """
        df = pd.read_excel(*args, **kwargs)
        columns = df.columns
        ne_columns = [
            col for col in columns
            if not (str(col).startswith('Unnamed: ') and
                    not df[col].notnull().any())
            ]
        return df[ne_columns]

    def _xls_convert(self):
        """
        Преобразует excel лист в csv
        Returns:
            str: Название csv-файла
        """
        columns = self.tree['columns']

        indexes = [x['order'] for x in columns]
        dates = [x for x in columns if x['type'] in self.DATES]
        numeric = [x for x in columns if x['type'] in self.NUMERIC]

        val = self.tree['val']
        sheet_name = abs(HashEncoder.encode(val))

        csv_file_name = '{file_name}_{sheet_name}.csv'.format(
            file_name=os.path.splitext(self.source_url)[0],
            sheet_name=sheet_name)

        indent = self.tree['indents'][val]

        data_xls = self.read_excel_necols(
            self.source_url, val, skiprows=indent,
            parse_cols=indexes, index_col=False)

        # process columns here for click (dates, timestamps)
        for dat_col in dates:
            n = dat_col['name']
            t = dat_col['type']
            data_xls[n] = pd.to_datetime(
                data_xls[n], errors='coerce').dt.date.fillna(# dt.date for date
                self.DATES.get(t, self.DATE_DEFAULT)['val'])

        for num_col in numeric:
            n = num_col['name']
            t = num_col['type']
            data_xls[n] = pd.to_numeric(
                data_xls[n], errors='coerce').fillna(0).astype(
                self.NUMERIC.get(t, self.NUMERIC_DEFAULT)['type'])

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
