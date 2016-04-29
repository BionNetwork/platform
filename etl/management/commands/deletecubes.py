# coding: utf-8

__author__ = 'damir(GDR)'

from django.core.management.base import BaseCommand, CommandError
from core.models import (DatasourceMetaKeys, DatasourceMeta,
                         Dimension, Measure, DatasetToMeta, Cube, Dataset)
from etl.services.datasource.base import DataSourceService


class Command(BaseCommand):

    help = u'Удаляет все кубы указанного источника! '\
           u'Запускать python manage.py deletecubes <datasource_id>!'

    def add_arguments(self, parser):
        parser.add_argument('datasource_id', type=int)

    def handle(self, *args, **options):

        instance = DataSourceService.get_local_instance().datasource
        connection = instance.connection
        cursor = connection.cursor()

        datasource_id = options['datasource_id']

        metas = DatasourceMeta.objects.filter(datasource_id=datasource_id)
        meta_ids = metas.values_list('id', flat=True)
        print 'meta_ids', meta_ids

        meta_keys = DatasourceMetaKeys.objects.filter(meta_id__in=meta_ids)
        meta_values = meta_keys.values_list('value', flat=True)
        print 'meta_values', meta_values

        for value in meta_values:
            for t_name in ['sttm_datasource_', 'dimensions_', 'measures_']:
                cursor.execute("drop table if exists {0}{1}".format(t_name, value))
            connection.commit()

        Dimension.objects.filter(datasources_meta_id__in=meta_ids).delete()
        Measure.objects.filter(datasources_meta_id__in=meta_ids).delete()

        cubes_name = ['cube_%s' % x for x in meta_values]
        Cube.objects.filter(name__in=cubes_name).delete()

        dtms = DatasetToMeta.objects.filter(meta__in=metas)
        Dataset.objects.filter(
            id__in=dtms.values_list('dataset', flat=True)).delete()
        dtms.delete()

        meta_keys.delete()
        metas.delete()
