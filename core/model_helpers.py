# -*- coding: utf-8 -*-


import six

from django.db import transaction, router
from django.db.models.deletion import Collector
from django.db.models import signals, sql
from django.db.models.query_utils import Q


__author__ = 'damir(GDR)'


class MultiPrimaryKeyModel(object):

    def delete(self, using=None):
        using = using or router.db_for_write(self.__class__, instance=self)

        collector = CustomDeleteCollector(using=using)
        collector.collect([self])
        collector.delete()


class CustomDeleteQuery(sql.DeleteQuery):
    """
    Модель формирования sql запроса при удалении инстанса модели
    """
    def delete_batch(self, fields_values, using, field=None):

        self.where = self.where_class()
        for field, value in list(fields_values.items()):
            self.add_q(Q(**{field + '__in': [value, ]}))

        self.do_query(self.get_meta().db_table, self.where, using=using)


class CustomDeleteCollector(Collector):
    """
    Модель коллектора, участвующий при удалении инстанса модели
    """

    def delete(self):
        """
        Обрезанный метод, работает только при удалении экземпляра класса,
        не работает при удалении QuerySet-a
        """

        # sort instance collections
        (model, instance_set) = list(self.data.items())[0]
        instance = list(instance_set)[0]
        self.sort()
        with transaction.atomic(using=self.using, savepoint=False):
            # send pre_delete signals
            if not model._meta.auto_created:
                signals.pre_delete.send(
                    sender=model, instance=instance, using=self.using
                )

             # delete instance
            query = CustomDeleteQuery(model)

            fields_values = {field.attname: getattr(instance, field.attname)
                             for field in [f for f in model._meta.fields]}

            query.delete_batch(fields_values, self.using)

            # send post_delete signals
            if not model._meta.auto_created:
                signals.post_delete.send(
                    sender=model, instance=instance, using=self.using
                )

        for model, instances in six.iteritems(self.data):
            for instance in instances:
                setattr(instance, model._meta.pk.attname, None)
