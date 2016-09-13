# coding: utf-8
from django.core.management.base import BaseCommand, CommandError
from core.models import User
from etl.services.datasource.repository.storage import RedisSourceService


# FIXME запускать либо python manage.py handletasks <user_id>,
# FIXME либо python manage.py handletasks <user_id> -t <task_id>


# FIXME Переписать или удалить

class Command(BaseCommand):
    args = '<>'
    help = "Обработчик задач! Запускать:" \
           "1) Если таск не нужен - hanletasks <user_id> " \
           "2) Если таск нужен - handletasks <user_id> -t <task_id>"

    def add_arguments(self, parser):
        parser.add_argument('user_id', type=int, help='ID пользователя!')

        # optional
        parser.add_argument('-task_id', '-t', type=int,
                            help='ID таска на обработку!')

    def handle(self, *args, **options):
        user_id = options['user_id']
        task_id = options['task_id']

        try:
            User.objects.get(id=user_id)
        except User.DoesNotExist:
            raise CommandError(
                'Пользователя с таким ID={0} не существует!'.format(user_id))

        exist_task_ids = RedisSourceService.get_user_task_ids(user_id)

        exist_task_ids = list(map(int, exist_task_ids))

        if not exist_task_ids:
            raise CommandError(
                'Список тасков у пользователя с ID={0} пуст!'.format(user_id))

        if task_id:
            if task_id not in exist_task_ids:
                raise CommandError(
                    'У пользователя с ID={0} нет таска с ID={1}!'.format(
                        user_id, task_id))
        else:
            task_id = max(exist_task_ids)
        try:
            tasks.load_data(user_id, task_id)
        except Exception as e:
            raise CommandError(e.message)
