# -*- coding: utf-8 -*-


from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0024_cube'),
    ]

    operations = [
        migrations.CreateModel(
            name='Dataset',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('key', models.TextField(unique=True, verbose_name='\u041a\u043b\u044e\u0447')),
                ('date_created', models.DateTimeField(auto_now_add=True, verbose_name='\u0414\u0430\u0442\u0430 \u0441\u043e\u0437\u0434\u0430\u043d\u0438\u044f', db_index=True)),
                ('update_date', models.DateTimeField(auto_now=True, verbose_name='\u0414\u0430\u0442\u0430 \u043e\u0431\u043d\u043e\u0432\u043b\u0435\u043d\u0438\u044f', db_index=True)),
                ('state', models.SmallIntegerField(default=1, db_index=True, verbose_name='\u0421\u0442\u0430\u0442\u0443\u0441', choices=[(1, '\u0412 \u043e\u0436\u0438\u0434\u0430\u043d\u0438\u0438 \u0434\u0430\u043d\u043d\u044b\u0445'), (2, '\u041d\u0430\u043f\u043e\u043b\u043d\u0435\u043d\u0438\u0435 \u0434\u0430\u043d\u043d\u044b\u0445'), (3, '\u0421\u043e\u0437\u0434\u0430\u043d\u0438\u0435 \u0440\u0430\u0437\u043c\u0435\u0440\u043d\u043e\u0441\u0442\u0435\u0439'), (4, '\u0421\u043e\u0437\u0434\u0430\u043d\u0438\u0435 \u043c\u0435\u0440'), (5, '\u0417\u0430\u0433\u0440\u0443\u0437\u043a\u0430 \u0434\u0430\u043d\u043d\u044b\u0445 \u0437\u0430\u0432\u0435\u0440\u0448\u0438\u043b\u0430\u0441\u044c')])),
            ],
            options={
                'db_table': 'datasets',
            },
        ),
    ]
