# -*- coding: utf-8 -*-


from django.db import models, migrations
import core.helpers


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0034_auto_20160331_1607'),
    ]

    operations = [
        migrations.AddField(
            model_name='datasource',
            name='file',
            field=models.FileField(max_length=500, null=True, verbose_name='\u0424\u0430\u0439\u043b', upload_to=core.helpers.users_file_upload_path),
        ),
        migrations.AddField(
            model_name='datasource',
            name='name',
            field=models.CharField(max_length=255, null=True, verbose_name='\u041d\u0430\u0437\u0432\u0430\u043d\u0438\u0435 \u0438\u0441\u0442\u043e\u0447\u043d\u0438\u043a\u0430'),
        ),
        migrations.AlterField(
            model_name='datasource',
            name='conn_type',
            field=models.SmallIntegerField(default=1, verbose_name='\u0422\u0438\u043f \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0435\u043d\u0438\u044f', choices=[(1, 'Postgresql'), (2, 'Mysql'), (3, 'MsSql'), (4, 'Oracle'), (5, 'File')]),
        ),
        migrations.AlterField(
            model_name='datasource',
            name='db',
            field=models.CharField(help_text='\u0411\u0430\u0437\u0430 \u0434\u0430\u043d\u043d\u044b\u0445', max_length=255, null=True),
        ),
        migrations.AlterField(
            model_name='datasource',
            name='host',
            field=models.CharField(help_text='\u0438\u043c\u044f \u0445\u043e\u0441\u0442\u0430', max_length=255, null=True, db_index=True),
        ),
        migrations.AlterField(
            model_name='datasource',
            name='port',
            field=models.IntegerField(help_text='\u041f\u043e\u0440\u0442 \u043f\u043e\u0434\u043a\u043b\u044e\u0447\u0435\u043d\u0438\u044f', null=True),
        ),
    ]
