# -*- coding: utf-8 -*-


from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0011_dimension'),
    ]

    operations = [
        migrations.CreateModel(
            name='DatasourceMetaKeys',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('value', models.CharField(max_length=255, verbose_name='\u041a\u043b\u044e\u0447')),
                ('meta', models.ForeignKey(related_name='meta_keys', verbose_name='\u041c\u0435\u0442\u0430\u0434\u0430\u043d\u043d\u044b\u0435', to='core.DatasourceMeta')),
            ],
            options={
                'db_table': 'datasources_meta_keys',
            },
        ),
        migrations.AlterUniqueTogether(
            name='datasourcemetakeys',
            unique_together=set([('meta', 'value')]),
        ),
    ]
