# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations
import etl.models


class Migration(migrations.Migration):

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='App',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(max_length=255)),
                ('module', models.CharField(max_length=255)),
            ],
        ),
        migrations.CreateModel(
            name='Field',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(max_length=255)),
                ('type', models.CharField(max_length=255, validators=[etl.models.is_valid_field])),
            ],
        ),
        migrations.CreateModel(
            name='Model',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(max_length=255)),
                ('app', models.ForeignKey(related_name='models', to='etl.App')),
            ],
        ),
        migrations.CreateModel(
            name='Setting',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(max_length=255)),
                ('value', models.CharField(max_length=255)),
                ('field', models.ForeignKey(related_name='settings', to='etl.Field')),
            ],
        ),
        migrations.AddField(
            model_name='field',
            name='model',
            field=models.ForeignKey(related_name='fields', to='etl.Model'),
        ),
        migrations.AlterUniqueTogether(
            name='setting',
            unique_together=set([('field', 'name')]),
        ),
        migrations.AlterUniqueTogether(
            name='model',
            unique_together=set([('app', 'name')]),
        ),
        migrations.AlterUniqueTogether(
            name='field',
            unique_together=set([('model', 'name')]),
        ),
    ]
