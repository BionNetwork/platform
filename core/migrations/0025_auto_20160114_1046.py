# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models, migrations


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0024_datasettometa'),
    ]

    operations = [
         migrations.RunSQL(
             """
             DROP TABLE datasets_to_meta;
             CREATE TABLE datasets_to_meta
                 (
                     meta_id integer NOT NULL,
                     dataset_id integer NOT NULL,

                     CONSTRAINT datasets_to_meta_pkey PRIMARY KEY (meta_id, dataset_id),

                     CONSTRAINT datasets_to_meta_meta_id_custom_fk_datasources_meta_id
                         FOREIGN KEY (meta_id)
                         REFERENCES datasources_meta (id) MATCH SIMPLE
                         ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED,

                     CONSTRAINT datasets_to_meta_dataset_id_custom_fk_datasets_id
                         FOREIGN KEY (dataset_id)
                         REFERENCES datasets (id) MATCH SIMPLE
                         ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED
                 )
                 WITH (
                   OIDS=FALSE
                 );
             """
        ),
    ]
