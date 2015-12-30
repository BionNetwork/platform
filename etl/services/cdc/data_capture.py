# coding: utf-8

from operator import itemgetter


class BaseCdc(object):

    def apply_triggers(self, source, tables_info):
        """
        Создание триггеров в БД пользователя
        """
        db_instance = self.db_instance
        sep = db_instance.get_separator()
        remote_table_create_query = db_instance.remote_table_create_query()
        remote_triggers_create_query = db_instance.remote_triggers_create_query()

        connection = db_instance.connection
        cursor = connection.cursor()

        for table, columns in tables_info.iteritems():

            table_name = '_etl_datasource_cdc_{0}'.format(table)
            tables_str = "('{0}')".format(table_name)

            cdc_cols_query = db_instance.db_map.cdc_cols_query.format(
                tables_str, source.db, 'public')

            cursor.execute(cdc_cols_query)
            fetched_cols = cursor.fetchall()
            existing_cols = map(itemgetter(1), fetched_cols)

            cols_str = ''
            new = ''
            old = ''
            cols = ''

            for col in columns:
                name = col['name']
                new += 'NEW.{0}, '.format(name)
                old += 'OLD.{0}, '.format(name)
                cols += ('{name}, '.format(name=name))
                cols_str += ' {sep}{name}{sep} {typ},'.format(
                    sep=sep, name=name, typ=col['type']
                )

            # если таблица существует
            if existing_cols:

                new_foreign_cols = [(x['name'], x["type"]) for x in columns]

                # добавление недостающих колонок
                new_cols = new_foreign_cols + [
                    ('cdc_created_at', 'timestamp'),
                    ('cdc_updated_at', 'timestamp'),
                    ('cdc_delta_flag', 'smallint'),
                    ('cdc_synced', 'smallint'),
                ]

                diff_cols = [x for x in new_cols if x[0] not in existing_cols]

                if diff_cols:
                    add_cols_str = """
                        alter table {0} {1}
                    """.format(table_name, ', '.join(
                        ['add column {0} {1}'.format(x[0], x[1]) for x in diff_cols]))

                    cursor.execute(add_cols_str)
                    connection.commit()

                # проверяем индексы
                required_indexes = [
                    ['cdc_created_at', ],
                    ['cdc_synced', ],
                    ['cdc_synced', 'cdc_updated_at'], ]

                indexes_query = db_instance.db_map.indexes_query.format(
                    tables_str, source.db)
                cursor.execute(indexes_query)
                indexes = cursor.fetchall()
                existing_indexes = map(lambda i: sorted(i[1].split(',')), indexes)

                diff_indexes = [x for x in required_indexes if x not in existing_indexes]

                for d_ind in diff_indexes:
                    ind_query = """
                        CREATE INDEX {t}_{n}_index_bi ON {sep}{t}{sep} ({cols});
                    """.format(
                        t=table_name, n='_'.join(d_ind),
                        sep=sep, cols=','.join(d_ind))
                    cursor.execute(ind_query)

                connection.commit()

            # если таблица не существует
            else:

                # multi queries of mysql, delimiter $$
                for query in remote_table_create_query.format(
                        table_name, cols_str).split('$$'):
                    cursor.execute(query)

                connection.commit()

            trigger_commands = remote_triggers_create_query.format(
                orig_table=table, new_table=table_name, new=new, old=old,
                cols=cols)

            # multi queries of mysql, delimiter $$
            for query in trigger_commands.split('$$'):
                cursor.execute(query)

            connection.commit()

    def apply_checksum(self):
        pass
