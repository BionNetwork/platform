# coding: utf-8


class BaseCdc(object):
    # инстанс базы данных для выполнения удаленных команд
    db_instance = None

    def get_db_instance(self):
        if self.db_instance is None:
            raise ValueError("Database instance for CDC is not set properly")
        return self.db_instance

    def apply_triggers(self, tables_info):
        """
        Создание триггеров в БД пользователя

        Args:
            tables_info: tuple информация о колонках внутри таблиц. Приходит в виде кортежа (table, {name, type})
        """
        db_instance = self.get_db_instance()
        sep = db_instance.get_separator()
        remote_table_create_query = db_instance.remote_table_create_query()
        remote_triggers_create_query = db_instance.remote_triggers_create_query()

        connection = db_instance.connection
        cursor = connection.cursor()

        for table, columns in tables_info.iteritems():

            table_name = '_etl_datasource_cdc_{0}'.format(table)
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
