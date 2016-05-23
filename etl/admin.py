from django.contrib import admin

# Register your models here.

"""DROP SERVER IF EXISTS postgres_server;
CREATE SERVER postgres_server
        FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (host '127.0.0.1', port '5432', dbname 'biplatform');

CREATE USER MAPPING FOR biplatform
        SERVER postgres_server
        OPTIONS (user 'biplatform', password 'biplatform');

DROM FOREIGN TABLE IF EXISTS auth_group_foreign;
CREATE FOREIGN TABLE auth_group_foreign (
        id serial NOT NULL,
        name text
)
        SERVER postgres_server
        OPTIONS (schema_name 'public', table_name 'auth_group');"""
