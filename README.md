## Документация

#### Настройки конфигурации

Папка config содержит все исходные настройки проекта
Локальные настройки проекта для текущего окружения находятся в файле

* config/settings/local.py

#### Развертывание приложения

Для установки необходимых библиотек выполнить команду:
pip install -r requirements/base.txtp

В файле config/settings/local.py нужно прописать

DEBUG=False
и строку ALLOWED_HOSTS
После этого выполнить
#### python manage.py collectstatic --noinput

