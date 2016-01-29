"""
Django settings for biplatform project.

Generated by 'django-admin startproject' using Django 1.8.3.

For more information on this file, see
https://docs.djangoproject.com/en/1.8/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/1.8/ref/settings/
"""

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/1.8/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = '7z8t&gp3&u3=jbdje*blzaw-a$702j%b=oa6q=x@tj_2sem$v6'

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

ALLOWED_HOSTS = []

# set owr model for users
AUTH_USER_MODEL = 'core.User'

LOGIN_URL = '/login'

LOGOUT_URL = '/logout'

# Application definition

INSTALLED_APPS = (
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'djcelery',
    'core',
    'etl',
    'api',
)

MIDDLEWARE_CLASSES = (
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.auth.middleware.SessionAuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    'django.middleware.security.SecurityMiddleware',
    'core.middleware.http.HttpResponseNotAllowedMiddleware',
    'core.middleware.thread_local.ThreadLocalMiddleware'
)

ROOT_URLCONF = 'config.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [os.path.join(BASE_DIR, 'templates')]
        ,
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
                'core.context_processor.settings_processor',
            ],
        },
    },
]

WSGI_APPLICATION = 'config.wsgi.application'


# Database
# https://docs.djangoproject.com/en/1.8/ref/settings/#databases

DATABASES = {
    'default': {
        'ENGINE': 'core.db.postgresql_psycopg2',
        'NAME': 'biplatform',
        'USER': 'biplatform',
        'PASSWORD': '',
        'HOST': 'localhost',
        'PORT': '5432',
    }
}

# smtp server
EMAIL_BACKEND = 'django.core.mail.backends.smtp.EmailBackend'
DEFAULT_FROM_EMAIL = ''
EMAIL_USE_TLS = True
EMAIL_HOST = 'smtp.mail.ru'
EMAIL_PORT = 25
EMAIL_HOST_USER = ''
EMAIL_HOST_PASSWORD = ''

OLAP_SERVER_HOST = 'localhost'
OLAP_SERVER_PORT = '8080'
OLAP_SERVER_USER = 'admin'
OLAP_SERVER_PASS = 'admin'


import djcelery
djcelery.setup_loader()


# logging
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '%(asctime)s %(levelname)s: %(message)s: %(module)s %(process)d %(thread)d'
        },
        'simple': {
            'format': '%(levelname)s %(message)s'
        },
    },
    'handlers': {
        'core': {
            'level': os.getenv('DJANGO_LOG_LEVEL', 'DEBUG'),
            'class': 'logging.FileHandler',
            'filename': os.path.join(BASE_DIR, 'data', 'logs', 'application.log'),
            'formatter': 'verbose'
        },
        'etl': {
            'level': os.getenv('DJANGO_LOG_LEVEL', 'DEBUG'),
            'class': 'logging.FileHandler',
            'filename': os.path.join(BASE_DIR, 'data', 'logs', 'etl.log'),
            'formatter': 'verbose'
        },
        'console': {
            'class': 'logging.StreamHandler'
        }
    },
    'loggers': {
        'django': {
            'handlers': ['console'],
            'level': os.getenv('DJANGO_LOG_LEVEL', 'INFO')
        },
        'django.request': {
            'handlers': ['core'],
            'level': 'ERROR',
            'propagate': False
        },
        'core.views': {
            'handlers': ['core'],
            'level': 'ERROR',
            'propagate': True
        },
        'etl.views': {
            'handlers': ['etl'],
            'level': 'ERROR',
            'propagate': True
        },
        'celery': {
            'handlers': ['console'],
            'level': 'DEBUG',
        },
        'core.helpers': {
            'handlers': ['core'],
            'level': 'ERROR',
            'propagate': True
        },
        'etl.tasks': {
            'handlers': ['etl'],
            'level': 'ERROR',
            'propagate': True
        },
        'etl.services.model_creation': {
            'handlers': ['etl'],
            'level': 'ERROR',
            'propagate': True
        }
    }
}

# Internationalization
# https://docs.djangoproject.com/en/1.8/topics/i18n/

LANGUAGE_CODE = 'ru-RU'

TIME_ZONE = 'Europe/Moscow'

USE_I18N = True

USE_L10N = True

USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/1.8/howto/static-files/

STATIC_URL = '/assets/'

STATICFILES_DIRS = (
    os.path.join(BASE_DIR, "assets"),
)

# redis conf
REDIS_HOST = 'localhost'
REDIS_PORT = '6379'
REDIS_DB = '0'
REDIS_EXPIRE = 60 * 5
USE_REDIS_CACHE = False

# mongo conf
MONGO_HOST = 'localhost'
MONGO_PORT = 27017

# rows select limit
ETL_COLLECTION_PREVIEW_LIMIT = 1000
# rows load limit
ETL_COLLECTION_LOAD_ROWS_LIMIT = 1000

# host, port for websockets
SOCKET_HOST = ''  # localhost
SOCKET_PORT = ''  # 8080
# dvoetochie ne nravitsya autobahn connection, poetomu tire '-'
SOCKET_CHANNEL = 'jobs-etl-extract-{0}-{1}'

RETRY_COUNT = 3
DEADLOCK_WAIT_TIMEOUT = 500
DATABASE_WAIT_TIMEOUT = 10000
REDIS_LOCK_TIMEOUT = 500

try:
    from .local import *
except ImportError:
    pass
