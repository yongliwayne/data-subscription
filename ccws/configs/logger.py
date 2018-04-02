import logging.config
from ccws.configs import HOME_PATH


def load_logger_config(ex):
    config = {
        'disable_existing_loggers': False,
        'version': 1,
        'formatters': {
            'short': {
                'format': '%(asctime)s %(levelname)s %(name)s: %(message)s'
            },
            'detail': {
                'format': '%(asctime)s %(levelname)s %(name)s %(funcName)s: %(message)s',
                'datefmt': '%Y-%m-%d %H:%M:%S %z'
            },
        },
        'handlers': {
            'console': {
                'level': 'DEBUG',
                'formatter': 'short',
                'class': 'logging.StreamHandler',
            },
            'file': {
                'level': 'NOTSET',
                'formatter': 'detail',
                'class': 'logging.handlers.RotatingFileHandler',
                'filename': '%s/log/%s.log' % (HOME_PATH, ex),
            },
        },
        'loggers': {
            '%s' % ex: {
                'level': 'NOTSET',
                'handlers': ['file', 'console'],
            }
        },
        'root': {
            'level': 'NOTSET',
        }
    }

    logging.config.dictConfig(config)
