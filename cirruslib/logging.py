import logging
from os import getenv


config = {
  "version": 1,
  "disable_existing_loggers": False,
  "formatters": {
      "standard": {
          "format": "%(asctime)s %(name)s %(levelname)s %(message)s",
          "datefmt": "%Y-%m-%dT%H:%M:%S%z",
      },
      "json": {
          "format": "%(asctime)s %(name)s %(levelname)s %(message)s",
          "datefmt": "%Y-%m-%dT%H:%M:%S%z",
          "class": "pythonjsonlogger.jsonlogger.JsonFormatter"
      }
  },
  "handlers": {
      "standard": {
          "class": "logging.StreamHandler",
          "formatter": "json"
      }
  },
  "loggers": {
      "": {
          "handlers": ["standard"],
          "level": getenv('CIRRUS_LOG_LEVEL', 'DEBUG')
      }
  }
}

logging.config.dictConfig(config)

logger = logging.getLogger(__name__)


class DynamicLoggerAdapter(logging.LoggerAdapter):

    def __init__(self, *args, keys=None, **kwargs):
        super(DynamicLoggerAdapter, self).__init__(*args, **kwargs)
        self.keys = keys

    def process(self, msg, kwargs):
        if self.keys is not None:
            kwargs = {k: self.extra[k] for k in self.keys if k in self.extra}
            return (msg, {"extra": kwargs})
        else:
            return (msg, kwargs)