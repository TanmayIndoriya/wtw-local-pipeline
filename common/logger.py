import logging
import logging.config

from config import get_config

_is_configured = False

def configure_logger():
    global _is_configured

    if _is_configured:
        return
    
    config = get_config().get_logging()

    logging.config.dictConfig(config)

    _is_configured = True

def get_logger(name : str) -> logging.Logger:
    configure_logger()
    return logging.getLogger(name)

