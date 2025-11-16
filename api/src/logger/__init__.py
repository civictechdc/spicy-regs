import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s:%(filename)s:%(funcName)s:%(lineno)d - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.handlers.RotatingFileHandler("api.log", maxBytes=1024 * 1024 * 5),
    ],
)
logger = logging.getLogger("spicy-regs:api")
