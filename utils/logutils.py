import logging


class LogUtils:

    @staticmethod
    def logger():
        logging.basicConfig(level=logging.INFO, format='app="rm_spark_high_availability_framework" module="%(module)s" '
                                                       'timestamps="%(asctime)s" level="%(levelname)s" function="%('
                                                       'funcName)s" message="%(message)s"')
        log = logging.getLogger(__name__)
        return log
