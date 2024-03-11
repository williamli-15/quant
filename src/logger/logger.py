
from datetime import datetime
import logging

from pathmgmt import pathmgmt as myPath

class Logger():
    def __init__(self, name, level=logging.DEBUG):
        self.__name = name
        self.__level = level
        
    def init(self, console_handler=False):
        logFormatter = logging.Formatter(
            '%(asctime)s:%(levelname)s:%(name)s:%(message)s')
        self.__logger = logging.getLogger(self.__name)
        self.__logPath = myPath.makeLogPath(datetime.now(), self.__name)
        
        self.__fileHandler = logging.FileHandler(self.__logPath)
        self.__fileHandler.setFormatter(logFormatter)
        self.__logger.addHandler(self.__fileHandler)
        
        if console_handler:
            self.__consoleHandler = logging.StreamHandler()
            self.__consoleHandler.setFormatter(logFormatter)
            self.__logger.addHandler(self.__consoleHandler)
            
        self.__logger.setLevel(self.__level)
    
    def __call__(self):
        return self.__logger