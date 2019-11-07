# -*- coding: utf-8 -*-
import datetime
import logging
from stack import Stack
import time
from config import settings

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO, filename=settings.LOG_PATH + time.strftime('%Y-%m-%d') +'.log', filemode='a')

console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)

logger = logging.getLogger()
logger.addHandler(console)

class Log(object):
    
    def __init__(self):
        self.stack = Stack()

    def start(self):
        self.stack.push(datetime.datetime.now())
        return self;

    def end(self):
        endtime =  datetime.datetime.now()
        
        if not self.stack.is_empty():
            starttime = self.stack.pop()
            logger.info("time: %s", starttime)
            logger.info("Run time: %s", endtime - starttime)

        return self;

    def reset(self):
        self.stack.clear()
        return self;

    def info(self, msg, *args, **kwargs):
        logger.info(msg, *args, **kwargs)
        return self;

    def warning(self, msg, *args, **kwargs):
        logger.warning(msg, *args, **kwargs)
        return self;

    def error(self, msg, *args, **kwargs):
        logger.error(msg, *args, **kwargs)
        return self;

    def debug(self, msg, *args, **kwargs):
        logger.debug(msg, *args, **kwargs)
        return self;

    def log_progress(self, function, address, page, total):
        logger.info("Progress: %s %s: current page %d total pages %d" %
                (function, address, page, total))
        return self;