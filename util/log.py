# -*- coding: utf-8 -*-
import datetime
import logging
from stack import Stack

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

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
            logging.info("time: %s", starttime)
            logging.info("Run time: %s", endtime - starttime)

        return self;

    def reset(self):
        self.stack.clear()
        return self;

    def info(self, msg, *args, **kwargs):
        logging.info(msg, *args, **kwargs)
        return self;

    def warning(self, msg, *args, **kwargs):
        logging.warning(msg, *args, **kwargs)
        return self;

    def error(self, msg, *args, **kwargs):
        logging.error(msg, *args, **kwargs)
        return self;

    def debug(self, msg, *args, **kwargs):
        logging.debug(msg, *args, **kwargs)
        return self;

    def log_progress(self, function, address, page, total):
        logging.info("Progress: %s %s: current page %d total pages %d" %
                (function, address, page, total))
        return self;