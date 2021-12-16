import logging
import os
import sys 
import socket 
import platform
import settings

def get_script_directory():
    return os.path.dirname(os.path.realpath(__file__))

def get_hostname():
    return socket.gethostname()

def get_ip():
    return socket.gethostbyname(socket.gethostname())

class ACSLogger:
    def __init__(self, log_level=settings.log_file_debug_level, name=None) -> None:
        self.logger = logging.getLogger(name or settings.logger_name)
        if log_level is not None:
            self.logger.setLevel(log_level)
            formatter = logging.Formatter('%(asctime)s | %(name)s | %(levelname)s | %(message)s')

            stdout_handler = logging.StreamHandler(sys.stdout)
            stdout_handler.setFormatter(formatter)
            stdout_handler.setLevel(settings.console_debug_level)

            log_file_handler = logging.FileHandler(get_script_directory() + '/' + settings.logger_file_name)
            log_file_handler.setFormatter(formatter)
            log_file_handler.setLevel(settings.log_file_debug_level)

            # self.logger.addHandler(stdout_handler)
            self.logger.addHandler(log_file_handler)

        # self.logger = self.get_logger(log_level=settings.log_file_debug_level)
        self.logger.info("Platform : " + str(platform.uname()))
        self.logger.info("Version :  " + str(sys.version_info))

