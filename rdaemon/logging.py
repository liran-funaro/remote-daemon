"""
Useful utilities for logging

Author: Liran Funaro <funaro@cs.technion.ac.il>

Copyright (C) 2006-2018 Liran Funaro

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""
import os
import logging
import logging.handlers
from threading import RLock
from typing import NewType, IO


class LoggedEntity(object):
    """
    This is a base class to be inherit from.
    Is creates a logger for the child class with the name of the class and a specialized name.
    """

    def __init__(self, name=None):
        """
        :param name: The name of the logger. If is None, then only the class name will be used
        """
        self.__init_logger__(name)
        self.log_debug("Init")

    ###########################################################################
    # Wrappers for the logger
    ###########################################################################

    def log_info(self, msg, *args, **kwargs):
        return self.__logger__.info(msg, *args, **kwargs)

    def log_debug(self, msg, *args, **kwargs):
        return self.__logger__.debug(msg, *args, **kwargs)

    def log_warning(self, msg, *args, **kwargs):
        return self.__logger__.warning(msg, *args, **kwargs)

    def log_error(self, msg, *args, **kwargs):
        return self.__logger__.error(msg, *args, **kwargs)

    def log_critical(self, msg, *args, **kwargs):
        return self.__logger__.critical(msg, *args, **kwargs)

    def log_exception(self, msg, *args, exc_info=True, **kwargs):
        return self.__logger__.exception(msg, *args, exc_info=exc_info, **kwargs)

    def log_data(self, data):
        pass
        # return self.__logger__.log(LOG_DATA_LEVEL, str(data))

    def __repr__(self):
        return str(self.__log_name__)

    ###########################################################################
    # Helper functions
    ###########################################################################

    def __init_logger__(self, name):
        """
        Init the logger instance and name

        :param name: The name of the logger. If is None, then only the class name will be used
        :return: None
        """

        # To allow diamond inheritance, we first check if the logger attribute
        # as already been set. If so, we don't need to initialize the logger.
        if hasattr(self, "__logger__"):
            return

        self.__entity_name__ = name
        if name is not None:
            self.__log_name__ = "%s-%s" % (self.__class__.__name__, name)
        else:
            self.__log_name__ = self.__class__.__name__

        self.__logger__ = logging.getLogger(self.__log_name__)


class LogManager:
    """
    This class is based on the class LogUtils from MOM:
    Memory Overcommitment Manager
    Copyright (C) 2010 Adam Litke, IBM Corporation

    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License version 2 as
    published by the Free Software Foundation.

    This program is distributed in the hope that it will be useful, but
    WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
    General Public License for more details.

    You should have received a copy of the GNU General Public
    License along with this program; if not, write to the Free Software
    Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
    """
    lock = RLock()
    logging_handlers = []
    data_level_initiated = False

    FMT = "%(created)f - %(asctime)s - %(processName)s - %(threadName)s - %(name)s - %(levelname)s - %(message)s"

    verbosity_translator = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warn': logging.WARN,
        'error': logging.ERROR,
        'critical': logging.CRITICAL,
    }

    @classmethod
    def init_logger(cls, name, verbosity='debug', output_path="/tmp", max_bytes=0, backups_count=0):
        """
        Init logger to a file.
        By default, the file grows indefinitely. You can specify particular
        values of max_bytes and backup_count to allow the file to rollover at
        a predetermined size.

        :param name: The name of the logger (will define the file name)
        :param verbosity: Log verbosity level (default: debug)
        :param output_path: The path to store the log file (ignored if file is false)
        :param max_bytes: Rollover over when log exceed this size (ignored if file is false)
        :param backups_count: Maximum number of rollovers (ignored if file is false)
        :return: None
        """
        cls.__init_logger_handler(name, verbosity, output_path, max_bytes, backups_count)
        logging.log(logging.DEBUG, "Logging initiated")

    @classmethod
    def stop_logging(cls):
        """
        Stop logging for specific handler
        :return: None
        """
        with cls.lock:
            while cls.logging_handlers:
                handler = cls.logging_handlers.pop()
                try:
                    logging.getLogger().removeHandler(handler)
                except Exception as e:
                    logging.log(logging.DEBUG, "Failed to remove logging handler %s: %e", handler, e)

    ###########################################################
    # Helper functions
    ###########################################################

    @classmethod
    def get_verbosity_level(cls, verbosity):
        """
        Get the ID of the verbosity level
        :param verbosity: Verbosity level as int or text
        :return: Verbosity level as int
        """
        if isinstance(verbosity, int):
            return verbosity

        try:
            return int(verbosity)
        except ValueError:
            pass

        if not isinstance(verbosity, str):
            raise ValueError("Verbosity level must be an integer or a string")

        verbosity = verbosity.lower()
        return cls.verbosity_translator.get(verbosity, 0)

    @classmethod
    def __init_logger_handler(cls, name, verbosity='debug', output_path="/tmp", max_bytes=0, backups_count=0):
        """
        Init logger to stdio/file.
        By default, the file grows indefinitely. You can specify particular
        values of max_bytes and backup_count to allow the file to rollover at
        a predetermined size.
        :param name: The name of the logger (will define the file name)
        :param verbosity: Log verbosity level (default: debug)
        :param output_path: The path to store the log file (ignored if stdio)
        :param max_bytes: Rollover over when log exceed this size (ignored if stdio)
        :param backups_count: Maximum number of rollovers (ignored if stdio)
        :return: None
        """
        verbosity = cls.get_verbosity_level(verbosity)

        logger = logging.getLogger()
        logger.setLevel(verbosity)

        with cls.lock:
            if cls.logging_handlers:
                return
            log_file = os.path.join(output_path, f"{name}.log")
            handler = logging.handlers.RotatingFileHandler(
                log_file, 'a', max_bytes, backups_count)

            handler.setLevel(verbosity)
            handler.setFormatter(logging.Formatter(cls.FMT))
            logger.addHandler(handler)
            cls.logging_handlers.append(handler)


class StreamToLogger(IO):
    """
    Taken from:
    https://github.com/jdloft/multiprocess-logging/blob/master/main.py
    """

    def __init__(self, name, log_level=logging.INFO):
        self._name = name
        self.logger = logging.getLogger(name)
        self.log_level = log_level
        self.buffer_list = []

    def name(self):
        return self._name

    def write(self, buf):
        """
        Append the buffer to the buffer list
        and attempt to flush a full line
        :param buf: The buffer to write
        :return: None
        """
        self.buffer_list.append(buf)
        self.flush()
        return len(buf)

    def flush(self):
        """
        Will flush the buffer line by line.
        Will not flush "half" lines. Will save for later
        :return: None
        """
        lines = "".join(self.buffer_list).lstrip().split("\n")
        self.buffer_list = lines[-1:]
        lines = lines[:-1]

        for line in lines:
            self.logger.log(self.log_level, line.rstrip().lstrip())


###################################################################################
# Logging conf
###################################################################################

def read_logging_configuration(conf):
    """
    Read the logging configuration
    :param conf: A configuration object
    :return: A dict
    """
    return dict(
        verbosity=conf.logging.verbosity,
        max_bytes=conf.logging.max_bytes,
        backups_count=conf.logging.backups_count,
        output_path=conf.logging.output_path,
    )


def default_log_conf():
    """
    :return: The default log configuration
    """
    return dict(
        verbosity='debug',
        max_bytes=0,
        backups_count=0,
        output_path="/tmp",
    )
