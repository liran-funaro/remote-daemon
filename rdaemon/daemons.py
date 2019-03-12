"""
Author: Liran Funaro <liran.funaro@gmail.com>

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
import atexit
import multiprocessing
import threading
from time import time, sleep

from rdaemon.interfaces import IDaemon, IPeriodicTask
from rdaemon.logging import LoggedEntity
from zope.interface.declarations import implementer
from zope.interface.verify import verifyObject


@implementer(IDaemon)
class DaemonThread(threading.Thread):
    """
    A thread that runs a daemon
    """

    def __init__(self, daemon_object):
        verifyObject(IDaemon, daemon_object, tentative=True)
        threading.Thread.__init__(self,
                                  name=daemon_object.__class__.__name__,
                                  daemon=True)
        self.daemon_object = daemon_object

        # Assert that the daemon is terminated on exit
        atexit.register(self.terminate)

    def run(self):
        """ IDaemon interface function """
        return self.daemon_object.run()

    def notify(self, *args, **kwargs):
        """ IDaemon interface function """
        return self.daemon_object.notify(*args, **kwargs)

    def terminate(self):
        """ IDaemon interface function """
        return self.daemon_object.terminate()

    def is_terminated(self):
        """ IDaemon interface function """
        return self.daemon_object.is_terminated()

    def __getattr__(self, item):
        """
        Extent the thread object to have the same functionality as the daemon.
        Only applicable for attributes that are not already taken by Thread object.
        :param item: The attribute name
        :return: This attribute from the daemon object
        """
        if item == 'daemon_object':
            raise AttributeError("Daemon object is not initialized yet")
        return getattr(self.daemon_object, item)


@implementer(IDaemon)
class BaseDaemon(LoggedEntity):
    """
    Base class that implements basic daemon functionality
    """

    def __init__(self, name=None):
        LoggedEntity.__init__(self, name)
        self._event = DaemonEvent()

        # Assert that the daemon is terminated on exit
        atexit.register(self.terminate)

    def __del__(self):
        """
        Assert that the daemon is terminated on exit
        """
        self.terminate()

    ###########################################################################
    # IDaemon interface
    ###########################################################################

    def notify(self, *args, **kwargs):
        """
        IDaemon interface function.
        Will force wake-up before the period.
        """
        self._event.set()

    def terminate(self):
        """
        IDaemon interface function.
        Will immediately terminate the daemon if is waiting.
        If it is working, it will be terminated when the work is done.
        """
        self._event.terminate()

    def is_terminated(self):
        """ IDaemon interface function """
        return self._event.is_terminated()

    def run(self):
        """ IDaemon interface function """
        while not self.is_terminated():
            self._event.wait_and_clear()


def wrap_as_daemon(obj, daemon_name=None):
    """
    Wraps a class to have a daemon functionality
    :param obj: The object to wrap
    :param daemon_name: The daemon name
    :return: None
    """
    class_name = f"{obj.__class__.__name__}DaemonWrapper"
    new_class = type(class_name, (BaseDaemon, obj.__class__), {})
    implementer(IDaemon)(new_class)
    obj.__class__ = new_class
    BaseDaemon.__init__(obj, name=daemon_name)


@implementer(IDaemon)
class PeriodicDaemon(BaseDaemon):
    """
    Allow periodic daemon functionality:
     - Run a periodic task on a periodic interval.
    """

    def __init__(self, task, wakeup_period, name=None):
        verifyObject(IPeriodicTask, task, tentative=True)
        if name is None:
            name = task.__class__.__name__
        BaseDaemon.__init__(self, name=name)

        self.task = task

        if wakeup_period < 1:
            self.log_warning("Wake interval must be at least 1 second."
                             "Was: %s and reset to 1 second.", wakeup_period)
            wakeup_period = 1

        self.wakeup_period = wakeup_period

    ###########################################################################
    # IDaemon interface
    ###########################################################################

    def run(self):
        """ IDaemon interface function """
        self.log_info("Setting up %s", self)
        if not self.__setup():
            self.log_error("%s cannot be setup. Terminating daemon.", self)
            return

        self.log_info("Staring %s periodic loop", self)

        try:
            last_wakeup = time()
            self._event.reset()

            while not self.is_terminated() and self.__is_finished() is False:
                is_scheduled_wakeup = self.wait_for_next_period(last_wakeup)
                last_wakeup = time()
                self.__periodic_task(is_scheduled_wakeup)
        except Exception as e:
            self.log_exception("Exception occur during %s: %s", self, e)
        finally:
            self.log_info("Tearing down %s", self)
            self.__teardown()

        self.log_info("%s terminated", self)

    ###########################################################################
    # Internal private functions (should not be overridden by child)
    ###########################################################################

    def calc_next_wait_time(self, last_wakeup):
        """
        :param last_wakeup: The last wakeup timestamp
        :return: How long to sleep until the next period
        """
        return max(0, self.wakeup_period - (time() - last_wakeup))

    def wait_for_next_period(self, last_wakeup):
        """
        Waits until the next period
        :param last_wakeup: The last wakeup timestamp
        :return: True if waked up on time,
                 False if waked up due to an interrupt
        """
        wait_time = self.calc_next_wait_time(last_wakeup)
        return not self._event.wait_and_clear(wait_time)

    ###########################################################################
    # Wrappers for IPeriodicTask functions (should not be overridden)
    ###########################################################################

    def __setup(self):
        """ See IPeriodicTask """
        try:
            self.task.setup()
            return True
        except Exception as e:
            self.log_exception("Exception occur during setup of %s: %s", self, e)
            return False

    def __teardown(self):
        """ See IPeriodicTask """
        try:
            return self.task.teardown()
        except Exception as e:
            self.log_exception("Exception occur during teardown of %s: %s", self, e)

    def __periodic_task(self, is_scheduled_wakeup):
        """ See IPeriodicTask """
        try:
            return self.task.periodic_task(is_scheduled_wakeup)
        except Exception as e:
            self.log_exception("Exception occur during executing of %s: %s", self, e)

    def __is_finished(self):
        """ See IPeriodicTask """
        try:
            return self.task.is_finished()
        except Exception as e:
            self.log_exception("Exception occur during checking on %s: %s", self, e)


@implementer(IDaemon)
class TestCounterDaemon(BaseDaemon):
    """
    Test daemon counts its wakeups
    """

    def __init__(self):
        BaseDaemon.__init__(self)
        self.__count = 0

    ###########################################################################
    # IDaemon interface
    ###########################################################################

    def run(self):
        """ IDaemon interface function """
        self._event.reset()

        while not self.is_terminated():
            self._event.wait_and_clear()
            self.__count += 1

    ###########################################################################
    # Test interface
    ###########################################################################

    def get_notify_count(self):
        """
        :return: Notify counter
        """
        return self.__count

    def long_operation(self, seconds, return_value):
        """
        Wait for some time and return a value
        :param seconds: The time to wait
        :param return_value: The value to return
        :return: return value
        """
        sleep(seconds)
        return return_value


@implementer(IDaemon)
class TestSleeperDaemon(BaseDaemon):
    """
    Test daemon that sleeps after wakeup
    """

    def __init__(self, sleep_time=1):
        BaseDaemon.__init__(self)
        self.sleep_time = sleep_time

    def run(self):
        """ IDaemon interface function """
        self._event.reset()

        while not self.is_terminated():
            self._event.wait_and_clear()
            sleep(self.sleep_time)


###########################################################################
# Internal Classes
###########################################################################

class ThreadingValue:
    def __init__(self, init_val):
        self.value = init_val


class DaemonEvent:
    """
    A re-implementation of python's threading.Event to support termination
    """

    def __init__(self):
        try:
            self.__cond = multiprocessing.Condition()
            self.__flag = multiprocessing.Value("b", False)
            self.__terminated = multiprocessing.Value("b", False)
        except:
            self.__cond = threading.Condition()
            self.__flag = ThreadingValue(False)
            self.__terminated = ThreadingValue(False)

    def _reset_internal_locks(self):
        # private!  called by Thread._reset_internal_locks by _after_fork()
        self.__cond.__init__()

    def is_set(self):
        """
        :return: True if the flag is set
        """
        return bool(self.__flag.value)

    def set(self):
        """
        Set the flag
        :return: None
        """
        self.__cond.acquire()
        try:
            self.__flag.value = True
            self.__cond.notify_all()
        finally:
            self.__cond.release()

    def clear(self):
        """
        Clear the flag
        :return: Previous flag value
        """
        self.__cond.acquire()
        try:
            ret_flag = bool(self.__flag.value)
            if not bool(self.__terminated.value):
                self.__flag.value = False
            return ret_flag
        finally:
            self.__cond.release()

    def reset(self):
        """
        Reset the Event object
        :return: None
        """
        self.__cond.acquire()
        try:
            self.__flag.value = False
            self.__terminated.value = False
        finally:
            self.__cond.release()

    def wait_and_clear(self, timeout=None):
        """
        Wait for the flag to be set.
        When wake up, clear the flag.
        :param timeout: Wake up after this timeout
        :return: Previous flag value
        """
        self.__cond.acquire()
        try:
            if self.is_terminated():
                return False
            if not bool(self.__flag.value):
                self.__cond.wait(timeout)
            ret_flag = bool(self.__flag.value)
            self.__flag.value = False
            return ret_flag
        finally:
            self.__cond.release()

    def terminate(self):
        """
        Set the termination flag and wake up all who is waiting fot the event
        :return: Previous flag value
        """
        self.__cond.acquire()
        try:
            self.__terminated.value = True
            ret_flag = bool(self.__flag.value)
            self.__flag.value = True
            self.__cond.notify_all()
            return ret_flag
        finally:
            self.__cond.release()

    def is_terminated(self):
        """
        :return: Is the event terminated
        """
        return bool(self.__terminated.value)
