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
from rdaemon.bookkeeping.file import get_daemon_pid
from rdaemon.process import pid_exists
from rdaemon.logging import LoggedEntity
from rdaemon.interfaces import IPeriodicTask
from zope.interface.declarations import implementer


@implementer(IPeriodicTask)
class IsAlivePeriodicChecker(LoggedEntity):
    def __init__(self, name, pid=None, pid_file=None, on_dead_daemon_func=None):
        LoggedEntity.__init__(self, name)

        self.name = name
        self.pid = pid
        self.pid_file = pid_file
        assert pid is not None or pid_file is not None
        self.daemon_is_dead_func = on_dead_daemon_func
        self.finished = None

    def setup(self):
        """ IPeriodicTask interface function """
        if self.pid is None:
            self.pid = get_daemon_pid(self.name, self.pid_file)
        self.finished = False

    def teardown(self):
        """ IPeriodicTask interface function """
        if self.deamon_is_dead_func:
            self.deamon_is_dead_func()

    def periodic_task(self):
        """ IPeriodicTask interface function """
        self.finished = not self.is_alive()

    def is_finished(self):
        """ IPeriodicTask interface function """
        return self.finished

    def is_alive(self):
        """
        :return: True if the daemon is alive
        """
        return pid_exists(self.pid)


@implementer(IPeriodicTask)
class TestPeriodicTask:
    """ Test PeriodicTask counts scheduled wakups """

    def __init__(self):
        self.counter = 0

    def setup(self):
        """ IPeriodicTask interface function """
        self.counter = 0

    def teardown(self):
        """ IPeriodicTask interface function """
        pass

    def periodic_task(self, is_scheduled_wakeup):
        """ IPeriodicTask interface function """
        if is_scheduled_wakeup:
            self.counter += 1

    @staticmethod
    def is_finished():
        """ IPeriodicTask interface function """
        return False

    def get_count(self):
        """ :return: The number of scheduled wakeups """
        return self.counter
