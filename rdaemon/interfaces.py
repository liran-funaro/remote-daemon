"""
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
from zope.interface import Interface


class IDaemon(Interface):
    """
    A daemon interface to be assert the class allow initiation and termination
    """

    def run():
        """
        Run the daemon.
        This should NOT create a background thread.
        This function should only return when the daemon should terminate
        :return: None
        """

    def notify(*args, **kwargs):
        """
        Should wake up the daemon if it is waiting for interrupt
        :param args, kwargs: (Optional) parameters to pass to the daemon
        :return: None
        """

    def terminate():
        """
        Calling this method should make run() return and thus terminate the daemon.
        After this call, is_terminated() should return True
        """

    def is_terminated():
        """
        :return: True if terminate() have been called
        """


class IPeriodicTask(Interface):
    """
    A task that should be preformed periodically
    """

    def setup():
        """
        Setup the task: build all internal data structures.
        :return: None
        """

    def teardown():
        """
        Tear down the task: destroy and release all the internal resources.
        :return: None
        """

    def periodic_task(is_scheduled_wakeup):
        """
        Perform the task
        :param is_scheduled_wakeup: Determine if the wake up was scheduled or
            the duo to an interrupt
        :return: None
        """

    def is_finished():
        """
        Ask the task if it is finished
        :return: True if finished
        """
