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
import logging
import multiprocessing
import sys

from rdaemon.bookkeeping import daemonize, assert_valid_bookkeeping
from rdaemon.logging import LogManager, StreamToLogger


def launch_daemon(name, target, args=(), kwargs=None, daemon_group=None, launcher_timeout=60,
                  log_conf=None, daemon_bookkeeping=None):
    """
    Spawn a new python process that launch a daemon
    Will spawn a new process with nothing in common with the calling process.
    :param name: The daemon name
    :param target: A target function (unlike Process/Thread, this is not optional)
    :param args: The target function args
    :param kwargs: The target function kwargs
    :param daemon_group: A group name
    :param launcher_timeout: Timeout for waiting to launcher process
    :param log_conf: Log parameters. See LogManager.init_logger()
    :param daemon_bookkeeping: What bookkeeping method to use (file or cgroup)
    """
    assert_valid_bookkeeping(daemon_bookkeeping)
    if log_conf is None:
        log_conf = {}
    if kwargs is None:
        kwargs = {}

    ctx = multiprocessing.get_context('spawn')
    process = ctx.Process(name="daemon-launcher-%s" % name,
                          target=_run_daemon_,
                          args=(name, target, args, kwargs, daemon_group,
                                log_conf, daemon_bookkeeping))

    process.start()
    process.join(timeout=launcher_timeout)


def _run_daemon_(name, target, args, kwargs, daemon_group, log_conf, daemon_bookkeeping):
    """ The target function of the new process. """
    daemonize(daemon_name=name, sub_path=daemon_group, bookkeeping_method=daemon_bookkeeping)

    try:
        LogManager.init_logger(name, **log_conf)
    except Exception as e:
        print("Failed to initiate logger for daemon:", e, file=sys.stderr)
        exit(1)

    sys.stdout = StreamToLogger('stdout', logging.INFO)
    sys.stderr = StreamToLogger('stderr', logging.ERROR)

    try:
        target(*args, **kwargs)
    except Exception as e:
        print("Daemon exited with an error:", e, file=sys.stderr)
        exit(1)

    try:
        LogManager.stop_logging()
    except Exception as e:
        print("Failed to stop logger for daemon:", e, file=sys.stderr)
        exit(1)

    exit(0)
