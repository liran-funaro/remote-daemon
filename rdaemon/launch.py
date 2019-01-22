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
import logging
import multiprocessing
import sys

from rdaemon.cgroup import daemonize as daemonize_cgroup
from rdaemon.file import daemonize as daemonize_file

from ginseng.util.logging import LogManager, StreamToLogger


def launch_daemon(name, target, args=tuple(), kwargs={}, daemon_group="", launcher_timeout=60,
                  log_conf={}, daemon_bookkeeping='file'):
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
    assert daemon_bookkeeping in ['file', 'cgroup']

    ctx = multiprocessing.get_context('spawn')
    process = ctx.Process(name="daemon-launcher-%s" % name,
                               target=_run_daemon_,
                               args=(name, target, args, kwargs, daemon_group,
                                     log_conf, daemon_bookkeeping))

    process.start()
    process.join(timeout=launcher_timeout)


def _run_daemon_(name, target, args, kwargs, daemon_group, log_conf, daemon_bookkeeping):
    """
    The target function of the new process.
    """
    if daemon_bookkeeping == 'file':
        daemonize_file(daemon_name=name, sub_path=daemon_group)
    elif daemon_bookkeeping == 'cgroup':
        daemonize_cgroup(daemon_name=name, sub_path=daemon_group)

    try:
        LogManager.init_logger(name, stdio=False, file=True, **log_conf)
    except Exception as e:
        print("Failed to initiate logger for daemon:", e, file=sys.stderr)
        exit(1)

    sys.stdout = StreamToLogger(logging.INFO)
    sys.stderr = StreamToLogger(logging.ERROR)

    try:
        target(*args, **kwargs)
    except Exception as e:
        print("Daemon exited with an error:", e, file=sys.stderr)
        exit(1)

    # try:
    #     LogManager.stop_logger(stdio=True, file=True)
    # except Exception as e:
    #     print("Failed to stop logger for daemon:", e, file=sys.stderr)
    #     exit(1)

    exit(0)
