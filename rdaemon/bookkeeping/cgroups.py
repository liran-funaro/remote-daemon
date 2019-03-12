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
import os
import signal
import sys

from pycgroups import Cgroup

import rdaemon.process as daemon_process

DEFAULT_DAEMON_CGROUP = "rdaemons"


#########################################################################
# Module PID file API
#########################################################################

def default_daemon_cgroup_path(daemon_name, sub_path=None):
    """
    Default cgroup is rdaemons/<daemon_name>
    :param daemon_name: The daemon name
    :param sub_path: A sub path for a specific group of daemons
    :return: The daemon cgroup path
    """
    if sub_path is None:
        sub_path = ""
    return os.path.join(DEFAULT_DAEMON_CGROUP, sub_path, daemon_name)


def daemon_cgroup_path(daemon_name=None, sub_path=None, cgroup_path=None):
    """
    Retrieve the daemon cgroup path
    :param daemon_name: The daemon name (if pid_file is None)
    :param sub_path: A sub path for a specific group of daemons
    :param cgroup_path: Can specify the cgroup path directly
    :return: The cgroup path or raise exception if bad parameters
    """
    if cgroup_path is not None:
        if not isinstance(cgroup_path, str):
            raise ValueError("Cgroup path must be a string")
        return cgroup_path
    else:
        if not isinstance(daemon_name, str):
            raise ValueError("If cgroup path was not specified, daemon name must be string")
        return default_daemon_cgroup_path(daemon_name, sub_path)


def daemon_cgroup(daemon_name=None, sub_path=None, cgroup_path=None, create=False):
    """
    Retrieve the daemon cgroup object
    :param daemon_name: The daemon name (if pid_file is None)
    :param sub_path: A sub path for a specific group of daemons
    :param cgroup_path: Can specify the cgroup path directly
    :param create: Create the cgroup if not exists
    :return: The cgroup object or raise exception if bad parameters
    """
    cgroup_path = daemon_cgroup_path(daemon_name=daemon_name, sub_path=sub_path, cgroup_path=cgroup_path)
    try:
        return Cgroup(cgroup_path, create=create)
    except:
        raise ValueError(f"The daemon is not active. No cgroup: {cgroup_path}.")


#########################################################################
# Module interface API
#########################################################################

def get_daemon_pid(daemon_name=None, sub_path=None, cgroup_path=None):
    """
    Read the daemon PIDs
    :param daemon_name:
    :param sub_path:
    :param cgroup_path: See daemon_cgroup_path()
    :return: The PIDs as a list of integers or raise error
    """
    cgroup = daemon_cgroup(daemon_name=daemon_name, sub_path=sub_path, cgroup_path=cgroup_path, create=False)
    procs = cgroup.procs
    if len(procs) == 0:
        cgroup.delete(recursive=True)
        raise ValueError(f"The daemon is not active. Cgroup path have no tasks: {cgroup.path}.")

    return procs


def kill_daemon(daemon_name=None, sub_path=None, cgroup_path=None, sig=signal.SIGTERM):
    """
    Kill a daemon by fetching the PID from the cgroup
    :param daemon_name:
    :param sub_path:
    :param cgroup_path: See daemon_cgroup_path()
    :param sig:
    :return: True if successful
    """
    try:
        pids = get_daemon_pid(daemon_name=daemon_name, sub_path=sub_path, cgroup_path=cgroup_path)
    except:
        return

    daemon_process.kill_multiple_process(pids, sig=sig)
    if sig == signal.SIGKILL:
        cgroup = daemon_cgroup(daemon_name=daemon_name, sub_path=sub_path,
                               cgroup_path=cgroup_path, create=False)
        cgroup.delete(recursive=True)
    return True


def is_daemon_running(daemon_name=None, sub_path=None, cgroup_path=None):
    """
    Check if a daemon is running
    :param daemon_name:
    :param sub_path:
    :param cgroup_path: See daemon_cgroup_path()
    :return: True if it is running
    """
    try:
        pids = get_daemon_pid(daemon_name=daemon_name, sub_path=sub_path, cgroup_path=cgroup_path)
    except:
        return False

    return len(pids) > 0


def kill_all_daemons(sub_path=None, cgroup_path=DEFAULT_DAEMON_CGROUP, sig=signal.SIGTERM):
    """
    Kills all the daemons
    :param sub_path: See daemon_pid_file()
    :param cgroup_path: The cgroup to lookup daemons
    :param sig: The signal to send
    :return: None
    """
    if sub_path is None:
        sub_path = ""
    cgroup = Cgroup(cgroup_path, sub_path, create=False)

    pids = cgroup.hierarchy_procs()
    daemon_process.kill_multiple_process(pids, sig=sig)

    if sig == signal.SIGKILL:
        cgroup.delete(recursive=True)


def clear_empty_sub_path(sub_path=None, cgroup_path=DEFAULT_DAEMON_CGROUP):
    """
    Clear sub path if it is empty
    :param sub_path: See daemon_pid_file()
    :param cgroup_path: The cgroup to lookup daemons
    :return:
    """
    try:
        cgroup = daemon_cgroup(daemon_name="", sub_path=sub_path, cgroup_path=cgroup_path,
                               create=False)
        cgroup.delete()
    except:
        pass


def daemonize(daemon_name=None, sub_path=None, cgroup_path=None):
    """
    Convert current process to a background daemon.
    The daemon can be tracked using the specified cgroup.
    :param daemon_name:
    :param sub_path:
    :param cgroup_path: See daemon_cgroup_path()
    :return: None. Will exit if fail.
    """
    cgroup = daemon_cgroup(daemon_name=daemon_name, sub_path=sub_path,
                           cgroup_path=cgroup_path, create=True)
    # Adding the current process PID will automatically add its children.
    # i.e, the daemon process
    cgroup.add_tasks(os.getpid())

    daemon_process.convert_to_daemon()

    atexit.register(__del_cgroup, cgroup.path)


#########################################################################
# Module helper functions
#########################################################################

def __del_cgroup(cgroup_path):
    """
    Remove the pid file of a daemon
    :param cgroup_path: The cgroup to remove
    :return: None
    """
    try:
        cgroup = Cgroup(cgroup_path, create=False)
    except OSError as e:
        print("Unable to open cgroup %s: %d (%s)" % (cgroup_path, e.errno, e.strerror), file=sys.stderr)
        return

    try:
        cgroup.clear_and_delete(recursive=True)
    except OSError as e:
        print("Unable to remove cgroup %s: %d (%s)" % (cgroup_path, e.errno, e.strerror), file=sys.stderr)
        return
