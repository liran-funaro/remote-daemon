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
import atexit
import os
import signal
import sys
from argparse import ArgumentParser

import rdaemon.process as daemon_process
from ginseng.util import shell

DEFAULT_DAEMON_PID_FOLDER = "/tmp/rdaemons"


#########################################################################
# Module PID file API
#########################################################################

def default_daemon_pid_file(daemon_name, sub_path=""):
    """
    Default PID file is /tmp/daemons/<daemon_name>.pid
    :param daemon_name: The daemon name
    :param sub_path: A sub path for a specific group of daemons
    :return: The default daemon PID file
    """
    return os.path.join(DEFAULT_DAEMON_PID_FOLDER, sub_path, f"{daemon_name}.pid")


def daemon_pid_file(daemon_name=None, sub_path="", pid_file=None):
    """
    Retrieve the daemon PID file
    :param daemon_name: The daemon name (if pid_file is None)
    :param sub_path: A sub path for a specific group of daemons
    :param pid_file: Can specify the pid file directly
    :return: The pid file or raise exception if bad parameters
    """
    if pid_file is not None:
        if not isinstance(pid_file, str):
            raise ValueError("PID file path must be a string")
        return pid_file
    else:
        if not isinstance(daemon_name, str):
            raise ValueError("If PID file was not specified, daemon name must be string")
        return default_daemon_pid_file(daemon_name, sub_path)


#########################################################################
# Module interface API
#########################################################################

def get_daemon_pid(daemon_name=None, sub_path="", pid_file=None):
    """
    Read the daemon PID from a file
    :param daemon_name, sub_path, pid_file: See daemon_pid_file()
    :return: The PID as integer or raise error
    """
    pid_file = daemon_pid_file(daemon_name=daemon_name, sub_path=sub_path, pid_file=pid_file)

    try:
        with open(pid_file, "r") as fp:
            pid_data = fp.read()
        return int(pid_data)
    except:
        raise ValueError("The daemon is not active. "
                         "No pid file named: %s" % pid_file)


def kill_daemon(daemon_name=None, sub_path="", pid_file=None, sig=signal.SIGTERM):
    """
    Kill a daemon by fetching the PID from the file
    :param daemon_name, sub_path, pid_file: See daemon_pid_file()
    :return: True if successful
    """
    pid_file = daemon_pid_file(daemon_name=daemon_name, sub_path=sub_path, pid_file=pid_file)
    pid = get_daemon_pid(pid_file=pid_file)
    success = daemon_process.kill_process(pid, sig)
    try:
        if success and sig == signal.SIGKILL:
            __delpid(pid_file)
    except:
        pass
    return success


def is_daemon_running(daemon_name=None, sub_path="", pid_file=None):
    """
    Check if a daemon is running
    :param daemon_name, sub_path, pid_file: See daemon_pid_file()
    :return: True if it is running
    """
    pid_file = daemon_pid_file(daemon_name=daemon_name, sub_path=sub_path, pid_file=pid_file)

    try:
        pid = get_daemon_pid(pid_file=pid_file)
    except:
        return False

    exists = daemon_process.pid_exists(pid)
    if not exists:
        __delpid(pid_file)

    return exists


def kill_all_daemons(sub_path="", pid_path=DEFAULT_DAEMON_PID_FOLDER, sig=signal.SIGTERM):
    """
    Kills all the daemons
    :param sub_path: See daemon_pid_file()
    :param pid_path: The folder to lookup pid files
    :param sig: The signal to send
    :return: None
    """
    folder = os.path.join(pid_path, sub_path)
    try:
        pid_path_list = [os.path.join(folder, f) for f in os.listdir(folder)]
    except:
        return

    for pid_file_path in pid_path_list:
        if os.path.isfile(pid_file_path):
            try:
                kill_daemon(pid_file=pid_file_path, sig=sig)
            except:
                pass


def clear_empty_sub_path(sub_path="", pid_path=DEFAULT_DAEMON_PID_FOLDER):
    """
    Clear sub path if it is empty
    :param sub_path: See daemon_pid_file()
    :param pid_path: The folder to lookup pid files
    :return:
    """
    folder = os.path.join(pid_path, sub_path)
    try:
        os.removedirs(folder)
    except:
        pass


def daemonize(daemon_name=None, sub_path="", pid_file=None):
    """
    Convert current process to a background daemon.
    The daemon can be tracked using the specified pid file.
    :param daemon_name, sub_path, pid_file: See daemon_pid_file()
    :return: None. Will exit if fail.
    """
    pid_file = daemon_pid_file(daemon_name=daemon_name, sub_path=sub_path, pid_file=pid_file)
    daemon_process.convert_to_daemon()

    __write_daemon_pid_file(pid_file)
    atexit.register(__delpid, pid_file)


#########################################################################
# Module helper functions
#########################################################################


def __write_daemon_pid_file(pid_file):
    """
    Write the current process PID to a file
    :param pid_file: The file to write the PID to
    :return: None. Will exit if fail.
    """
    pid_file_folder = os.path.dirname(pid_file)
    if not os.path.isdir(pid_file_folder):
        os.makedirs(pid_file_folder)

    pid = str(os.getpid())
    try:
        with open(pid_file, "w+") as f:
            f.write("%s\n" % pid)
    except EnvironmentError as e:
        print("Daemon failed to write PID file: %d (%s)" % (e.errno, e.strerror), file=sys.stderr)
        sys.exit(1)


def __delpid(pid_file):
    """
    Remove the pid file of a daemon
    :param pid_file: The pid file to remove
    :return: None
    """
    try:
        os.remove(pid_file)
    except OSError as e:
        print("Unable to remove PID file %s: %d (%s)" % (pid_file, e.errno, e.strerror), file=sys.stderr)


###########################################################################
# Main
###########################################################################

if __name__ == "__main__":
    description = "A runnable application to start/end a daemon and read it's output"
    params = dict(pid_file="/tmp/%(daemon_local_name)s.pid",
                  output_file="/tmp/%(daemon_local_name)s.out",
                  daemon_name="%(daemon_local_name)s"
                  )
    execute_cmd = "%(execute)s &> %(output_file)s & echo $! > %(pid_file)s"

    parser = ArgumentParser(description=description)
    parser.add_argument("daemon_local_name", metavar="DAEMON-NAME")
    parser.add_argument("-e", "--execute-daemon", dest="execute", metavar="CMD",
                        help="Will execute the command to start the daemon."
                             "If the daemon is already running, won't start it again.")
    parser.add_argument("-o", "--get-output-file", dest="get_output", action="store_true",
                        help="Print to stdout the name of the output file for this daemon")
    parser.add_argument("-k", "--kill", action="store_true")
    parser.add_argument("-c", "--check-if-running", dest="is_running",
                        action="store_true")

    args = parser.parse_args()
    if args.daemon_local_name is None:
        exit(1)

    args_vars = vars(args)
    for key, value in params.items():
        if isinstance(value, str):
            params[key] = value % args_vars
    params.update(args_vars)

    pid_file = params['pid_file']
    output_file = params['output_file']
    daemon_name = params['daemon_name']

    if args.execute is not None:
        if is_daemon_running(daemon_name, pid_file):
            print("Backend server with this name is already running: %(daemon_name)s" % params)
            exit(1)

        try:
            os.remove(pid_file)
        except:
            pass

        try:
            os.remove(output_file)
        except:
            pass

        shell.run_shell(execute_cmd % params)

    if args.get_output is True:
        print(output_file)

    if args.kill is True:
        kill_daemon(daemon_name, pid_file)

    if args.is_running is True:
        print(is_daemon_running(daemon_name, pid_file))
