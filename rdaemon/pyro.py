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
import multiprocessing
import signal
import threading
import time
from threading import Thread

import Pyro4
import rdaemon.cgroup as daemon_cgroup
import rdaemon.file as daemon_file
from rdaemon.daemons import wrap_as_daemon, DaemonThread
from rdaemon.interfaces import IDaemon
from rdaemon.launch import launch_daemon
from ginseng.util.config import SimplerConfig
from zope.interface.declarations import implementer

from ginseng.util.logging import LoggedEntity, read_logging_configuration


def launch_pyro_daemon(name, conf=None, log_conf=None,
                       conf_string=None, conf_sub_path=None,
                       daemon_group="", daemon_bookkeeping='file'):
    """
    Launch a daemon controlled using Pyro.
    If the configured class implements IDeamon, will start the daemon in a
    sub-thread of the new process.
    """
    if conf is not None:
        if log_conf is None:
            log_conf = read_logging_configuration(conf)
        conf_string = conf.root.dumps()
        conf_sub_path = conf.sub_path
    else:
        if log_conf is None or conf_string is None or conf_sub_path is None:
            raise ValueError("If configuration is not supplied, "
                             "the following must be available: "
                             "log_conf, conf_string and conf_sub_path")

    launch_daemon(name, PyroDaemon.run_pyro_daemon,
                  kwargs=dict(name=name, conf_string=conf_string, conf_sub_path=conf_sub_path),
                  daemon_group=daemon_group, log_conf=log_conf,
                  daemon_bookkeeping=daemon_bookkeeping)


@implementer(IDaemon)
class PyroDaemon(LoggedEntity):
    """
    Runs the daemon. run() should be called inside a new process/daemon.
    """

    def __init__(self, name, daemon_object):
        LoggedEntity.__init__(self, name)
        self.name = name
        self.daemon_object = daemon_object

        self.daemon_thread = None
        self.pyro_daemon = None
        self.pyro_thread = None
        self.daemon_uri = None
        atexit.register(self.finalize)

    @classmethod
    def run_pyro_daemon(cls, name, conf=None, conf_file_path=None,
                        conf_string=None, conf_sub_path=tuple()):
        """
        The target function of the new pyro daemon.
        """
        daemon_object = cls.create_daemon_object(conf, conf_file_path, conf_string, conf_sub_path)
        daemon = cls(name, daemon_object)
        signal.signal(signal.SIGTERM, daemon.sigterm_handler)
        daemon.run()

    ######################################################################
    # Interface
    ######################################################################

    def run(self):
        """ IDaemon interface function """
        try:
            try:
                self.initialize()
            except Exception as e:
                self.log_exception("Exception in daemon process: %s", e)
            else:
                self.log_info("Starting daemon")
                self.daemon_thread.start()
                try:
                    self.daemon_thread.join()
                except Exception as e:
                    self.log_warning("Exception occurred while waiting for daemon: %s", e)
                self.log_info("Daemon finished")
        except Exception as e:
            self.log_exception("Exception in daemon execution: %s", e)
        finally:
            try:
                self.finalize()
            except Exception as e:
                self.log_exception("Exception in daemon finalizing: %s", e)

    def initialize(self):
        """
        Wraps daemon object if needed.
        Initialize the pyro daemon.
        :return: None
        """
        self.init_daemon_object()
        self.init_pyro_daemon()

    def finalize(self):
        """
        Terminate the daemon.
        Shutdown pyro daemon and waits.
        Unregister and close pyro daemon.
        :return: None
        """
        self.terminate()
        self.shutdown()
        self.join_pyro_thread()
        self.unregister_daemon()
        self.close_pyro_daemon()

    def notify(self, *args, **kwargs):
        """ IDaemon interface function """
        try:
            self.daemon_object.notify(*args, **kwargs)
        except Exception as e:
            self.log_exception("Exception while notifying daemon: %s", e)

    def terminate(self):
        """ IDaemon interface function """
        try:
            self.daemon_object.terminate()
        except Exception as e:
            self.log_exception("Exception while terminating daemon: %s", e)

    def is_terminated(self):
        """ IDaemon interface function """
        try:
            return self.daemon_object.is_terminated()
        except Exception as e:
            self.log_exception("Exception while checking if daemon is terminated: %s", e)

    def shutdown(self):
        """
        Shutdown the pyro daemon.
        Will stop the request loop and will cleanly exit.
        :return: None
        """
        if self.pyro_daemon is not None:
            try:
                self.pyro_daemon.shutdown()
            except Exception as e:
                self.log_exception("Exception while shutting down pyro daemon: %s", e)

    def is_pyro_thread_alive(self):
        """
        Check if the pyro daemon is still alive
        :return: True if the thread is alive, None if no thread
        """
        if self.pyro_thread:
            return self.pyro_thread.is_alive()

    def join_pyro_thread(self, timeout=None):
        """
        Waits for the pyro daemon thread to finish
        :param timeout: Timeout to wait
        :return: see join()
        """
        if self.pyro_thread is not None:
            try:
                return self.pyro_thread.join(timeout)
            except Exception as e:
                self.log_exception("Exception while waiting for pyro daemon: %s", e)

    def __getattr__(self, item):
        """
        Extent the pyro daemon object to have the same functionality as the daemon object.
        Only applicable for attributes that are not already taken by the pyro object.
        :param item: The attribute name
        :return: This attribute from the daemon object
        """
        if item == 'daemon_object':
            raise AttributeError("Daemon object is not initialized yet")
        return getattr(self.daemon_object, item)

    ######################################################################
    # Private helper functions
    ######################################################################

    @classmethod
    def init_conf(cls, conf=None, conf_file_path=None, conf_string=None):
        """
        Get the configuration for the process
        :param conf: (Optional) The configuration object
        :param conf_file_path: (Optional) Full path of the configuration file
        :param conf_string: (Optional) Configuration as string
        :param conf_categories: (Optional) The categories to use
        :return: A configuration object
        """
        if conf is not None:
            pass
        elif conf_file_path:
            conf = SimplerConfig(file_path=conf_file_path)
        elif conf_string:
            conf = SimplerConfig.from_string(conf_string)
        else:
            raise Exception("No configuration supplied")

        conf.set_immutable()
        return conf

    @classmethod
    def create_daemon_object(cls, conf=None, conf_file_path=None, conf_string=None, conf_sub_path=None):
        """
        Create the daemon object using a configuration
        :param conf: The configuration
        :param conf_sub_path: A sub-path of the configuration
        :return: A daemon object, or raise exception
        """
        conf = cls.init_conf(conf, conf_file_path, conf_string)
        if conf_sub_path:
            conf = conf[tuple(conf_sub_path)]

        daemon_object = conf.instance
        if daemon_object is None:
            raise Exception("No daemon object available")

        return daemon_object

    def init_daemon_object(self):
        """
        If the daemon object implements IDeamon, will use it as is.
        Otherwise, will wrap it around daemon functionality
        """
        if not IDaemon.providedBy(self.daemon_object):
            wrap_as_daemon(self.daemon_object, daemon_name=self.name)

        self.daemon_thread = DaemonThread(self.daemon_object)

    def init_pyro_daemon(self):
        """
        Init the pyro daemon and register the daemon object as pyro object.
        Then, will register the daemon in the name-server.
        """
        Pyro4.config.REQUIRE_EXPOSE = False

        if self.pyro_daemon is None:
            self.pyro_daemon = Pyro4.Daemon()

        self.log_info("Registering pyro daemon")
        self.daemon_uri = self.pyro_daemon.register(self.daemon_object)
        with name_server_lookup(30) as ns:
            ns.register(self.name, self.daemon_uri, safe=True)

        self.log_info("Staring pyro daemon thread")
        self.pyro_thread = Thread(name="%s-pyro" % self.name,
                                  target=self.request_loop, daemon=True)
        self.pyro_thread.start()

    def request_loop(self):
        """
        Starts the request loop until SIGTERM received.
        """
        self.log_info("Daemon %s is available in Pyro4 name server", self.name)
        self.pyro_daemon.requestLoop()
        self.log_info("Daemon %s pyro server is terminated", self.name)

    def sigterm_handler(self, signal_number, current_stack_frame):
        """
        Handle SIGTERM event.
        Will shutdown the daemon request loop if running.
        """
        Thread(target=self.__sigterm_thread__, daemon=True).start()

    def __sigterm_thread__(self):
        """
        Handle SIGTERM event in a thread
        """
        self.terminate()
        self.shutdown()

    def unregister_daemon(self):
        """ If daemon exists, unregister it """
        if self.daemon_object and self.pyro_daemon:
            try:
                self.log_info("Unregistering pyro daemon")
                self.pyro_daemon.unregister(self.daemon_object)
            except Exception as e:
                self.log_warning("Failed to unregister daemon object: %s", e)

            try:
                self.log_info("Removing pyro daemon from name space")
                with name_server_lookup() as ns:
                    ns.remove(self.name)
            except Exception as e:
                self.log_warning("Failed to unregister daemon name: %s", e)

    def close_pyro_daemon(self):
        """ If pyro daemon object is open, close it """
        if self.pyro_daemon:
            try:
                self.log_info("Closing pyro daemon")
                self.pyro_daemon.close()
            except Exception as e:
                self.log_warning("Failed to close pyro daemon: %s", e)


class PyroDaemonThread(PyroDaemon, threading.Thread):
    """
    Initiate a pyro daemon in its own thread
    """

    def __init__(self, name, daemon_object):
        threading.Thread.__init__(self, name=name, daemon=True)
        PyroDaemon.__init__(self, name, daemon_object)


class PyroDaemonProcess(multiprocessing.Process):
    """
    Initiate a pyro daemon in its own process
    """

    def __init__(self, name, conf=None, conf_file_path=None, conf_string=None, conf_sub_path=None):
        multiprocessing.Process.__init__(self, name=name, target=PyroDaemon.run_pyro_daemon,
                                         kwargs=dict(
                                             name=name, conf=conf, conf_file_path=conf_file_path,
                                             conf_string=conf_string, conf_sub_path=conf_sub_path
                                         ))
        self.daemon=True


class PyroDaemonsDeployment(LoggedEntity):
    """
    Deploy multiple daemons
    """

    def __init__(self, conf, daemon_group="", daemon_bookkeeping="cgroup"):
        LoggedEntity.__init__(self)
        self.deploy_daemons = set(conf.deploy_daemons)
        self.log_conf = read_logging_configuration(conf)
        self.conf_string = conf.root.dumps()
        self.conf_sub_path = conf.sub_path
        self.active_daemons = set()

        self.daemon_group = daemon_group
        self.daemon_bookkeeping = daemon_bookkeeping
        if daemon_bookkeeping == "cgroup":
            self.daemon_module = daemon_cgroup
        else:
            self.daemon_module = daemon_file

    def deploy(self, wait_timeout_sec=30):
        """
        Deploy all the daemons that are not already deployed
        :return: None
        """
        for daemon_name in self.deploy_daemons:
            if daemon_name not in self.active_daemons:
                self.deploy_daemon(daemon_name)

        services = PyroServices()
        for daemon_name in self.deploy_daemons:
            services.daemon(daemon_name, wait_timeout_sec)

    def clear_dead_daemons(self):
        """
        Clear the dead daemon from the processes list
        :return: None
        """
        for name in list(self.active_daemons):
            if not self.daemon_module.is_daemon_running(daemon_name=name, sub_path=self.daemon_group):
                self.log_warning("Daemon %s is dead", name)
                self.active_daemons.remove(name)

    def deploy_daemon(self, daemon_name):
        """
        Deploy a daemon. If already exists, raise error.
        :param daemon_name: The daemon name in the configuration
        :return: None
        """
        if daemon_name in self.active_daemons:
            raise Exception("Daemon %s already deployed" % daemon_name)

        launch_pyro_daemon(daemon_name, log_conf=self.log_conf,
                           conf_string=self.conf_string,
                           conf_sub_path=tuple([*self.conf_sub_path, daemon_name]),
                           daemon_group=self.daemon_group,
                           daemon_bookkeeping=self.daemon_bookkeeping
                           )
        self.active_daemons.add(daemon_name)

    def terminate_all(self):
        """
        Terminate all the active daemons
        :param join_timeout: Time to wait for the daemon to end
        :return: None
        """
        for name in list(self.active_daemons):
            self.terminate_daemon(name)

    def terminate_daemon(self, daemon_name):
        """
        Terminate a daemon
        :param daemon_name: The daemon name
        :return: see kill_daemon()
        """
        ret = self.daemon_module.kill_daemon(daemon_name=daemon_name,
                                             sub_path=self.daemon_group,
                                             sig=signal.SIGTERM)
        self.clear_dead_daemons()
        return ret

    def is_all_running(self):
        """
        :return: True if all the deployed daemons are still running
        """
        self.clear_dead_daemons()
        missing = self.deploy_daemons - self.active_daemons
        if len(missing) == 0:
            return True

        self.log_warning("The following daemons are missing: %s", list(missing))
        return False

    def daemons_status(self):
        """
        :return: A dict - for each daemons: is running
        """
        self.clear_dead_daemons()
        return {d:(d in self.active_daemons) for d in self.deploy_daemons}

    def join(self, timeout=None, check_interval=1):
        """
        Waits for all the daemons to finish
        :return: True if all finished
        """
        start = time.time()
        while timeout is None or start + timeout < time.time():
            self.clear_dead_daemons()
            if len(self.active_daemons) == 0:
                return True
            time.sleep(check_interval)

        return False

###################################################################################
# Pyro name-server
###################################################################################

def name_server_lookup(wait_timeout_sec=0):
    """
    Wait for name server to be active
    :param wait_timeout_sec: Time to wait for name server
    :return: name server if active, raise exception otherwise
    """
    end_time = time.time() + wait_timeout_sec
    again = True
    while again:
        try:
            return Pyro4.locateNS()
        except Exception as e:
            again = time.time() < end_time
            if not again:
                raise e
            else:
                time.sleep(0.5)


def is_name_server_alive(wait_timeout_sec=0):
    """
    Check if pyro name server is alive
    :param wait_timeout_sec: Time to wait for name server
    :return: True if alive, False otherwise
    """
    try:
        name_server_lookup(wait_timeout_sec)
    except:
        return False
    return True


def name_server_start(conf=None, log_conf=None):
    """
    Starts the pyro name server if it is not already active
    :param conf: A configuration to read the logging configuration from
    :param log_conf: Optionally, supply the log configuration directly
    :return: True if started, False if was active
    """
    if is_name_server_alive():
        return False

    if log_conf is None:
        log_conf = read_logging_configuration(conf)
    launch_daemon('pyro-name-server', Pyro4.naming.startNSloop,
                  kwargs=dict(host="0.0.0.0", enableBroadcast=True),
                  log_conf=log_conf,
                  daemon_bookkeeping='file')
    return True


def name_server_stop():
    """
    Stop the name server
    :return: True if successful
    """
    return daemon_file.kill_daemon(daemon_name='pyro-name-server', sig=signal.SIGTERM)


###################################################################################
# Service manager for Pyro
###################################################################################

class PyroServices(LoggedEntity):
    """
    Fetch a different Pyro Daemon object for each thread
    """

    def __init__(self):
        LoggedEntity.__init__(self)
        self.__daemon_uri = {}
        self.__thread_daemon_object = threading.local()

    #################################################################
    # Interface
    #################################################################

    def locate_daemon_uri(self, daemon_name, wait_timeout_sec=0):
        """
        :param daemon_name: The daemon name
        :param wait_timeout_sec: Time to wait for the name-server
        :return: The URI of the daemon if it is registered
        """
        with name_server_lookup(wait_timeout_sec) as ns:
            return ns.lookup(daemon_name)

    def daemon_uri(self, daemon_name, wait_timeout_sec=0):
        """
        Get the daemon URI from cache or check name-server if not read before
        :param wait_timeout_sec: Time to wait for the name-server
        :param daemon_name: The daemon name
        :return: The URI of the daemon if it is registered
        """
        if daemon_name not in self.__daemon_uri:
            self.__daemon_uri[daemon_name] = self.locate_daemon_uri(daemon_name, wait_timeout_sec)
        return self.__daemon_uri[daemon_name]

    def create_daemon_object(self, daemon_name, wait_timeout_sec=0):
        """
        :param daemon_name:
        :param wait_timeout_sec: Time to wait for the name-server
        :return: A new object for that daemon
        """
        uri = self.daemon_uri(daemon_name, wait_timeout_sec)
        return Pyro4.Proxy(uri)

    def daemon(self, daemon_name, wait_timeout_sec=0):
        """
        Get a daemon proxy.
        :param daemon_name: The daemon name
        :param wait_timeout_sec: Time to wait for the daemon to spawn
        :return: A thread local object of the daemon
        """
        daemons = self.__local_daemons()
        if daemon_name in daemons:
            return daemons[daemon_name]

        end_time = time.time() + wait_timeout_sec
        again = True
        while again:
            try:
                daemons[daemon_name] = self.create_daemon_object(daemon_name, wait_timeout_sec)
                return daemons[daemon_name]
            except Exception as e:
                again = time.time() < end_time
                if not again:
                    raise e
                else:
                    time.sleep(0.5)

    def async_daemon(self, daemon_name, wait_timeout_sec=0):
        """
        Get a daemon asynchronous proxy.
        :param daemon_name: The daemon name
        :param wait_timeout_sec: Time to wait for the daemon to spawn
        :return: A thread local object of the daemon
        """
        proxy = self.daemon(daemon_name, wait_timeout_sec)
        return Pyro4.async(proxy)

    def list_daemons(self, wait_timeout_sec=0):
        """
        :param wait_timeout_sec: Time to wait for the name-server
        :return: The registered items as a dictionary name-to-URI
        """
        with name_server_lookup(wait_timeout_sec) as ns:
            return ns.list()

    def __getitem__(self, item):
        return self.daemon(item)

    #################################################################
    # Helper functions
    #################################################################

    def __local_daemons(self):
        """
        :return: A thread local map to daemon objects
        """
        if not hasattr(self.__thread_daemon_object, 'daemons'):
            self.__thread_daemon_object.daemons = {}
        return self.__thread_daemon_object.daemons