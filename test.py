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
import os
import threading
import time
import unittest

import rdaemon.bookkeeping.cgroups as daemon_cgroup
import rdaemon.bookkeeping.file as daemon_file
import rdaemon.pyro as pyro
from rdaemon.daemons import TestCounterDaemon, TestSleeperDaemon
from simpleconfig import SimplerConfig

from rdaemon.logging import default_log_conf, LogManager

from rdaemon.daemons import PeriodicDaemon, DaemonThread
from rdaemon.tasks import TestPeriodicTask


class TestCounter:

    def __init__(self):
        self.c = 0

    def add(self):
        self.c += 1

    def count(self):
        return self.c


TEST_OUTPUT_PATH = "/tmp/test-pyro"


class TestPyroDaemon(unittest.TestCase):
    def setUp(self):
        daemon_file.kill_all_daemons()
        daemon_cgroup.kill_all_daemons()

        if not os.path.isdir(TEST_OUTPUT_PATH):
            os.makedirs(TEST_OUTPUT_PATH)

        log_conf = default_log_conf()
        log_conf['output_path'] = TEST_OUTPUT_PATH
        pyro.name_server_start(log_conf=log_conf)
        LogManager.init_logger("unit-test", **log_conf)

    def tearDown(self):
        pyro.name_server_stop()

        daemon_file.kill_all_daemons()
        daemon_cgroup.kill_all_daemons()

    def get_base_config(self):
        conf = SimplerConfig()
        conf.globals.logging.verbosity = logging.DEBUG
        conf.globals.logging.color = True
        conf.globals.logging.output_path = TEST_OUTPUT_PATH
        conf.globals.logging.max_bytes = 0
        conf.globals.logging.backups_count = 0
        return conf

    def assertDaemonCleanup(self, daemon_name, daemon_group="", daemon_bookkeeping='file'):
        if daemon_bookkeeping == 'file':
            pid_file = daemon_file.daemon_pid_file(daemon_name=daemon_name,
                                                   sub_path=daemon_group)
            self.assertFalse(os.path.exists(pid_file))
        elif daemon_bookkeeping == 'cgroups':
            cgroup = daemon_cgroup.daemon_cgroup(daemon_name=daemon_name,
                                                 sub_path=daemon_group)
            self.assertTupleEqual(cgroup.subsystems, tuple())

    def test_naming_exists(self):
        ret = pyro.list_daemons()
        self.assertTrue('Pyro.NameServer' in ret)

    def test_register_daemon_manual_terminate_thread(self):
        conf = self.get_base_config()
        conf.daemon = TestCounterDaemon
        conf_string = conf.dumps()

        counter = pyro.PyroDaemonProcess("test.counter",
                                         conf_string=conf_string, conf_sub_path=["daemon"])
        counter.start()

        expected_counts = 127

        try:
            services = pyro.PyroServices()
            counter_proxy = services.daemon("test.counter", 3)
            for _ in range(expected_counts):
                counter_proxy.notify()

            count = counter_proxy.get_notify_count()
            self.assertEqual(count, expected_counts)

            counter_proxy.terminate()
            is_terminated = counter_proxy.is_terminated()
            self.assertTrue(is_terminated)
            self.assertTrue(counter.is_alive())
        finally:
            try:
                counter.terminate()
                counter.join()
            except:
                pass

    def test_register_daemon_auto_terminate_thread(self):
        conf = self.get_base_config()
        conf.daemon = TestCounterDaemon
        conf_string = conf.dumps()

        counter = pyro.PyroDaemonProcess("test.counter",
                                         conf_string=conf_string, conf_sub_path=["daemon"])
        counter.start()

        expected_counts = 127

        try:
            services = pyro.PyroServices()
            counter_proxy = services.daemon("test.counter", 3)
            for _ in range(expected_counts):
                counter_proxy.notify()

            count = counter_proxy.get_notify_count()
            self.assertEqual(count, expected_counts)

            is_terminated = counter_proxy.is_terminated()
            self.assertFalse(is_terminated)
        finally:
            try:
                counter.terminate()
                counter.join()
            except:
                pass

    def test_wait_for_daemon_fail(self):
        services = pyro.PyroServices()
        with self.assertRaises(Exception) as context:
            services.daemon("test.counter", 3)

    def test_wait_for_daemon_success(self):
        conf = self.get_base_config()
        conf.daemon = TestCounterDaemon
        conf_string = conf.dumps()

        counter = pyro.PyroDaemonProcess("test.counter",
                                         conf_string=conf_string, conf_sub_path=["daemon"])
        start_thread = threading.Timer(5, counter.start)

        try:
            services = pyro.PyroServices()
            start_thread.start()
            counter_proxy = services.daemon("test.counter", 10)
            counter_proxy.notify()

            count = counter_proxy.get_notify_count()
            self.assertEqual(count, 1)
        finally:
            try:
                start_thread.cancel()
                start_thread.join()
            except:
                pass
            try:
                counter.terminate()
                counter.join()
            except:
                pass

    def test_wait_for_daemon_too_late(self):
        conf = self.get_base_config()
        conf.daemon = TestCounterDaemon
        conf_string = conf.dumps()

        counter = pyro.PyroDaemonProcess("test.counter",
                                         conf_string=conf_string, conf_sub_path=["daemon"])
        start_thread = threading.Timer(15, counter.start)

        try:
            services = pyro.PyroServices()
            start_thread.start()
            with self.assertRaises(Exception) as context:
                counter_proxy = services.daemon("test.counter", 10)
        finally:
            try:
                start_thread.cancel()
                start_thread.join()
            except:
                pass
            try:
                counter.terminate()
                counter.join()
            except:
                pass

    def test_deployment_file(self):
        self.base_deployment('file')

    def test_deployment_cgroup(self):
        self.base_deployment('cgroups')

    def base_deployment(self, daemon_bookkeeping):
        daemon_group = "system"
        conf = self.get_base_config()
        conf.system["test.counter1"] = TestCounterDaemon
        conf.system["test.counter2"] = TestCounterDaemon
        conf.system.deploy_daemons = ["test.counter1", "test.counter2"]
        conf.set_immutable()

        deploy = pyro.PyroDaemonsDeployment(conf.system, daemon_group=daemon_group,
                                            daemon_bookkeeping=daemon_bookkeeping)

        deploy.deploy()
        # time.sleep(60)

        expected_counts_1 = 13
        expected_counts_2 = 27

        try:
            services = pyro.PyroServices()
            counter_1_proxy = services.daemon("test.counter1")
            counter_2_proxy = services.daemon("test.counter2")
            for _ in range(expected_counts_1):
                counter_1_proxy.notify()

            for _ in range(expected_counts_2):
                counter_2_proxy.notify()

            count_1 = counter_1_proxy.get_notify_count()
            count_2 = counter_2_proxy.get_notify_count()
            self.assertEqual(count_1, expected_counts_1)
            self.assertEqual(count_2, expected_counts_2)

            self.assertTrue(deploy.is_all_running())

            deploy.terminate_daemon("test.counter1")
            time.sleep(2)
            self.assertFalse(deploy.is_all_running())
        finally:
            deploy.terminate_all()
            deploy.join()

        self.assertDaemonCleanup("test.counter1", daemon_group=daemon_group, daemon_bookkeeping=daemon_bookkeeping)
        self.assertDaemonCleanup("test.counter2", daemon_group=daemon_group, daemon_bookkeeping=daemon_bookkeeping)
        self.assertDaemonCleanup("", daemon_group=daemon_group, daemon_bookkeeping=daemon_bookkeeping)

    def test_daemon_process_conf(self):
        conf = self.get_base_config()
        conf.daemon = TestCounterDaemon

        counter = pyro.PyroDaemonProcess("test.counter", conf.daemon)
        counter.start()

        expected_counts = 127

        try:
            services = pyro.PyroServices()
            counter_proxy = services.daemon("test.counter", 3)
            for _ in range(expected_counts):
                counter_proxy.notify()

            count = counter_proxy.get_notify_count()
            self.assertEqual(count, expected_counts)
        finally:
            try:
                counter.terminate()
                counter.join()
            except:
                pass

    def test_daemon_thread(self):
        counter_object = TestCounterDaemon()
        counter = pyro.PyroDaemonThread("test.counter", counter_object)
        counter.start()

        expected_counts = 127

        try:
            services = pyro.PyroServices()
            counter_proxy = services.daemon("test.counter", 3)
            for _ in range(expected_counts):
                counter_proxy.notify()

            count = counter_proxy.get_notify_count()
            self.assertEqual(count, expected_counts)

            self.assertFalse(counter.is_terminated())
            self.assertTrue(counter.is_alive())
        finally:
            counter.terminate()
            counter.join()

    def test_daemon_thread_join(self):
        sleeper_object = TestSleeperDaemon(sleep_time=5)
        sleeper = pyro.PyroDaemonThread("test.sleeper", sleeper_object)
        sleeper.start()

        try:
            services = pyro.PyroServices()
            sleeper_proxy = services.async_daemon("test.sleeper", 3)
            sleeper_proxy.terminate()

            t1 = time.time()
            sleeper.join()
            t2 = time.time()
            self.assertGreaterEqual(t2 - t1, 4)
            self.assertTrue(sleeper.is_terminated())
            self.assertFalse(sleeper.is_pyro_thread_alive())
            self.assertFalse(sleeper.is_alive())
        finally:
            sleeper.terminate()
            sleeper.join()

    def test_daemon_async(self):
        counter_object = TestCounterDaemon()
        counter = pyro.PyroDaemonThread("test.counter", counter_object)
        counter.start()

        compute_time = 5
        compute_result = 'good'

        try:
            services = pyro.PyroServices()
            counter_proxy = services.async_daemon("test.counter", 3)
            future = counter_proxy.long_operation(compute_time, compute_result)
            self.assertEqual(future.ready, False)
            finish = future.wait(compute_time + 1)
            self.assertTrue(finish)
            self.assertEqual(future.value, compute_result)
        finally:
            counter.terminate()
            counter.join()

    def test_pyro_daemon_thread_with_non_daemon_class(self):
        counter_object = TestCounter()
        counter = pyro.PyroDaemonThread("test.counter", counter_object)
        counter.start()

        expected_counts = 127

        try:
            services = pyro.PyroServices()
            counter_proxy = services.daemon("test.counter", 3)
            for _ in range(expected_counts):
                counter_proxy.add()

            count = counter_proxy.count()
            self.assertEqual(count, expected_counts)
        finally:
            try:
                counter.terminate()
                counter.join()
            except:
                pass


class UnitTestPeriodicTask(unittest.TestCase):

    def test_periodic_task(self):
        task = TestPeriodicTask()
        daemon = PeriodicDaemon(task, 1)
        daemon_thread = DaemonThread(daemon)

        expected_count = 5
        daemon_thread.start()
        time.sleep(expected_count)
        daemon_thread.terminate()
        count = task.get_count()
        self.assertGreaterEqual(count, expected_count - 1)
        self.assertLessEqual(count, expected_count + 1)
