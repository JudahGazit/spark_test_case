import os
import sys
import time
import traceback
import unittest

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

from spark_test_case import consts
from spark_test_case.DirectoryReloader import DirectoryReloader
from spark_test_case.DirectoryUpdateWatcher import DirectoryUpdateWatcher


class UnittestRunner:
    def __init__(self, append_spark_config=None):
        append_spark_config = append_spark_config or {}
        self.__create_spark_context(append_spark_config)
        self.loader = unittest.TestLoader()

    def __create_spark_context(self, append_spark_config):
        conf = consts.SPARK_DEFAULT_CONF
        conf.update(append_spark_config)
        sc = SparkContext(conf=SparkConf().setAll(conf.items()))
        sc.setLogLevel('WARN')
        self.spark = SparkSession(sc.getOrCreate())

    def __run_tests_in_module(self, test_module):
        suite = self.loader.loadTestsFromModule(test_module)
        runner = unittest.TextTestRunner(sys.stdout, verbosity=3)
        runner.run(suite)

    def __try_rerun_tests(self, test_module):
        try:
            reloader = DirectoryReloader()
            # Doing twice in order to make sure that all imports are using the most updated version
            reloader.reload_directory(os.getcwd())
            reloader.reload_directory(os.getcwd())
            self.__run_tests_in_module(test_module)
        except Exception:
            traceback.print_exc()

    def __run_tests_when_directory_modified(self, test_module, watcher, max_modified_time):
        last_modified_time = watcher.get_dir_modified_time(os.getcwd())
        if last_modified_time > max_modified_time:
            self.__try_rerun_tests(test_module)
            return last_modified_time

    def run(self, test_module, wait_time=consts.WAIT_TIME_BETWEEN_CHECKS):
        max_directory_modified_time = 0
        watcher = DirectoryUpdateWatcher()
        while True:
            max_directory_modified_time = self.__run_tests_when_directory_modified(test_module, watcher,
                                                                                   max_directory_modified_time)
            time.sleep(wait_time)
