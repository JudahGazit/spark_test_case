import sys
import os

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import unittest

from spark_test_case import consts


class UnittestRunner:
    def __init__(self, append_spark_config={}):
        self.__create_spark_context(append_spark_config)
        self.loader = unittest.TestLoader()

    def __create_spark_context(self, append_spark_config):
        conf = consts.SPARK_DEFAULT_CONF
        conf.update(append_spark_config)
        sc = SparkContext(conf=SparkConf().setAll(conf.items()))
        sc.setLogLevel('WARN')
        self.spark = SparkSession(sc.getOrCreate())

    def _run_tests_in_module(self, test_module):
        suite = self.loader.loadTestsFromModule(test_module)
        runner = unittest.TextTestRunner(sys.stdout, verbosity=3)
        runner.run(suite)

    def __reload_all_modules(self):
        modules = sys.modules.values()
        for module in modules:
            if module is not None:
                module_is_main = module.__name__ == '__main__'
                module_in_working_directory = hasattr(module, '__file__') and os.getcwd() in module.__file__
                if not module_is_main and module_in_working_directory:
                    reload(module)

    def run(self, test_module):
        while True:
            raw_input('Press ENTER to rerun...')
            print 'Running...'
            self.__reload_all_modules()
            self._run_tests_in_module(test_module)