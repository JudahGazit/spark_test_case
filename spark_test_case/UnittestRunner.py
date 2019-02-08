import os
import re
import sys
import time
import traceback
import unittest

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

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

    def __get_files_in_dir(self, root_dir, ignore_dirs=['\\.git', '\\.idea'], extensions=['py', 'json']):
        files = []
        file_extensions_pattern = '.*\\.({})$'.format('|'.join(extensions))
        ignore_dirs_pattern = '/({})/'.format('|'.join(ignore_dirs))
        for root, directories, filenames in os.walk(root_dir):
            if re.search(ignore_dirs_pattern, root) is None:
                for filename in filenames:
                    if re.match(file_extensions_pattern, filename) is not None:
                        files.append(os.path.join(root, filename))
        return files

    def __get_dir_modified_time(self, root_dir):
        files = self.__get_files_in_dir(os.getcwd())
        files_modified_time = [os.path.getmtime(filename) for filename in files]
        last_modified_time = max(files_modified_time)
        return last_modified_time

    def __try_rerun_tests(self, test_module):
        try:
            self.__reload_all_modules()
            self._run_tests_in_module(test_module)
        except Exception:
            traceback.print_exc()

    def run(self, test_module, wait_time=3):
        max_modified_time = 0
        while True:
            last_modified_time = self.__get_dir_modified_time(os.getcwd())
            if last_modified_time > max_modified_time:
                max_modified_time = last_modified_time
                self.__try_rerun_tests(test_module)
            time.sleep(wait_time)
