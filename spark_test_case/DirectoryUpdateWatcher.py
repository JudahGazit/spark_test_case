import os
import re

from spark_test_case import consts


class DirectoryUpdateWatcher:
    def __get_files_in_dir(self, root_dir, ignore_dirs=consts.IGNORE_DIRS, extensions=consts.EXTENSIONS_TO_WATCH):
        files = []
        file_extensions_pattern = '.*\\.({})$'.format('|'.join(extensions))
        ignore_dirs_pattern = '/({})/'.format('|'.join(ignore_dirs))
        for root, directories, filenames in os.walk(root_dir):
            if re.search(ignore_dirs_pattern, root) is None:
                for filename in filenames:
                    if re.match(file_extensions_pattern, filename) is not None:
                        files.append(os.path.join(root, filename))
        return files

    def get_dir_modified_time(self, root_dir):
        files = self.__get_files_in_dir(os.getcwd())
        files_modified_time = [os.path.getmtime(filename) for filename in files]
        last_modified_time = max(files_modified_time)
        return last_modified_time
