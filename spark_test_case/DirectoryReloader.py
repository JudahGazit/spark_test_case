import sys


class DirectoryReloader:
    def reload_directory(self, root_directory):
        modules = sys.modules.values()
        for module in modules:
            if module is not None:
                module_is_main = module.__name__ == '__main__'
                module_in_working_directory = hasattr(module, '__file__') and root_directory in module.__file__
                if not module_is_main and module_in_working_directory:
                    reload(module)
