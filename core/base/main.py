import os
import sys
from pathlib import Path
from functools import wraps

# set up path for project root directory
sys.path.append(str(Path(__file__).parent.parent.parent))
fn = sys.argv[0]

# native logger import section start
try:
    from core.loggers.native_logger import init_logger
except ImportError as ie:
    exit(f'failed to import native logger module:: {fn} :: {ie}')
# native logger import section end

# helpers import section start
try:
    from helpers.snippets import (read_json_config, get_unique_identifier)
except ImportError as ie:
    exit(f'failed to import required snippets:: {fn} :: {ie}')
# helpers import section end

# aiopg import section start
try:
    from core.db.db import DB as AsyncDB
except ImportError as ie:
    exit(f'failed to import aiopg module:: {fn} :: {ie}')
# aiopg import section end


class BaseMain:
    service_name = 'demo_service'

    def __init__(self):
        # set up path entrypoint of project catalog
        self.project_dir = str(Path(__file__).parent.parent.parent)
        # task identifier
        self.task_id = get_unique_identifier()

        # general config reading section
        self.general_config_path = os.path.join(self.project_dir, 'settings', 'general.json')
        if not os.path.isfile(self.general_config_path):
            exit(f'configuration file not found on path:: {self.general_config_path}')

        self.config = read_json_config(self.general_config_path)
        if isinstance(self.config, Exception):
            exit(f'config reading finished with exception:: {self.config}')

        # native logger init section start
        self.logger_config = self.config.get(self.service_name, {}).get('logger', self.config.get('logger'))
        if not self.logger_config:
            exit(f'logger configuration not found :: {self.service_name}')

        # set up manually catalog that would store logs data
        self.logs_dir_path = os.path.join(self.project_dir, 'logs')
        self.logger = init_logger(name=self.service_name, log_dir_path=self.logs_dir_path, **self.logger_config)
        # native logger init section end

        self.db_config_path = os.path.join(self.project_dir, 'settings', 'db.json')
        if not os.path.isfile(self.db_config_path):
            self.logger.critical(f'config file not found:: {self.db_config_path} :: {self.service_name}')
            exit()

        self.db_full_config = read_json_config(self.db_config_path)
        if isinstance(self.db_full_config, Exception):
            self.logger.critical(f'config file reading finished with errors:: {self.db_full_config} :: {self.service_name}')
            exit()

        # could be multiple configurations for multiple services or just one general
        self.db_config = self.db_full_config.get(self.service_name, self.db_full_config)

        # aiopg init section start
        self.db = AsyncDB(connection_link='postgresql://{user}:{pwd}@{host}:{port}/{db}'.format(**self.db_config), logger=self.logger)
        # aiopg init section end

        # proxies init section start
        self.proxies_path = os.path.join(self.project_dir, 'settings', 'proxies.json')
        if not os.path.isfile(self.proxies_path):
            self.logger.critical(f'proxies configuration not found on path:: {self.proxies_path}')
            exit()

        self.proxies_config = read_json_config(self.proxies_path)
        if isinstance(self.proxies_config, Exception):
            self.logger.critical(f'proxies config file reading finished with errors:: {self.proxies_config}')
            exit()

        self.proxies_settings = self.proxies_config.get(self.service_name, self.proxies_config.get('default_group'))
        # proxies init section end

        self.agents_list = []
        # desktop agents init section start
        self.agents_path = os.path.join(self.project_dir, 'resources', 'ua', 'agents.json')
        if not os.path.isfile(self.agents_path):
            self.logger.critical(f'agents not found on path:: {self.agents_path}')
            exit()

        self.agents_list = read_json_config(self.agents_path)
        if isinstance(self.agents_list, Exception):
            self.logger.critical(f'desktop agents reading finished with errors:: {self.agents_list}')
            exit()
        self.agents_list.extend(self.agents_list)
        # desktop agents init section end

    def exception(method):
        @wraps(method)
        def wrapper(self, *args, **kwargs):
            if method.__annotations__.get('return') is not None:
                if callable(method.__annotations__['return']):
                    result = method.__annotations__['return']()
                else:
                    result = method.__annotations__['return']
            else:
                result = None

            try:
                result = method(self, *args, **kwargs)
            except Exception as e:
                self.logger.exception(f'exception in "{method.__name__}" => {e}')
                result = e if result is None else result
            finally:
                return result

        return wrapper