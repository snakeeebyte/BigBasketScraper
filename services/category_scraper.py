import random
from time import sleep

import requests

class CategoryScraper:
    """
    Scraper class for extracting categories from BigBasket.
    Handles session creation with proxies and user agents,
    category tree parsing, and category data retrieval.
    """

    def __init__(self,base_url:str, base_headers:dict, base_proxy:list, base_user_agents:list, logger):
        """
        Initialize category scraper with base configuration.

        :param base_url: Base URL of the website
        :param base_headers: Default HTTP headers
        :param base_proxy: List of proxies for requests
        :param base_user_agents: List of user-agent headers
        :param logger: Logger instance for tracking execution
        """

        self.logger = logger

        self.base_url = base_url
        self.base_headers = base_headers
        self.base_proxy = base_proxy
        self.base_user_agents = base_user_agents

        self.max_retries = 5

    def get_random_proxy(self):
        """
        Select a random proxy from the available pool.

        :return: Proxy string
        """

        return random.choice(self.base_proxy)

    def get_random_headers(self):
        """
        Generate request headers with a random user-agent.

        :return: Updated HTTP headers
        """

        headers = self.base_headers.copy()
        user_agent_with_chh = random.choice(self.base_user_agents)

        headers.update(user_agent_with_chh)

        return headers

    def initialize_session(self):
        """
        Create and validate a requests session with retries.

        :return: requests.Session object on success, False on failure
        """

        for attempt in range(self.max_retries):
            try:
                self.logger.info(f"Start creating session... Attempt:: {attempt}")
                session = requests.Session()
                session.headers.update(self.get_random_headers())

                proxy = self.get_random_proxy()
                session.proxies.update({
                    'http':proxy,
                    'https':proxy,
                })

                response = session.get(self.base_url, timeout=15)

                if response.status_code != 200:
                    raise Exception(f"Not available status code {response.status_code}")

                return session

            except Exception as e:
                self.logger.error(f"Unexpected error on creating session {e}")
                continue
        else:
            self.logger.error("Max retries expected...")
            return False

    def parse_categories(self, data: dict) -> list[dict]:
        """
        Parse category JSON tree into a flat list of leaf categories.

        :param data: Category tree JSON response
        :return: List of leaf categories with keys: type, slug, category_name, id
        """

        results = []

        def walk(categories):
            for cat in categories:
                if not cat.get("children"):
                    results.append({
                        "type": cat.get("type"),
                        "slug": cat.get("slug"),
                        "category_name": cat.get("name"),
                        "id": cat.get("id")
                    })
                else:
                    walk(cat["children"])

        walk(data.get("categories", []))
        return results

    def get_categories(self):
        """
        Retrieve all categories from the API endpoint.

        Workflow:
        - Initialize a session
        - Request the category-tree endpoint
        - Parse categories into a flat list

        :return: List of categories or False if failed
        """

        session = self.initialize_session()
        if not session:
            self.logger.error("Cannot get session...")
            return False

        for attempt in range(self.max_retries):
            try:
                self.logger.info(f"Try to get categories... Attempt:: {attempt} ")

                url = self.base_url + 'ui-svc/v1/category-tree'

                response = session.get(url, timeout=15)

                if response.status_code != 200:
                    raise Exception(f'Not allowed status code:: {response.status_code}')

                data = response.json()

                result = self.parse_categories(data)

                if not result:
                    raise Exception(f'Cannot parse categories...')

                del response, data

                return result

            except Exception as e:
                self.logger.error(f"Unexpected error on getting categories:: {e}")
                sleep(random.uniform(1.5, 3.5))
