import sys
from datetime import datetime
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

try:
    from core.base.main import BaseMain
except ImportError as ie:
    exit(f"Cannot import BaseMain class:: {ie}")

try:
    from services.category_scraper import CategoryScraper
except ImportError as ie:
    exit(f"Cannot import CategoryScraper:: {ie}")

try:
    from services.products_scraper import ProductsScraper
except ImportError as ie:
    exit(f"Cannot import ProductScraper:: {ie}")

try:
    from helpers.snippets import write_json_config
except ImportError as ie:
    exit(f"Cannot import snippets:: {ie}")

class MainManager(BaseMain):
    """
    Manager class for orchestrating BigBasket scraping pipeline.
    Combines category scraping, product scraping, and result saving.
    """

    service_name = 'bigbasket_scraping_manager'

    def __init__(self):
        """
        Initialize scraping manager with base configuration,
        category scraper, and product scraper.
        """

        super().__init__()

        self.base_url = 'https://www.bigbasket.com/'
        self.base_headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en,en-US;q=0.9',
            'cache-control': 'max-age=0',
            'priority': 'u=0, i',
            'upgrade-insecure-requests': '1',
        }

        proxy_pool = self.proxies_config.get(self.service_name).get('pool', [])

        self.category_scraper = CategoryScraper(
            base_url=self.base_url,
            base_headers=self.base_headers,
            base_user_agents=self.agents_list,
            base_proxy= proxy_pool,
            logger=self.logger
        )

        self.product_scraper = ProductsScraper(
            base_url=self.base_url,
            base_headers=self.base_headers,
            base_user_agents=self.agents_list,
            base_proxy=proxy_pool,
            logger=self.logger,
            th_num=20,
            db=self.db,
            save_result_limit=250
        )

        self.start_time = datetime.now()

    def run(self):
        """
        Run the full scraping pipeline:
        - Scrape categories
        - Scrape products
        - Save results to database and JSON

        :param self: instance of MainManager
        :return: True on success, False on failure
        """

        self.logger.info("Starting pipeline")

        categories_to_scrape = self.category_scraper.get_categories()
        if not categories_to_scrape:
            self.logger.error(f"Cannot get categories... Exiting...")
            return False

        self.logger.info(f"Found {len(categories_to_scrape)} categories. Starting product scraper")

        self.product_scraper.run(categories_to_scrape)

        result = self.db.get_results()
        self.logger.info(f"Scrapped {len(result)} items")

        is_saved = write_json_config(result, "output_data.json")
        if is_saved is not True:
            self.logger.error(f"Unexpected error on saving results {is_saved}")
            return False

        self.logger.info(f"Pipeline execution completed. Time of execution:: {datetime.now() - self.start_time}")


if __name__ == '__main__':
    ex = MainManager()
    ex.run()
