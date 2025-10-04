import sys
from logging import exception
from pathlib import Path
import random
from time import sleep

import requests

sys.path.append(str(Path(__file__).parent.parent))

try:
    from core.task_destribution.thread_task_destribution import ThreadingBase
except ImportError as ie:
    exit(f"Cannot import ThreadingBase:: {ie}")

class ProductsScraper(ThreadingBase):
    """
    Scraper class for extracting products from BigBasket.
    Uses multi-threaded execution, session handling with proxies,
    and stores results into a database queue for batch saving.
    """

    def __init__(self,base_url:str, base_headers:dict, base_proxy:list, base_user_agents:list, logger, th_num: int, db, save_result_limit:int):
        """
        Initialize products scraper with base configuration.

        :param base_url: Base URL of BigBasket
        :param base_headers: Default HTTP headers
        :param base_proxy: List of proxies for requests
        :param base_user_agents: List of user-agent headers
        :param logger: Logger instance for tracking execution
        :param th_num: Number of threads to use for scraping
        :param db: Database handler for saving results
        :param save_result_limit: Maximum batch size for saving results
        """

        super().__init__(logger=logger, th_num=th_num)

        self.base_url = base_url
        self.api_base_url = f"{self.base_url}listing-svc/v2/products"
        self.base_headers = base_headers
        self.base_proxy = base_proxy
        self.base_user_agents = base_user_agents
        self.db = db
        self.save_result_limit = save_result_limit

        self.max_retries = 5
        self.on_conflict_stmt = None

    def get_random_proxy(self):
        """
        Select a random proxy from the proxy pool.

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
                    'http': proxy,
                    'https': proxy,
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

    def scraping_consumer(self, method):
        """
        Single thread worker for processing product tasks.

        :param method: Function to process each task
        :return: False if session cannot be created, otherwise None
        """

        session = self.initialize_session()
        if not session:
            self.logger.info("Cannot create session... Exiting")
            return False

        try:
            while self.tasks.qsize() != 0 and not self.is_stopped:
                try:
                    self.block.acquire()
                    task = self.tasks.get(timeout=0.1)
                    method(task, session)
                except Empty:
                    self.logger.info(f'task queue is empty')
                    break
                except Exception as e:
                    self.logger.error(f'consumer exception occurred:: {e}')
                    break
                finally:
                    self.block.release()

        finally:
            session.close()
        self.logger.info(f'consumer finished procession')

    @ThreadingBase.exception
    def parse_product_data(self, data):
        """
        Extract product details from API response and store into results queue.

        :param data: JSON response from product listing endpoint
        :return: Number of pages for pagination or False if parsing fails
        """

        product_info = data.get('tabs', [{}])[0].get('product_info', False)
        if not product_info:
            return False

        count_of_pages = product_info.get('number_of_pages', 1)
        products = product_info.get('products')

        for product in products:
            try:
                product_id = int(product.get('id', 0))

                if not product_id:
                    continue

                name = product.get('desc', '')
                brand = product.get('brand', {}).get('name', '')
                product_url = product.get('absolute_url', '')
                unit = product.get('unit', '')
                quantity_label = product.get('magnitude', '')

                # Extract images array
                images = []
                for image in product.get('images', []):
                    if image.get('l'):  # Get large image URL
                        images.append(image['l'])

                # Extract pricing information
                pricing = product.get('pricing', {})
                discount_info = pricing.get('discount', {})

                price_mrp = None
                price_sp = None
                discount_percent = None

                if discount_info.get('mrp'):
                    price_mrp = float(discount_info['mrp']) / 100  # Convert from paise to rupees

                if discount_info.get('prim_price', {}).get('sp'):
                    price_sp = float(discount_info['prim_price']['sp']) / 100

                # Calculate discount percentage if both prices available
                if price_mrp and price_sp and price_mrp > 0:
                    discount_percent = round(((price_mrp - price_sp) / price_mrp) * 100, 2)

                # Extract availability info
                availability = product.get('availability', {})
                is_best_value = product.get('is_best_value', False)
                available_quantity = product.get('sku_max_quantity', 0)
                availability_code = availability.get('avail_status', '')

                # Extract category information
                category = product.get('category', {})
                category_main = category.get('tlc_name', '')  # Top Level Category
                category_mid = category.get('mlc_name', '')  # Mid Level Category
                category_leaf = category.get('llc_name', '')  # Leaf Level Category

                # Extract created/updated on website
                parent_info = product.get('parent_info')
                created_on_website = parent_info.get('created_on', None)
                updated_on_website = parent_info.get('updated_on', None)

                # Prepare the result dictionary
                result = {
                    'product_id': product_id,
                    'name': name,
                    'brand': brand,
                    'product_url': product_url,
                    'images': images,
                    'unit': unit,
                    'quantity_label': quantity_label,
                    'price_mrp': price_mrp,
                    'price_sp': price_sp,
                    'discount_percent': discount_percent,
                    'is_best_value': is_best_value,
                    'available_quantity': available_quantity,
                    'availability_code': availability_code,
                    'category_main': category_main,
                    'category_mid': category_mid,
                    'category_leaf': category_leaf,
                    'created_at_on_web_site': created_on_website,
                    'updated_at_on_web_site': updated_on_website
                }

                self.results.put(result)

                del result
            except Exception as e:
                self.logger.error(f"Unexpected error on parsing product {e}")
                continue

        return count_of_pages

    @ThreadingBase.progress_logger
    @ThreadingBase.exception
    def scraping_executor(self, task: dict, session: requests.Session):
        """
        Execute product scraping for a single category.

        :param task: Dictionary containing category info (type, slug, id, name)
        :param session: Active requests.Session object
        :return: True on successful processing
        """

        type_ = task.get("type")
        slug = task.get("slug")
        category_name = task.get("category_name")
        category_id = task.get('id')

        self.logger.info(f"Starting processing:: {category_id}::{category_name}")

        page = 1
        last_page = 2
        params = {
            'type': type_,
            'slug': slug,
        }

        while page <= last_page:
            for attempt in range(self.max_retries):
                try:
                    params['page'] = f"{page}"

                    response = session.get(self.api_base_url, params=params, timeout=15)
                    if response.status_code == 204:
                        break

                    if response.status_code != 200:
                        raise Exception(f"Not allowed status code:: {response.status_code}")


                    data = response.json()

                    last_page = self.parse_product_data(data)
                    del data

                    if not isinstance(last_page, int):
                        raise Exception(f"Cannot parse count of pages")

                    self.logger.info(f"Successfully parsed {page}/{last_page} for category {category_id}::{category_name}")
                    break
                except Exception as e:
                    self.logger.error(f"Attempt:: {attempt}:: Unexpected error on {category_id}::{category_name}::{page}:  {e}")
                    sleep(random.uniform(1.5, 3.5))
            else:
                self.logger.error(f"Max retries occupied for {category_id}::{category_name}::{page}")

            page += 1

        self.logger.info(f"Successfully parsed category {category_id}::{category_name}")
        return True

    @ThreadingBase.exception
    def saving_executor(self, results):
        """
        Save scraped product results into the database with conflict handling.

        :param results: List of product dictionaries to save
        :return: True if saved successfully, False otherwise
        """

        unique_ids = set()
        to_save = []

        for result in results:
            product_id = result.get('product_id')
            if product_id not in unique_ids:
                unique_ids.add(product_id)
                to_save.append(result)

        del results

        if self.on_conflict_stmt is None:
            keys = list(to_save[0].keys())
            keys.remove('product_id')

            self.on_conflict_stmt = self.db.prepare_statement(
                update_keys=keys,
                conflict_key="product_id",
                updated_at = "CURRENT_TIMESTAMP"
            )
        result = self.db.save_products(to_save, self.on_conflict_stmt)
        del to_save
        return result
