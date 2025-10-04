# BigBasket Scraper (Study Project)

> ⚠️ **Note:** This project was created for educational purposes for the SnakeByte YouTube channel. It is not intended for production or commercial use.  

BigBasket Scraper is a Python-based scraping pipeline designed to extract category and product data from [BigBasket](https://www.bigbasket.com/). The project uses multi-threaded scraping with proxies, user-agent rotation, and batch saving of results to a database or JSON file.

---

## Features

- **Category Scraper:** Recursively extracts leaf categories (categories with no children).
- **Product Scraper:** Scrapes products with detailed information including:
  - Product ID, Name, Brand, URL  
  - Pricing (MRP, Selling Price, Discount %)  
  - Images  
  - Availability and stock quantity  
  - Category hierarchy (Main / Mid / Leaf)  
  - Created and updated timestamps
- **Multi-threading:** Uses `ThreadingBase` for task distribution and batch saving.
- **Proxies and User-Agent Rotation:** Random selection for each session/request.
- **Database Integration:** Saves results to PostgreSQL via prepared statements.
- **JSON Output:** Can save scraping results to `output_data.json`.

---

## Project Structure
```
│ pyproject.toml
│ poetry.lock
│ test.py
│
├───.idea
│ ├───inspectionProfiles
│ └───dataSources
│
├───sql
│ models.sql
│
├───core
│ ├───base
│ │ main.py
│ ├───db
│ │ db.py
│ ├───loggers
│ │ native_logger.py
│ └───task_destribution
│ thread_task_destribution.py
│
├───helpers
│ snippets.py
│
├───resources
│ └───ua
│ agents.json
│
├───services
│ category_scraper.py
│ products_scraper.py
│
├───managers
│ bigbasket_manager.py
│ output_data.json
│
├───settings
| general.json
| db.json
| proxies.json
└
```

---

## Installation

1. Clone the repository:

```bash
git clone <repo-url>
cd BigBasletScraper
```

2. Install dependencies:

```bash
poetry install
```
3. Ensure your PostgreSQL database is configured if you want to use the DB integration.

## Configuration

- **Proxies and User-Agents**:

  Configure proxies in settings/proxies.json and user-agents in resources/ua/agents.json.

- **Database**:

  Configure database credentials in settings/db.json.

## Usage

Run the full scraping pipeline using the MainManager:

```bash
poetry run python managers/bigbasket_manager.py
```

Steps performed by the pipeline:

1) Scrapes all categories from BigBasket.

2) Scrapes all products in each category using multi-threading.

3) Saves results to the database and `output_data.json`.

## Example Output (JSON)

```json
{
    "product_id": 12345,
    "name": "Organic Milk",
    "brand": "Amul",
    "product_url": "https://www.bigbasket.com/p/12345",
    "images": ["https://.../image1.jpg", "https://.../image2.jpg"],
    "unit": "1 L",
    "quantity_label": "1 Liter",
    "price_mrp": 60.0,
    "price_sp": 55.0,
    "discount_percent": 8.33,
    "is_best_value": false,
    "available_quantity": 10,
    "availability_code": "available",
    "category_main": "Dairy",
    "category_mid": "Milk & Cream",
    "category_leaf": "Organic Milk",
    "created_at_on_web_site": "2025-10-01T10:00:00",
    "updated_at_on_web_site": "2025-10-02T15:00:00"
}
```

## License

MIT License
