# Overview

As a software engineer focused on building practical data tools, I wanted to create a system that bridges the gap between a live ERP system and actionable purchasing intelligence. Many businesses have data trapped in their ERP that is difficult to analyze quickly — this project is my attempt to unlock that data and turn it into something useful.

This software connects to an ERPNext instance via session-based API authentication, pulls item and stock data through paginated API calls, and imports sales data from exported reports. All data is cleaned and loaded into a PostgreSQL relational database. From there, the software runs SQL queries to generate a 3-month moving average sales forecast, calculates reorder points, and produces a demand analysis showing which items need to be ordered across multiple planning horizons. Results are exported as timestamped CSV files for use in purchasing decisions.

The pipeline also pushes a filtered demand snapshot to a Google Firestore cloud database, which feeds a live hosted dashboard built with vanilla JavaScript and deployed via Firebase Hosting. The dashboard shows which brands need ordering, the total order value per brand, and SKU-level demand broken down by country.

To use the program, configure your `.env` file with your database and ERP credentials, place your sales report CSV in the project directory, and run `python main.py`. Functions can be enabled or disabled from the menu depending on what you need to update.

My purpose for writing this software was to solve a real business problem — a motorcycle parts retailer operating across two countries needed a faster, data-driven way to make purchasing decisions rather than relying on gut feel or manual spreadsheet analysis.

[Software Demo Video](https://youtu.be/ykZ_EO7QQIY)

---

# Relational Database

This project uses PostgreSQL as the relational database management system.

The database contains three tables:

- **items** — stores product master data pulled from the ERP API, including item code (primary key), item name, and brand
- **stock** — stores current inventory levels per item per warehouse, pulled from the ERPNext Bin DocType. Uses a composite primary key of item code and warehouse. Contains actual quantity, reserved quantity, valuation rate, and stock value
- **sales** — stores historical sales transactions imported from an ERPNext itemwise sales report. Contains invoice details, customer information, quantities, pricing, and VAT data. The sales table is fully replaced on each import to reflect the latest data

The `stock` and `sales` tables both reference `item_code` which links back to the `items` table, enabling joins across all three tables for analysis.

---

# Cloud Database

This project uses Google Firestore (part of Firebase) as the cloud database.

After generating the demand analysis locally, the pipeline pushes a filtered snapshot to Firestore. Only items with a combined 2-month demand of 1 or more are included, keeping the dataset within free tier limits.

Firestore contains two related collections:

- **demand_items** — one document per SKU, keyed by item code. Contains brand code, item name, valuation rate, UAE 2-month demand, Qatar 2-month demand, and total order value. The `brand_code` field links each item back to its parent brand document
- **brands** — one document per brand, keyed by brand code. Contains the brand name and total order value, derived by summing all SKU order values within that brand

A `meta` collection stores a `last_updated` timestamp that is written on every push, displayed in the dashboard header so the viewer always knows when the data was last refreshed.

The live dashboard is hosted on Firebase Hosting and reads from Firestore in real time. It shows brands ranked by total order value, with an expandable panel per brand showing SKU-level demand quantities for UAE and Qatar alongside unit cost and total order value.

---

# Development Environment

- **VS Code** — primary code editor
- **pgAdmin** — PostgreSQL database management and query testing
- **PostgreSQL** — relational database
- **ERPNext / Frappe** — source ERP system accessed via REST API
- **Firebase / Firestore** — cloud database and hosting platform

The software is written in Python 3.14. Libraries used:

- `pandas` — data manipulation, CSV handling, and DataFrame operations
- `sqlalchemy` — database engine and connection management
- `psycopg2` — PostgreSQL driver
- `requests` — HTTP session management and ERP API calls
- `python-dotenv` — environment variable management for secure credential storage
- `firebase-admin` — Firestore SDK for pushing data to the cloud database
- `re` — regular expressions for column name cleaning
- `math` — ceiling rounding for forecast quantities
- `datetime` — timestamping output files

---

# Useful Websites

- [ERPNext REST API Documentation](https://frappeframework.com/docs/user/en/api/rest)
- [pandas Documentation](https://pandas.pydata.org/docs/)
- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [python-dotenv Documentation](https://pypi.org/project/python-dotenv/)
- [Firebase Admin Python SDK](https://firebase.google.com/docs/admin/setup)
- [Firestore Documentation](https://firebase.google.com/docs/firestore)
- [Firebase Hosting Documentation](https://firebase.google.com/docs/hosting)

---

# Future Work

- Incorporate a full year of sales data to implement Holt-Winters seasonal forecasting for more accurate demand prediction during season transitions
- Add upsert logic for the items table so existing records are updated rather than requiring a full table replacement
- Integrate purchase order data from ERPNext to show items already on order and subtract from demand calculations
- Add automated scheduling so the pipeline runs on a defined cadence without manual execution
- Expand the dashboard to support filtering by brand or sorting by demand quantity