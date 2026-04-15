import os
from datetime import datetime
import math
import pandas as pd
import requests
import re
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import firebase_admin
from firebase_admin import credentials, firestore

load_dotenv()

###########################################################
###################### CONFIGURATION #####################
###########################################################

# Weighted Moving Average weights — must sum to 1.0
# WMA_RECENT_WEIGHT applies to the last 3 months of sales
# WMA_OLDER_WEIGHT applies to months 4-6
WMA_RECENT_WEIGHT = 0.65
WMA_OLDER_WEIGHT  = 0.35

###########################################################
##################### CONNECTIONS ########################
###########################################################

# Amazon credentials — currently unused
AMAZON_ID              = os.getenv("AMAZON_ID")
AMAZON_SECRET          = os.getenv("AMAZON_SECRET")
AMAZON_BASE_URL        = os.getenv("AMAZON_BASE_URL")
AMAZON_BASE_URL_SANDBOX= os.getenv("AMAZON_BASE_URL_SANDBOX")
AMAZON_MARKETPLACE_ID  = os.getenv("AMAZON_MARKETPLACE_ID")

# PostgreSQL credentials
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST     = os.getenv("DB_HOST")
DB_PORT     = os.getenv("DB_PORT")
DB_NAME     = os.getenv("DB_NAME")

# ERPNext credentials
ERP_BASE_URL = os.getenv("ERP_BASE_URL")
ERP_USER     = os.getenv("ERP_USER")
ERP_PASSWORD = os.getenv("ERP_PASSWORD")

# ERPNext session login
session = requests.Session()
login = session.post(f"{ERP_BASE_URL}/api/method/login", data={
    "usr": ERP_USER,
    "pwd": ERP_PASSWORD
})
print(f"Logged in as {login.json()['full_name']}")

# Firebase connection
cred = credentials.Certificate("firebaseJWT.json")
firebase_admin.initialize_app(cred)
db_firebase = firestore.client()
print("Firestore Connected Successfully")

# PostgreSQL engine
engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

###########################################################
#################### ERP API FUNCTIONS ###################
###########################################################

def fetch_all_items():
    """
    Pulls all items from ERPNext via paginated API calls.
    Fetches item_code, item_name, and brand.
    Saves to full_item_list.csv and returns a DataFrame.
    """
    all_items = []
    start     = 0
    batch     = 500

    while True:
        print(f"Fetching items {start} to {start + batch}...")
        response = session.get(f"{ERP_BASE_URL}/api/resource/Item", params={
            "limit_page_length": batch,
            "limit_start"      : start,
            "fields"           : '["item_code", "item_name", "brand"]'
        }, timeout=30)

        data = response.json().get("data", [])
        if not data:
            break

        all_items.extend(data)
        start += batch

    if not all_items:
        print("No items fetched. Check API connection.")
        return

    print(f"Total items fetched: {len(all_items)}")
    df = pd.DataFrame(all_items)
    df.to_csv("full_item_list.csv", index=False)
    print("Saved to full_item_list.csv")
    return df


def fetch_stock():
    """
    Pulls all stock bin records from ERPNext via paginated API calls.
    Fetches item_code, warehouse, actual_qty, reserved_qty,
    valuation_rate, and stock_value.
    Saves to raw_stock.csv and returns a DataFrame.
    """
    stock = []
    start = 0
    batch = 500

    while True:
        print(f"Fetching stock {start} to {start + batch}...")
        response = session.get(f"{ERP_BASE_URL}/api/resource/Bin", params={
            "limit_page_length": batch,
            "limit_start"      : start,
            "fields"           : '["item_code", "warehouse", "actual_qty", "reserved_qty", "valuation_rate", "stock_value"]'
        }, timeout=30)

        data = response.json().get("data", [])
        if not data:
            break

        stock.extend(data)
        start += batch

    if not stock:
        print("Stock levels could not be fetched. Check API connection.")
        return

    print(f"Total stock lines fetched: {len(stock)}")
    df = pd.DataFrame(stock)
    df.to_csv("raw_stock.csv", index=False)
    print("Saved to raw_stock.csv")
    return df


###########################################################
################# DATABASE FUNCTIONS #####################
###########################################################

def data_to_db(df, table_name, if_exists="replace"):
    """
    Uploads a DataFrame to PostgreSQL.
    Wipes the existing table and replaces it with fresh data.
    Prints row count before and after.
    """
    count = pd.read_sql(
        text(f"SELECT COUNT(*) FROM {table_name}"), engine
    ).iloc[0, 0]
    print(f"Wiping {count} rows from '{table_name}'...")

    df.to_sql(table_name, engine, if_exists=if_exists, index=False)
    print(f"Uploaded {len(df)} rows to '{table_name}'")


###########################################################
################# FIREBASE FUNCTIONS #####################
###########################################################

def push_to_firebase(df, collection_name, document_id):
    """
    Pushes a DataFrame to a Firestore collection.
    Deletes all existing documents in the collection first
    to ensure a clean, accurate snapshot every push.
    Batches writes in groups of 500 (Firestore limit).
    """
    # Delete existing documents
    docs         = db_firebase.collection(collection_name).stream()
    delete_batch = db_firebase.batch()
    count        = 0

    for doc in docs:
        delete_batch.delete(doc.reference)
        count += 1
        if count % 500 == 0:
            delete_batch.commit()
            delete_batch = db_firebase.batch()

    delete_batch.commit()
    print(f"Cleared {count} documents from '{collection_name}'")

    # Push fresh data
    records = df.to_dict(orient="records")
    batch   = db_firebase.batch()

    for i, row in enumerate(records):
        ref = db_firebase.collection(collection_name).document(row[document_id])
        batch.set(ref, row)
        if (i + 1) % 500 == 0:
            batch.commit()
            batch = db_firebase.batch()

    batch.commit()
    print(f"Pushed {len(records)} documents to '{collection_name}'")


###########################################################
############# SALES IMPORT AND CLEANING ##################
###########################################################

def read_clean_sales():
    """
    Reads the raw sales CSV exported from ERPNext.
    Cleans column names, parses posting dates,
    and saves the cleaned version to sales_data_cleaned.csv.
    Returns a DataFrame ready to upload to PostgreSQL.
    """
    df = pd.read_csv("sales_data.csv", encoding="utf-8-sig", usecols=[
        "Item Code",
        "Invoice",
        "Posting Date",
        "Customer Group",
        "Customer",
        "Mode Of Payment",
        "Company",
        "Stock Qty",
        "Rate",
        "Amount",
        "VAT 5% Rate",
        "VAT 5% Amount",
        "Total Tax",
        "Total Other Charges",
        "Total"
    ])

    # Normalize column names to snake_case
    df.columns = [
        re.sub(r'[^a-z0-9_]', '', col.strip().lower().replace(" ", "_"))
        for col in df.columns
    ]

    df['posting_date'] = pd.to_datetime(df['posting_date'], dayfirst=True)
    df.to_csv("sales_data_cleaned.csv", index=False)
    print("Saved to sales_data_cleaned.csv")
    return df


###########################################################
#################### QUERY FUNCTIONS #####################
###########################################################

def get_item_details():
    """
    Pulls item master data from PostgreSQL.
    Joins items table with stock to get average valuation rate per item.
    Returns a DataFrame with item_code, item_name, brand, valuation_rate.
    """
    df = pd.read_sql(text("""
        SELECT
            i.item_code,
            i.item_name,
            i.brand,
            COALESCE(v.valuation_rate, 0) AS valuation_rate
        FROM items i
        LEFT JOIN (
            SELECT
                item_code,
                AVG(valuation_rate) AS valuation_rate
            FROM stock
            WHERE valuation_rate > 0
            GROUP BY item_code
        ) v ON i.item_code = v.item_code
    """), engine)

    df.to_csv("full_item_list.csv", index=False)
    print("Saved to full_item_list.csv")
    return df


def actual_stock():
    """
    Pulls current stock levels from PostgreSQL separated by country.
    UAE stock excludes transit, warranty, Qatar, and non-sellable warehouses.
    Qatar stock includes DHS, DHW, and Qatar-specific warehouses.
    Saves to a timestamped CSV and returns a DataFrame.
    """
    df = pd.read_sql(text("""
        WITH uae AS (
            SELECT
                item_code,
                SUM(actual_qty) AS uae_total_qty
            FROM stock
            WHERE warehouse NOT IN (
                'MCS Distribution', 'Warranty-Handed Over', 'Warranty - Ready',
                'Returned Items', 'Warranty - Damaged Good', 'Sold Items',
                'Reservation JAW', 'Work In Progress', 'Finished Goods',
                'Goods In Transit', 'E-Commerce', 'Sold-To-Distribution',
                'Sold-To-KSA', 'Warranty - Missing Parts', 'JAW-TO-Distribution',
                'Reservation MCS', 'DXB-TO-AMAZON', 'OldTimer Cafe', 'DXB-TO-NOON',
                'DXB-TO-KSA', 'MCX-TO-QAT', 'DXB-TO-QAT', 'QA-TO-KSA', 'DHS',
                'DHS Distribution', 'GF Warehouse', 'DWH-6-F-4-C', 'Showroom',
                'Main Warehouse', 'Qatar Work In Progress', 'Qatar Reservation',
                'Qatar Returns', 'Qatar Receiving', 'All Warehouses - OTQAR'
            )
            AND warehouse NOT LIKE 'DHW%'
            GROUP BY item_code
        ),
        qat AS (
            SELECT
                item_code,
                SUM(actual_qty) AS qat_total_qty
            FROM stock
            WHERE warehouse IN (
                'DHS', 'DHS Distribution', 'DWH-6-F-4-C', 'DXB-TO-QAT',
                'Qatar Receiving', 'Qatar Reservation', 'QA-TO-KSA', 'QAT-TO-DXB'
            )
            OR warehouse LIKE 'DHW%'
            GROUP BY item_code
        )
        SELECT
            u.item_code,
            u.uae_total_qty,
            q.qat_total_qty
        FROM uae u
        LEFT JOIN qat q ON u.item_code = q.item_code
        ORDER BY u.item_code
    """), engine)

    df.to_csv(f"actual_stock_{datetime.now().date()}.csv", index=False)
    print(f"UPDATED: actual_stock_{datetime.now().date()}.csv")
    return df


###########################################################
################# FORECAST FUNCTIONS #####################
###########################################################

def _get_company_date_range(company_name):
    """
    Queries the actual MIN and MAX posting dates for a given company
    in the sales table. Returns (min_date, max_date, total_days).
    Used to determine whether to use a fixed 6-month split or a
    dynamic split based on available data range.
    """
    result = pd.read_sql(text("""
        SELECT
            MIN(posting_date) AS min_date,
            MAX(posting_date) AS max_date
        FROM sales
        WHERE company = :company
    """), engine, params={"company": company_name})

    min_date   = result['min_date'].iloc[0]
    max_date   = result['max_date'].iloc[0]
    total_days = (max_date - min_date).days

    return min_date, max_date, total_days


def _apply_wma(recent_total, older_total, recent_days, older_days):
    """
    Applies the weighted moving average formula to two pandas Series.

    WMA = (recent_avg * WMA_RECENT_WEIGHT) + (older_avg * WMA_OLDER_WEIGHT)

    Both averages are expressed as monthly rates (divided by months in each half).
    Uses 30.44 as average days per month for dynamic range calculations.

    Parameters:
        recent_total  — Series: total units sold in the recent half
        older_total   — Series: total units sold in the older half
        recent_days   — int: number of days in the recent half
        older_days    — int: number of days in the older half

    Returns a Series representing the weighted monthly forecast.
    """
    DAYS_PER_MONTH = 30.44

    recent_months = recent_days / DAYS_PER_MONTH
    older_months  = older_days  / DAYS_PER_MONTH

    recent_avg = recent_total.fillna(0) / recent_months
    older_avg  = older_total.fillna(0)  / older_months

    return (recent_avg * WMA_RECENT_WEIGHT) + (older_avg * WMA_OLDER_WEIGHT)


def get_uae_forecast():
    """
    Pulls UAE sales data from PostgreSQL and applies a weighted moving average.

    Date range logic:
    - If UAE has >= 180 days of sales data: split into fixed 3m recent / 3m older
    - If UAE has < 180 days of sales data: split available range in half dynamically

    Filters out items with fewer than 2 total units sold in the available range.
    Returns a DataFrame with uae_monthly_forecast per item.
    """
    UAE_COMPANY  = 'OldTimer Motorcycles and Spare Parts Trading LLC'
    MIN_DAYS     = 180

    # Get actual date range for UAE
    min_date, max_date, total_days = _get_company_date_range(UAE_COMPANY)
    print(f"UAE sales range: {min_date.date()} to {max_date.date()} ({total_days} days)")

    if total_days >= MIN_DAYS:
        # Full 6 months available — use fixed 3m/3m split
        recent_days = 90
        older_days  = 90
        print("UAE: using fixed 3m/3m split")

        df = pd.read_sql(text("""
            WITH recent AS (
                SELECT
                    item_code,
                    SUM(CASE WHEN customer_group IN ('Individual', 'VIP', 'Customers', 'Walk-In Customers')
                        THEN stock_qty ELSE 0 END) AS uae_showroom_recent,
                    SUM(CASE WHEN customer_group IN ('Market Place', 'Amazon', 'ruroc.ae')
                        THEN stock_qty ELSE 0 END) AS uae_ecommerce_recent,
                    SUM(CASE WHEN customer_group IN ('Dealers and Distribution', 'B2B')
                        THEN stock_qty ELSE 0 END) AS uae_distribution_recent
                FROM sales
                WHERE company = 'OldTimer Motorcycles and Spare Parts Trading LLC'
                AND posting_date >= NOW() - INTERVAL '3 months'
                GROUP BY item_code
            ),
            older AS (
                SELECT
                    item_code,
                    SUM(CASE WHEN customer_group IN ('Individual', 'VIP', 'Customers', 'Walk-In Customers')
                        THEN stock_qty ELSE 0 END) AS uae_showroom_older,
                    SUM(CASE WHEN customer_group IN ('Market Place', 'Amazon', 'ruroc.ae')
                        THEN stock_qty ELSE 0 END) AS uae_ecommerce_older,
                    SUM(CASE WHEN customer_group IN ('Dealers and Distribution', 'B2B')
                        THEN stock_qty ELSE 0 END) AS uae_distribution_older
                FROM sales
                WHERE company = 'OldTimer Motorcycles and Spare Parts Trading LLC'
                AND posting_date >= NOW() - INTERVAL '6 months'
                AND posting_date < NOW() - INTERVAL '3 months'
                GROUP BY item_code
            )
            SELECT
                COALESCE(r.item_code, o.item_code) AS item_code,
                r.uae_showroom_recent, r.uae_ecommerce_recent, r.uae_distribution_recent,
                o.uae_showroom_older,  o.uae_ecommerce_older,  o.uae_distribution_older
            FROM recent r
            FULL OUTER JOIN older o ON r.item_code = o.item_code
        """), engine)

    else:
        # Less than 6 months available — split range in half dynamically
        half_days   = total_days // 2
        recent_days = half_days
        older_days  = total_days - half_days
        split_date  = max_date - pd.Timedelta(days=half_days)
        print(f"UAE: using dynamic split — recent {recent_days} days / older {older_days} days")

        df = pd.read_sql(text("""
            WITH recent AS (
                SELECT
                    item_code,
                    SUM(CASE WHEN customer_group IN ('Individual', 'VIP', 'Customers', 'Walk-In Customers')
                        THEN stock_qty ELSE 0 END) AS uae_showroom_recent,
                    SUM(CASE WHEN customer_group IN ('Market Place', 'Amazon', 'ruroc.ae')
                        THEN stock_qty ELSE 0 END) AS uae_ecommerce_recent,
                    SUM(CASE WHEN customer_group IN ('Dealers and Distribution', 'B2B')
                        THEN stock_qty ELSE 0 END) AS uae_distribution_recent
                FROM sales
                WHERE company = 'OldTimer Motorcycles and Spare Parts Trading LLC'
                AND posting_date > :split_date
                GROUP BY item_code
            ),
            older AS (
                SELECT
                    item_code,
                    SUM(CASE WHEN customer_group IN ('Individual', 'VIP', 'Customers', 'Walk-In Customers')
                        THEN stock_qty ELSE 0 END) AS uae_showroom_older,
                    SUM(CASE WHEN customer_group IN ('Market Place', 'Amazon', 'ruroc.ae')
                        THEN stock_qty ELSE 0 END) AS uae_ecommerce_older,
                    SUM(CASE WHEN customer_group IN ('Dealers and Distribution', 'B2B')
                        THEN stock_qty ELSE 0 END) AS uae_distribution_older
                FROM sales
                WHERE company = 'OldTimer Motorcycles and Spare Parts Trading LLC'
                AND posting_date <= :split_date
                GROUP BY item_code
            )
            SELECT
                COALESCE(r.item_code, o.item_code) AS item_code,
                r.uae_showroom_recent, r.uae_ecommerce_recent, r.uae_distribution_recent,
                o.uae_showroom_older,  o.uae_ecommerce_older,  o.uae_distribution_older
            FROM recent r
            FULL OUTER JOIN older o ON r.item_code = o.item_code
        """), engine, params={"split_date": split_date})

    # Combine channels into totals per half
    df['uae_total_recent'] = (
        df['uae_showroom_recent'].fillna(0) +
        df['uae_ecommerce_recent'].fillna(0) +
        df['uae_distribution_recent'].fillna(0)
    )
    df['uae_total_older'] = (
        df['uae_showroom_older'].fillna(0) +
        df['uae_ecommerce_older'].fillna(0) +
        df['uae_distribution_older'].fillna(0)
    )

    # Filter: minimum 2 total units sold across both halves
    df['uae_total_sold'] = df['uae_total_recent'] + df['uae_total_older']
    df = df[df['uae_total_sold'] >= 2].copy()

    # Apply weighted moving average
    df['uae_monthly_forecast'] = _apply_wma(
        df['uae_total_recent'], df['uae_total_older'],
        recent_days, older_days
    )

    return df


def get_qat_forecast():
    """
    Pulls Qatar sales data from PostgreSQL and applies a weighted moving average.

    Date range logic:
    - If Qatar has >= 180 days of sales data: split into fixed 3m recent / 3m older
    - If Qatar has < 180 days of sales data: split available range in half dynamically

    Filters out items with fewer than 2 total units sold in the available range.
    Returns a DataFrame with qat_monthly_forecast per item.
    """
    QAT_COMPANY = 'Oldtimer Motorcycles Trading WLL'
    MIN_DAYS    = 180

    # Get actual date range for Qatar
    min_date, max_date, total_days = _get_company_date_range(QAT_COMPANY)
    print(f"Qatar sales range: {min_date.date()} to {max_date.date()} ({total_days} days)")

    if total_days >= MIN_DAYS:
        # Full 6 months available — use fixed 3m/3m split
        recent_days = 90
        older_days  = 90
        print("Qatar: using fixed 3m/3m split")

        df = pd.read_sql(text("""
            WITH recent AS (
                SELECT
                    item_code,
                    SUM(CASE WHEN customer_group IN ('Individual', 'VIP', 'Customers', 'Walk-In Customers')
                        THEN stock_qty ELSE 0 END) AS qat_showroom_recent,
                    SUM(CASE WHEN customer_group IN ('Market Place', 'Amazon', 'ruroc.ae')
                        THEN stock_qty ELSE 0 END) AS qat_ecommerce_recent,
                    SUM(CASE WHEN customer_group IN ('Dealers and Distribution', 'B2B')
                        THEN stock_qty ELSE 0 END) AS qat_distribution_recent
                FROM sales
                WHERE company = 'Oldtimer Motorcycles Trading WLL'
                AND posting_date >= NOW() - INTERVAL '3 months'
                GROUP BY item_code
            ),
            older AS (
                SELECT
                    item_code,
                    SUM(CASE WHEN customer_group IN ('Individual', 'VIP', 'Customers', 'Walk-In Customers')
                        THEN stock_qty ELSE 0 END) AS qat_showroom_older,
                    SUM(CASE WHEN customer_group IN ('Market Place', 'Amazon', 'ruroc.ae')
                        THEN stock_qty ELSE 0 END) AS qat_ecommerce_older,
                    SUM(CASE WHEN customer_group IN ('Dealers and Distribution', 'B2B')
                        THEN stock_qty ELSE 0 END) AS qat_distribution_older
                FROM sales
                WHERE company = 'Oldtimer Motorcycles Trading WLL'
                AND posting_date >= NOW() - INTERVAL '6 months'
                AND posting_date < NOW() - INTERVAL '3 months'
                GROUP BY item_code
            )
            SELECT
                COALESCE(r.item_code, o.item_code) AS item_code,
                r.qat_showroom_recent, r.qat_ecommerce_recent, r.qat_distribution_recent,
                o.qat_showroom_older,  o.qat_ecommerce_older,  o.qat_distribution_older
            FROM recent r
            FULL OUTER JOIN older o ON r.item_code = o.item_code
        """), engine)

    else:
        # Less than 6 months available — split range in half dynamically
        half_days   = total_days // 2
        recent_days = half_days
        older_days  = total_days - half_days
        split_date  = max_date - pd.Timedelta(days=half_days)
        print(f"Qatar: using dynamic split — recent {recent_days} days / older {older_days} days")

        df = pd.read_sql(text("""
            WITH recent AS (
                SELECT
                    item_code,
                    SUM(CASE WHEN customer_group IN ('Individual', 'VIP', 'Customers', 'Walk-In Customers')
                        THEN stock_qty ELSE 0 END) AS qat_showroom_recent,
                    SUM(CASE WHEN customer_group IN ('Market Place', 'Amazon', 'ruroc.ae')
                        THEN stock_qty ELSE 0 END) AS qat_ecommerce_recent,
                    SUM(CASE WHEN customer_group IN ('Dealers and Distribution', 'B2B')
                        THEN stock_qty ELSE 0 END) AS qat_distribution_recent
                FROM sales
                WHERE company = 'Oldtimer Motorcycles Trading WLL'
                AND posting_date > :split_date
                GROUP BY item_code
            ),
            older AS (
                SELECT
                    item_code,
                    SUM(CASE WHEN customer_group IN ('Individual', 'VIP', 'Customers', 'Walk-In Customers')
                        THEN stock_qty ELSE 0 END) AS qat_showroom_older,
                    SUM(CASE WHEN customer_group IN ('Market Place', 'Amazon', 'ruroc.ae')
                        THEN stock_qty ELSE 0 END) AS qat_ecommerce_older,
                    SUM(CASE WHEN customer_group IN ('Dealers and Distribution', 'B2B')
                        THEN stock_qty ELSE 0 END) AS qat_distribution_older
                FROM sales
                WHERE company = 'Oldtimer Motorcycles Trading WLL'
                AND posting_date <= :split_date
                GROUP BY item_code
            )
            SELECT
                COALESCE(r.item_code, o.item_code) AS item_code,
                r.qat_showroom_recent, r.qat_ecommerce_recent, r.qat_distribution_recent,
                o.qat_showroom_older,  o.qat_ecommerce_older,  o.qat_distribution_older
            FROM recent r
            FULL OUTER JOIN older o ON r.item_code = o.item_code
        """), engine, params={"split_date": split_date})

    # Combine channels into totals per half
    df['qat_total_recent'] = (
        df['qat_showroom_recent'].fillna(0) +
        df['qat_ecommerce_recent'].fillna(0) +
        df['qat_distribution_recent'].fillna(0)
    )
    df['qat_total_older'] = (
        df['qat_showroom_older'].fillna(0) +
        df['qat_ecommerce_older'].fillna(0) +
        df['qat_distribution_older'].fillna(0)
    )

    # Filter: minimum 2 total units sold across both halves
    df['qat_total_sold'] = df['qat_total_recent'] + df['qat_total_older']
    df = df[df['qat_total_sold'] >= 2].copy()

    # Apply weighted moving average
    df['qat_monthly_forecast'] = _apply_wma(
        df['qat_total_recent'], df['qat_total_older'],
        recent_days, older_days
    )

    return df


###########################################################
################# DEMAND REPORT FUNCTIONS ################
###########################################################

def uae_current_demand():
    """
    Generates UAE demand report across multiple planning horizons
    (3, 4, 5, 6, 9 months) using the weighted moving average forecast.
    Saves to a timestamped CSV and returns a DataFrame.
    """
    df_stock    = actual_stock()
    df_forecast = get_uae_forecast()
    df_items    = get_item_details()

    df = (
        df_forecast
        .merge(df_stock, on='item_code', how='left')
        .merge(df_items, on='item_code', how='inner')
    )

    df = df.rename(columns={
        'uae_total_qty': 'uae_stock',
        'qat_total_qty': 'qat_stock'
    })

    # Calculate demand across horizons
    for months in [3, 4, 5, 6, 9]:
        forecast_col = f'uae_{months}m_forecast'
        demand_col   = f'uae_{months}m_demand'
        df[forecast_col] = (df['uae_monthly_forecast'] * months).apply(math.ceil)
        df[demand_col]   = df[forecast_col] - df['uae_stock'].fillna(0)

    df.insert(1, 'brand_code', df['item_code'].str[:4])

    # Drop intermediate forecast columns
    drop_cols = [f'uae_{m}m_forecast' for m in [3, 4, 5, 6, 9]]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns])

    # Move stock to end
    stock_cols = ['uae_stock', 'qat_stock']
    df = df[[c for c in df.columns if c not in stock_cols] + stock_cols]

    df = df.sort_values(by=['brand_code', 'uae_6m_demand'], ascending=[True, False])

    df.to_csv(f'uae_current_demand_{datetime.now().date()}.csv', index=False)
    print(f"UPDATED: uae_current_demand_{datetime.now().date()}.csv")
    return df


def qat_current_demand():
    """
    Generates Qatar demand report across multiple planning horizons
    (3, 4, 5, 6, 9 months) using the weighted moving average forecast.
    Saves to a timestamped CSV and returns a DataFrame.
    """
    df_stock    = actual_stock()
    df_forecast = get_qat_forecast()
    df_items    = get_item_details()

    df = (
        df_forecast
        .merge(df_stock, on='item_code', how='left')
        .merge(df_items, on='item_code', how='inner')
    )

    df = df.rename(columns={
        'qat_total_qty': 'qat_stock',
        'uae_total_qty': 'uae_stock'
    })

    for months in [3, 4, 5, 6, 9]:
        forecast_col = f'qat_{months}m_forecast'
        demand_col   = f'qat_{months}m_demand'
        df[forecast_col] = (df['qat_monthly_forecast'] * months).apply(math.ceil)
        df[demand_col]   = df[forecast_col] - df['qat_stock'].fillna(0)

    df.insert(1, 'brand_code', df['item_code'].str[:4])

    drop_cols = [f'qat_{m}m_forecast' for m in [3, 4, 5, 6, 9]]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns])

    stock_cols = ['uae_stock', 'qat_stock']
    df = df[[c for c in df.columns if c not in stock_cols] + stock_cols]

    df = df.sort_values(by=['brand_code', 'qat_6m_demand'], ascending=[True, False])

    df.to_csv(f'qat_current_demand_{datetime.now().date()}.csv', index=False)
    print(f"UPDATED: qat_current_demand_{datetime.now().date()}.csv")
    return df


def combined_forecast_report():
    """
    Generates a combined UAE + Qatar forecast report.
    Merges both country forecasts with stock levels and item details.
    Saves to a timestamped CSV and returns a DataFrame.
    """
    df_uae   = get_uae_forecast()
    df_qat   = get_qat_forecast()
    df_stock = actual_stock()
    df_items = get_item_details()

    df = (
        df_uae[['item_code', 'uae_monthly_forecast']]
        .merge(df_qat[['item_code', 'qat_monthly_forecast']], on='item_code', how='outer')
        .merge(df_stock, on='item_code', how='left')
        .merge(df_items, on='item_code', how='left')
    )

    df = df.rename(columns={
        'uae_total_qty': 'uae_stock',
        'qat_total_qty': 'qat_stock'
    })

    df['uae_6m_forecast'] = (df['uae_monthly_forecast'].fillna(0) * 6).apply(math.ceil)
    df['qat_6m_forecast'] = (df['qat_monthly_forecast'].fillna(0) * 6).apply(math.ceil)

    stock_cols = ['uae_stock', 'qat_stock']
    df = df[[c for c in df.columns if c not in stock_cols] + stock_cols]

    df.to_csv(f'combined_forecast_{datetime.now().date()}.csv', index=False)
    print(f"UPDATED: combined_forecast_{datetime.now().date()}.csv")
    return df


def danger_zone_report():
    """
    Generates a critical demand snapshot showing items where
    combined 2-month demand (UAE + Qatar) exceeds available stock.
    Uses combined demand logic — UAE is the main warehouse, Qatar
    draws from it. Only items with combined demand >= 1 are shown.
    Saves to a timestamped CSV and returns a DataFrame.
    """
    df_uae   = get_uae_forecast()
    df_qat   = get_qat_forecast()
    df_stock = actual_stock()
    df_items = get_item_details()

    df = (
        df_uae[['item_code', 'uae_monthly_forecast']]
        .merge(df_qat[['item_code', 'qat_monthly_forecast']], on='item_code', how='outer')
        .merge(df_stock, on='item_code', how='left')
        .merge(df_items, on='item_code', how='left')
    )

    df = df.rename(columns={
        'uae_total_qty': 'uae_stock',
        'qat_total_qty': 'qat_stock'
    })

    # Fill NA with 0
    df['uae_monthly_forecast'] = df['uae_monthly_forecast'].fillna(0)
    df['qat_monthly_forecast'] = df['qat_monthly_forecast'].fillna(0)
    df['uae_stock'] = df['uae_stock'].fillna(0)
    df['qat_stock'] = df['qat_stock'].fillna(0)

    df['uae_2m_demand'] = (
        (df['uae_monthly_forecast'] * 2).apply(math.ceil) - df['uae_stock']
    ).fillna(0)

    df['qat_2m_demand'] = (
        (df['qat_monthly_forecast'] * 2).apply(math.ceil) - df['qat_stock']
    ).fillna(0)

    # Filter: combined demand must be >= 1
    df['combined_demand'] = df['uae_2m_demand'] + df['qat_2m_demand']
    df = df[df['combined_demand'] >= 1].drop(columns=['combined_demand'])

    df.insert(1, 'brand_code', df['item_code'].str[:4])

    df = df[[
        'item_code', 'brand_code', 'item_name', 'valuation_rate', 'brand',
        'uae_monthly_forecast', 'qat_monthly_forecast',
        'uae_2m_demand', 'qat_2m_demand',
        'uae_stock', 'qat_stock'
    ]]

    df = df.sort_values(by='item_code')

    df.to_csv(f'danger_zone_{datetime.now().date()}.csv', index=False)
    print(f"UPDATED: danger_zone_{datetime.now().date()}.csv")
    return df


def reorder_point():
    """
    Calculates reorder points for items that have sold at least 2 units
    in the last 3 months. Reorder point is set at 2x the monthly forecast,
    giving a 2-month buffer before stockout.
    Saves to a timestamped CSV and returns a DataFrame.
    Intended for manual runs only — not part of the full cycle.
    """
    df = pd.read_sql(text("""
        SELECT
            item_code,
            SUM(stock_qty) AS units_sold
        FROM sales
        WHERE posting_date >= NOW() - INTERVAL '3 months'
        GROUP BY item_code
        HAVING SUM(stock_qty) >= 2
        ORDER BY item_code
    """), engine)

    df['monthly_forecast'] = df['units_sold'] / 3
    df['4m_forecast']      = (df['monthly_forecast'] * 4).apply(math.ceil)
    df['reorder_point']    = (df['monthly_forecast'] * 2).apply(math.ceil)

    df.to_csv(f"reorder_point_{datetime.now().date()}.csv", index=False)
    print(f"UPDATED: reorder_point_{datetime.now().date()}.csv")
    return df


###########################################################
############## FIREBASE DATA PREPARATION #################
###########################################################

def prepare_firebase_data():
    """
    Builds the demand snapshot for Firebase.
    Calculates 2, 3, and 4 month demand for both UAE and Qatar
    using the weighted moving average forecast.
    Pushes demand_items and brands collections to Firestore.
    Also writes last_updated timestamp to the meta collection.

    Filtering rules:
    - Combined demand (UAE + QAT) must be >= 1
    - total_value must be > 0 (items with no valuation rate are excluded)

    Item codes with '/' are sanitized to '-' for Firestore document IDs.
    """
    df_uae   = get_uae_forecast()
    df_qat   = get_qat_forecast()
    df_stock = actual_stock()
    df_items = get_item_details()

    df = (
        df_uae[['item_code', 'uae_monthly_forecast']]
        .merge(df_qat[['item_code', 'qat_monthly_forecast']], on='item_code', how='outer')
        .merge(df_stock, on='item_code', how='left')
        .merge(df_items, on='item_code', how='left')
    )

    df = df.rename(columns={
        'uae_total_qty': 'uae_stock',
        'qat_total_qty': 'qat_stock'
    })
    
    # Fill missing sales with 0
    df['uae_monthly_forecast'] = df['uae_monthly_forecast'].fillna(0)
    df['qat_monthly_forecast'] = df['qat_monthly_forecast'].fillna(0)

    # Fill missing stock with 0
    df['uae_stock'] = df['uae_stock'].fillna(0)
    df['qat_stock'] = df['qat_stock'].fillna(0)

    # Calculate demand for 2, 3, and 4 month horizons
    for months in [2, 3, 4]:
        df[f'uae_{months}m_demand'] = (
            (df['uae_monthly_forecast'] * months).apply(math.ceil) - df['uae_stock']
        ).fillna(0)
        df[f'qat_{months}m_demand'] = (
            (df['qat_monthly_forecast'] * months).apply(math.ceil) - df['qat_stock']
        ).fillna(0)

    # Calculate total order values per horizon
    # Combined demand is used for value (negatives reduce the total naturally)
    for months in [2, 3, 4]:
        combined = df[f'uae_{months}m_demand'] + df[f'qat_{months}m_demand']
        df[f'total_value_{months}m'] = combined * df['valuation_rate'].fillna(0)

    # Filter: combined 2-month demand must be >= 1
    df['combined_demand'] = df['uae_2m_demand'] + df['qat_2m_demand']
    df = df[df['combined_demand'] >= 1].drop(columns=['combined_demand'])

    # Filter: must have a valid valuation rate
    df = df[df['total_value_2m'] > 0]

    df.insert(1, 'brand_code', df['item_code'].str[:4])

    # Select final columns for Firebase
    df = df[[
        'item_code', 'brand_code', 'item_name', 'brand', 'valuation_rate',
        'uae_stock', 'qat_stock',
        'uae_2m_demand', 'qat_2m_demand',
        'uae_3m_demand', 'qat_3m_demand',
        'uae_4m_demand', 'qat_4m_demand',
        'total_value_2m', 'total_value_3m', 'total_value_4m'
    ]]

    df = df.sort_values(by='item_code')

    # Save CSV before sanitizing item codes
    df.to_csv('firebase_data_sku.csv', index=False)
    print("UPDATED: firebase_data_sku.csv")

    # Sanitize item codes for Firestore (no '/' allowed in document IDs)
    df['item_code'] = df['item_code'].str.replace('/', '-', regex=False)

    # Push demand_items collection
    while True:
        print("\n--- Push demand_items to Firebase? ---")
        print("1. Yes")
        print("2. No")
        choice = input("\nSelect option: ")

        if choice == "1":
            push_to_firebase(df, "demand_items", "item_code")
            db_firebase.collection("meta").document("last_updated").set({
                "timestamp": datetime.now().isoformat()
            })
            print("Updated last_updated timestamp")
            break
        elif choice == "2":
            break
        else:
            print("Invalid choice, try again.")

    # Build brands summary by aggregating SKU values
    df2 = df.groupby(['brand_code']).agg(
        brand         = ('brand', 'first'),
        total_value_2m = ('total_value_2m', 'sum'),
        total_value_3m = ('total_value_3m', 'sum'),
        total_value_4m = ('total_value_4m', 'sum'),
        sku_count     = ('item_code', 'count')
    ).reset_index()

    df2 = df2[['brand_code', 'brand', 'sku_count', 'total_value_2m', 'total_value_3m', 'total_value_4m']]
    df2 = df2.sort_values(by='total_value_2m', ascending=False)

    df2.to_csv('firebase_data_brand.csv', index=False)
    print("UPDATED: firebase_data_brand.csv")

    # Push brands collection
    while True:
        print("\n--- Push brands to Firebase? ---")
        print("1. Yes")
        print("2. No")
        choice = input("\nSelect option: ")

        if choice == "1":
            push_to_firebase(df2, "brands", "brand_code")
            db_firebase.collection("meta").document("last_updated").set({
                "timestamp": datetime.now().isoformat()
            })
            print("Updated last_updated timestamp")
            break
        elif choice == "2":
            break
        else:
            print("Invalid choice, try again.")


###########################################################
##################### FULL CYCLE ########################
###########################################################

def run_full_cycle():
    """
    Runs the complete pipeline end to end:
    1. Pulls and updates item list from ERP
    2. Pulls and updates stock levels from ERP
    3. Generates UAE demand report
    4. Generates Qatar demand report
    5. Generates combined forecast report
    6. Generates danger zone report
    7. Initiates Firebase data preparation and push
    """
    print("\n--- Running Full Cycle ---")

    print("\n1. Updating Items...")
    data_to_db(fetch_all_items(), "items")

    print("\n2. Updating Stock...")
    data_to_db(fetch_stock(), "stock")
    actual_stock()

    print("\n3. Generating UAE Demand...")
    uae_current_demand()

    print("\n4. Generating Qatar Demand...")
    qat_current_demand()

    print("\n5. Generating Combined Forecast...")
    combined_forecast_report()

    print("\n6. Generating Danger Zone Report...")
    danger_zone_report()

    print("\n7. Preparing Firebase Data...")
    prepare_firebase_data()

    print("\nFull cycle completed.")


###########################################################
###################### MENU LOOPS #######################
###########################################################

def country_selection_demand():
    """Sub-menu for selecting which country demand report to generate."""
    while True:
        print("\n--- Demand Reports ---")
        print("1. UAE Demand")
        print("2. Qatar Demand")
        print("3. Both")
        print("0. Back")
        choice = input("\nSelect option: ")

        if choice == "1":
            uae_current_demand()
        elif choice == "2":
            qat_current_demand()
        elif choice == "3":
            uae_current_demand()
            qat_current_demand()
        elif choice == "0":
            break
        else:
            print("Invalid choice, try again.")


def stock_fetching_selection():
    """Sub-menu for stock data — pull from ERP, clean, or both."""
    print("\n--- Stock Data ---")
    print("1. Pull and Clean")
    print("2. Pull Only")
    print("3. Clean Only")
    print("0. Back")
    choice = input("\nEnter option: ")

    if choice == "1":
        data_to_db(fetch_stock(), "stock")
        actual_stock()
    elif choice == "2":
        data_to_db(fetch_stock(), "stock")
    elif choice == "3":
        actual_stock()
    elif choice == "0":
        pass
    else:
        print("Invalid choice, try again.")


def main():
    """Main menu loop."""
    while True:
        print("\n--- Menu ---")
        print("1. Update Current Demand")
        print("2. Update Stock Levels")
        print("3. Update Item List")
        print("4. Combined Forecast Report")
        print("5. Danger Zone Report")
        print("6. Firebase — Prepare and Push")
        print("7. Run Full Cycle")
        print("8. Update Sales Data")
        print("9. Reorder Point Report")
        print("0. Exit")
        choice = input("\nEnter option: ")

        if choice == "1":
            country_selection_demand()
        elif choice == "2":
            stock_fetching_selection()
        elif choice == "3":
            data_to_db(fetch_all_items(), "items")
        elif choice == "4":
            combined_forecast_report()
        elif choice == "5":
            danger_zone_report()
        elif choice == "6":
            prepare_firebase_data()
        elif choice == "7":
            run_full_cycle()
        elif choice == "8":
            data_to_db(read_clean_sales(), "sales")
        elif choice == "9":
            reorder_point()
        elif choice == "0":
            print("Goodbye!")
            break
        else:
            print("Invalid choice, try again.")


# ----MAIN----- #
if __name__ == "__main__":
    main()