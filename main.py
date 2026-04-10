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

# AMAZON Environment #### CURRENTLY UNUSED
AMAZON_ID = os.getenv("AMAZON_ID")
AMAZON_SECRET = os.getenv("AMAZON_SECRET")
AMAZON_BASE_URL = os.getenv("AMAZON_BASE_URL")
AMAZON_BASE_URL_SANDBOX = os.getenv("AMAZON_BASE_URL_SANDBOX")
AMAZON_MARKETPLACE_ID = os.getenv("AMAZON_MARKETPLACE_ID")

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")



# Report Generation Helpers
ERP_BASE_URL = os.getenv("ERP_BASE_URL")
ERP_USER = os.getenv("ERP_USER")
ERP_PASSWORD = os.getenv("ERP_PASSWORD")

# Log to ERP Session
session = requests.Session()

login = session.post(f"{ERP_BASE_URL}/api/method/login", data={
    "usr": ERP_USER,
    "pwd": ERP_PASSWORD
})

print(f"Logged in as {login.json()['full_name']}") # It should say logged in

# Firebase Initiate
cred = credentials.Certificate("firebaseJWT.json")
firebase_admin.initialize_app(cred)
db = firestore.client()
print("Firestore Connected Successfully")

# Initiate Engine
engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

###########################################################
################## HELPER FUNCTIONS #######################
###########################################################


###############
# GET ALL ITEMS
def fetch_all_items():
    all_items = []
    start = 0
    batch = 500

    while True:
        print(f"Fetching items {start} to {start + batch}...")
        
        response = session.get(f"{ERP_BASE_URL}/api/resource/Item", params={
            "limit_page_length": batch,
            "limit_start": start,
            "fields": '["item_code", "item_name", "brand"]'
        }, timeout=30)

        data = response.json().get("data", [])
        if not data:
            break
        
        all_items.extend(data)
        start += batch

    if not all_items:
        print("No items fetched, check API connection")
        return

    print(f"Total items fetched: {len(all_items)}")
    df = pd.DataFrame(all_items)
    df.to_csv("full_item_list.csv", index=False)
    print("Saved to full_item_list.csv")

    return df

######################
# GET ALL STOCK LEVELS
def fetch_stock():
    stock = []
    start = 0
    batch = 500
    
    while True:
        print(f"Fetching stock {start} to {start + batch}...")

        response = session.get(f"{ERP_BASE_URL}/api/resource/Bin", params={
            "limit_page_length": batch,
            "limit_start": start,
            "fields": '["item_code", "warehouse", "actual_qty", "reserved_qty", "valuation_rate", "stock_value"]'
        }, timeout=30)

        data = response.json().get("data", [])
        if not data:
            break

        stock.extend(data)
        start+=batch
    if not stock:
        print("Stock levels could not be fetched. Check API connection")
        return
    
    print(f"Total lines fetched: {len(stock)}")
    df = pd.DataFrame(stock)
    df.to_csv("raw_stock.csv", index=False)
    print(f"Saved to raw_stock.csv")

    return df

#######################
# UPLOADING TO DATABASE
def data_to_db(df, table_name, if_exists="replace"):
    count = pd.read_sql(text(f"""
    SELECT COUNT(*) FROM {table_name}
    """), engine).iloc[0,0]
    print(f"Wiping: {count} rows from {table_name}")

    df.to_sql(table_name, engine, if_exists=if_exists, index=False)
    print(f"Uploaded {len(df)} rows to '{table_name}'")

#######################
# FIREBASE: IMPORTING
def push_to_firebase(df, collection_name, document_id):
    # Delete existing
    docs = db.collection(collection_name).stream()
    delete_batch = db.batch()
    count = 0
    for doc in docs:
        delete_batch.delete(doc.reference)
        count += 1
        if count % 500 == 0:
            delete_batch.commit()
            delete_batch = db.batch()
    delete_batch.commit()

    records = df.to_dict(orient="records")
    batch = db.batch()
    for i, row in enumerate(records):
        ref = db.collection(collection_name).document(row[document_id])
        batch.set(ref, row)
        if (i + 1) % 500 == 0: # check if the remainder is 0 that means 500 batch is looped
            batch.commit()
            batch = db.batch()
    batch.commit()


###########################################################
########## SALES DATE: IMPORTING AND CLEANING #############
###########################################################
def read_clean_sales():
    df = pd.read_csv("sales_data.csv", encoding="latin1", usecols=[
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
    
    df.columns = [re.sub(r'[^a-z0-9_]','', col.strip().lower().replace(" ", "_")) for col in df.columns]
    df['posting_date'] = pd.to_datetime(df['posting_date'], dayfirst=True)
    df.to_csv("sales_data_cleaned.csv", index=False)
    print(f"Saved to sales_data_cleaned.csv")

    return df


###########################################################
################### QUERIES FOR DATA ######################
###########################################################


#FULL ITEM LIST
def get_item_details():
    df = pd.read_sql(text("""
    SELECT
        i.item_code,
        i.item_name,
        i.brand,
        COALESCE(v.valuation_rate, 0) as valuation_rate
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

#UAE FORECAST FROM DATABASE
def get_uae_forecast():
    df = pd.read_sql(text("""
    WITH three_months AS(
        SELECT
            item_code,
            SUM(CASE WHEN company = 'OldTimer Motorcycles and Spare Parts Trading LLC' THEN stock_qty ELSE 0 END) as units_sold,
                            
            SUM(CASE WHEN customer_group IN ('Individual', 'VIP', 'Customers', 'Walk-In Customers') AND company = 'OldTimer Motorcycles and Spare Parts Trading LLC' THEN stock_qty ELSE 0 END) as uae_showroom_sales,
            SUM(CASE WHEN customer_group IN ('Market Place', 'Amazon', 'ruroc.ae') AND company = 'OldTimer Motorcycles and Spare Parts Trading LLC' THEN stock_qty ELSE 0 END) as uae_ecommerce_sales,
            SUM(CASE WHEN customer_group IN ('Dealers and Distribution', 'B2B') AND company ='OldTimer Motorcycles and Spare Parts Trading LLC' THEN stock_qty ELSE 0 END) as uae_distribution_sales
                            
        FROM sales
        WHERE posting_date >= NOW() - INTERVAL '3 months'
        GROUP BY item_code
        ORDER BY item_code
    ),
    six_months AS (
        SELECT
            item_code,
            SUM(CASE WHEN company = 'OldTimer Motorcycles and Spare Parts Trading LLC' THEN stock_qty ELSE 0 END) as units_sold_6m,
                            
            SUM(CASE WHEN customer_group IN ('Individual', 'VIP', 'Customers', 'Walk-In Customers') AND company = 'OldTimer Motorcycles and Spare Parts Trading LLC' THEN stock_qty ELSE 0 END) as uae_showroom_sales_6m,
            SUM(CASE WHEN customer_group IN ('Market Place', 'Amazon', 'ruroc.ae') AND company = 'OldTimer Motorcycles and Spare Parts Trading LLC' THEN stock_qty ELSE 0 END) as uae_ecommerce_sales_6m,
            SUM(CASE WHEN customer_group IN ('Dealers and Distribution', 'B2B') AND company ='OldTimer Motorcycles and Spare Parts Trading LLC' THEN stock_qty ELSE 0 END) as uae_distribution_sales_6m
                            
        FROM sales
        WHERE posting_date >= NOW() - INTERVAL '6 months'
        GROUP BY item_code
        ORDER BY item_code
            
    )
    SELECT
        COALESCE(t.item_code, s.item_code) as item_code,
        t.units_sold,
        t.uae_showroom_sales,
        t.uae_ecommerce_sales,
        t.uae_distribution_sales,
        s.units_sold_6m,
        s.uae_showroom_sales_6m,
        s.uae_ecommerce_sales_6m,
        s.uae_distribution_sales_6m
    FROM six_months s
    LEFT JOIN three_months t ON s.item_code = t.item_code
    """), engine)


    df['uae_monthly_forecast'] = (
        df['uae_showroom_sales'].fillna(0) + 
        df['uae_ecommerce_sales'].fillna(0) + 
        df['uae_distribution_sales'].fillna(0)
    ) / 3
    df['uae_4_month_forecast'] = df['uae_monthly_forecast'] * 4
    df['uae_4_month_forecast'] = df['uae_4_month_forecast'].apply(math.ceil)

    #6 months range
    df['uae_monthly_forecast_6m'] = (
        df['uae_showroom_sales_6m'].fillna(0) + 
        df['uae_ecommerce_sales_6m'].fillna(0) + 
        df['uae_distribution_sales_6m'].fillna(0)
    ) / 6
    df['slow_uae_forecast_10_months'] = df['uae_monthly_forecast_6m'] * 10
    df['slow_uae_forecast_10_months'] = df['slow_uae_forecast_10_months'].apply(math.ceil)
        
    return df

#QAT FORECAST FROM DATABASE
def get_qat_forecast():
    df = pd.read_sql(text("""
    WITH three_months AS(
        SELECT
            item_code,

            SUM(CASE WHEN company = 'Oldtimer Motorcycles Trading WLL' THEN stock_qty ELSE 0 END) as units_sold,
                            
            SUM(CASE WHEN customer_group IN ('Individual', 'VIP', 'Customers', 'Walk-In Customers') AND company = 'Oldtimer Motorcycles Trading WLL' THEN stock_qty ELSE 0 END) as qat_showroom_sales,
            SUM(CASE WHEN customer_group IN ('Market Place', 'Amazon', 'ruroc.ae') AND company = 'Oldtimer Motorcycles Trading WLL' THEN stock_qty ELSE 0 END) as qat_ecommerce_sales,
            SUM(CASE WHEN customer_group IN ('Dealers and Distribution', 'B2B') AND company ='Oldtimer Motorcycles Trading WLL' THEN stock_qty ELSE 0 END) as qat_distribution_sales
                            
        FROM sales
        WHERE posting_date >= NOW() - INTERVAL '3 months'
        GROUP BY item_code
        ORDER BY item_code
    ),
    six_months AS (
        SELECT
            item_code,
            SUM(CASE WHEN company = 'Oldtimer Motorcycles Trading WLL' THEN stock_qty ELSE 0 END) as units_sold_6m,
                            
            SUM(CASE WHEN customer_group IN ('Individual', 'VIP', 'Customers', 'Walk-In Customers') AND company = 'Oldtimer Motorcycles Trading WLL' THEN stock_qty ELSE 0 END) as qat_showroom_sales_6m,
            SUM(CASE WHEN customer_group IN ('Market Place', 'Amazon', 'ruroc.ae') AND company = 'Oldtimer Motorcycles Trading WLL' THEN stock_qty ELSE 0 END) as qat_ecommerce_sales_6m,
            SUM(CASE WHEN customer_group IN ('Dealers and Distribution', 'B2B') AND company ='Oldtimer Motorcycles Trading WLL' THEN stock_qty ELSE 0 END) as qat_distribution_sales_6m
                            
        FROM sales
        WHERE posting_date >= NOW() - INTERVAL '6 months'
        GROUP BY item_code
        ORDER BY item_code
            
    )
    SELECT
        COALESCE(t.item_code, s.item_code) as item_code,
        t.units_sold,
        t.qat_showroom_sales,
        t.qat_ecommerce_sales,
        t.qat_distribution_sales,
        s.units_sold_6m,
        s.qat_showroom_sales_6m,
        s.qat_ecommerce_sales_6m,
        s.qat_distribution_sales_6m
    FROM six_months s
    LEFT JOIN three_months t ON s.item_code = t.item_code
    """), engine)


    df['qat_monthly_forecast'] = (
        df['qat_showroom_sales'].fillna(0) + 
        df['qat_ecommerce_sales'].fillna(0) + 
        df['qat_distribution_sales'].fillna(0)
    ) / 3
    df['qat_4_month_forecast'] = df['qat_monthly_forecast'] * 4
    df['qat_4_month_forecast'] = df['qat_4_month_forecast'].apply(math.ceil)

    #6 months range
    df['qat_monthly_forecast_6m'] = (df['qat_showroom_sales_6m'].fillna(0) + df['qat_ecommerce_sales_6m'].fillna(0) + df['qat_distribution_sales_6m'].fillna(0)) / 6
    df['slow_qat_forecast_10_months'] = df["qat_monthly_forecast_6m"] * 10
    df['slow_qat_forecast_10_months'] = df['slow_qat_forecast_10_months'].apply(math.ceil)
    
    return df

def reorder_point():
    df = pd.read_sql(text("""
    SELECT
        item_code,
        SUM(stock_qty) as units_sold
    FROM sales
    WHERE posting_date >= NOW() - INTERVAL '3 months'
    GROUP BY item_code
    HAVING SUM(stock_qty) >= 2
    ORDER BY item_code
    """), engine)

    df['uae_monthly_forecast'] = df['units_sold'] / 3
    df['uae_4_month_forecast'] = df["uae_monthly_forecast"] * 4
    df['reorder_point'] = (df['uae_monthly_forecast']*2).apply(math.ceil)

    df.to_csv(f"reorder_point_{datetime.now().date()}.csv", index=False)
    print(f'UPDATED: reorder_point_{datetime.now().date()}.csv')
    return df

# GET ACTUAL STOCK LEVELS FROM DATABASE (SEPARATED BY COUNTRY)
def actual_stock():
    df = pd.read_sql(text("""
        WITH uae AS(
            SELECT
                item_code,
                SUM(actual_qty) as uae_total_qty
                FROM stock
                                
                WHERE warehouse NOT IN ( 'MCS Distribution', 'Warranty-Handed Over', 'Warranty - Ready', 'Returned Items', 'Warranty - Damaged Good', 'Sold Items', 'Reservation JAW', 'Work In Progress', 'Finished Goods', 'Goods In Transit', 'E-Commerce', 'Sold-To-Distribution', 'Sold-To-KSA', 'Warranty - Missing Parts', 'JAW-TO-Distribution', 'Reservation MCS', 'DXB-TO-AMAZON', 'OldTimer Cafe', 'DXB-TO-NOON', 'DXB-TO-KSA', 'MCX-TO-QAT', 'DXB-TO-QAT', 'QA-TO-KSA', 'DHS', 'DHS Distribution', 'GF Warehouse', 'DWH-6-F-4-C', 'Showroom', 'Main Warehouse', 'Qatar Work In Progress', 'Qatar Reservation', 'Qatar Returns', 'Qatar Receiving', 'All Warehouses - OTQAR' ) AND warehouse NOT LIKE 'DHW%'                  
            GROUP BY item_code
        ),
        qat AS(
            SELECT
                item_code,
                SUM(actual_qty) as qat_total_qty
                FROM stock
                                
                WHERE warehouse IN ( 'DHS', 'DHS Distribution', 'DWH-6-F-4-C', 'DXB-TO-QAT', 'Qatar Receiving', 'Qatar Reservation', 'QA-TO-KSA', 'QAT-TO-DXB' ) OR warehouse LIKE 'DHW%'                  
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
    print(f'UPDATED: actual_stock_{datetime.now().date()}.csv')
    return df

# generate UAE only demand
def uae_current_demand():
    df_stock = actual_stock()
    df_forecast = get_uae_forecast()
    df_items = get_item_details()

    df = (df_forecast
          .merge(df_stock, on='item_code', how='left')
          .merge(df_items, on='item_code', how='inner'))

    df = df.rename(columns={
        'uae_total_qty': 'uae_stock',
        'qat_total_qty': 'qat_stock'
    })

    # Forecast ranges (needed to compute demand)
    df['uae_3_month_forecast'] = (df['uae_monthly_forecast'] * 3).apply(math.ceil)
    df['uae_5_month_forecast'] = (df['uae_monthly_forecast'] * 5).apply(math.ceil)
    df['uae_6_month_forecast'] = (df['uae_monthly_forecast'] * 6).apply(math.ceil)
    df['uae_9_month_forecast'] = (df['uae_monthly_forecast'] * 9).apply(math.ceil)

    # DEMANDS (keep all)
    df['uae_3_month_demand'] = df['uae_3_month_forecast'] - df['uae_stock']
    df['uae_4_month_demand'] = df['uae_4_month_forecast'] - df['uae_stock']
    df['uae_5_month_demand'] = df['uae_5_month_forecast'] - df['uae_stock']
    df['uae_6_month_demand'] = df['uae_6_month_forecast'] - df['uae_stock']
    df['uae_9_month_demand'] = df['uae_9_month_forecast'] - df['uae_stock']


    df.insert(1, 'brand_code', df['item_code'].str[:4])

    drop_cols = [
        'uae_3_month_forecast',
        'uae_4_month_forecast',
        'uae_5_month_forecast',
        'uae_6_month_forecast',
        'uae_9_month_forecast'
    ]
    df = df.drop(columns=[col for col in drop_cols if col in df.columns])

    # Move stock columns to the end
    cols = [col for col in df.columns if col not in ['uae_stock', 'qat_stock']]
    df = df[cols + ['uae_stock', 'qat_stock']]

    # Sort
    df = df.sort_values(by=['brand_code', 'uae_6_month_demand'], ascending=[True, False])

    df.to_csv(f'uae_current_demand_{datetime.now().date()}.csv', index=False)
    print(f'UPDATED: uae_current_demand_{datetime.now().date()}.csv')

    return df

# generate Qatar only demand
def qat_current_demand():
    df_stock = actual_stock()
    df_forecast = get_qat_forecast()
    df_items = get_item_details()

    df = (df_forecast
          .merge(df_stock, on='item_code', how='left')
          .merge(df_items, on='item_code', how='inner'))

    df = df.rename(columns={
        'qat_total_qty': 'qat_stock',
        'uae_total_qty': 'uae_stock'
    })

    df['qat_3_month_forecast'] = (df['qat_monthly_forecast'] * 3).apply(math.ceil)
    df['qat_5_month_forecast'] = (df['qat_monthly_forecast'] * 5).apply(math.ceil)
    df['qat_6_month_forecast'] = (df['qat_monthly_forecast'] * 6).apply(math.ceil)
    df['qat_9_month_forecast'] = (df['qat_monthly_forecast'] * 9).apply(math.ceil)

    df['qat_3_month_demand'] = df['qat_3_month_forecast'] - df['qat_stock']
    df['qat_4_month_demand'] = df['qat_4_month_forecast'] - df['qat_stock']
    df['qat_5_month_demand'] = df['qat_5_month_forecast'] - df['qat_stock']
    df['qat_6_month_demand'] = df['qat_6_month_forecast'] - df['qat_stock']
    df['qat_9_month_demand'] = df['qat_9_month_forecast'] - df['qat_stock']

   
    df.insert(1, 'brand_code', df['item_code'].str[:4])

    drop_cols = [
        'qat_3_month_forecast',
        'qat_4_month_forecast',
        'qat_5_month_forecast',
        'qat_6_month_forecast',
        'qat_9_month_forecast'
    ]
    df = df.drop(columns=[col for col in drop_cols if col in df.columns])

    cols = [col for col in df.columns if col not in ['uae_stock', 'qat_stock']]
    df = df[cols + ['uae_stock', 'qat_stock']]

    df = df.sort_values(by=['brand_code', 'qat_6_month_demand'], ascending=[True, False])

    df.to_csv(f'qat_current_demand_{datetime.now().date()}.csv', index=False)
    print(f'UPDATED: qat_current_demand_{datetime.now().date()}.csv')

    return df

# get a combined forecast for all countries
# currently not used for anything other than a generating a csv report
def combined_forecast_report():
    df_uae = get_uae_forecast()
    df_qat = get_qat_forecast()
    df_stock = actual_stock()
    df_items = get_item_details()

    df = (df_uae[['item_code', 'uae_monthly_forecast', 'uae_4_month_forecast']]
          .merge(df_qat[['item_code', 'qat_monthly_forecast', 'qat_4_month_forecast']],
                 on='item_code', how='outer')
          .merge(df_stock, on='item_code', how='left')
          .merge(df_items, on='item_code', how='left'))

    df = df.rename(columns={
        'uae_total_qty': 'uae_stock',
        'qat_total_qty': 'qat_stock'
    })

    df['uae_6_month_forecast'] = (df['uae_monthly_forecast'] * 6).apply(math.ceil)
    df['qat_6_month_forecast'] = (df['qat_monthly_forecast'] * 6).apply(math.ceil)

    cols = [col for col in df.columns if col not in ['uae_stock', 'qat_stock']]
    df = df[cols + ['uae_stock', 'qat_stock']]

    df.to_csv(f'combined_forecast_{datetime.now().date()}.csv', index=False)
    print(f'UPDATED: combined_forecast_{datetime.now().date()}.csv')

    return df

# A supplementary report for critical status items, not used for anything but a csv generation
def danger_zone_report():
    df_uae = get_uae_forecast()
    df_qat = get_qat_forecast()
    df_stock = actual_stock()
    df_items = get_item_details()

    df = (df_uae[['item_code', 'uae_monthly_forecast']]
          .merge(df_qat[['item_code', 'qat_monthly_forecast']],
                 on='item_code', how='outer')
          .merge(df_stock, on='item_code', how='left')
          .merge(df_items, on='item_code', how='left'))

    df = df.rename(columns={
        'uae_total_qty': 'uae_stock',
        'qat_total_qty': 'qat_stock'
    })

    #Filling NA to 0
    df.fillna({
        'uae_stock': 0,
        'qat_stock': 0
    }, inplace=True)



    df['uae_2_month_forecast'] = (df['uae_monthly_forecast'] * 2).apply(math.ceil)
    df['qat_2_month_forecast'] = (df['qat_monthly_forecast'] * 2).apply(math.ceil)

    df['uae_2_month_demand'] = (df['uae_2_month_forecast'] - df['uae_stock']).fillna(0)
    df['qat_2_month_demand'] = (df['qat_2_month_forecast'] - df['qat_stock']).fillna(0)

    df = df[
        (df['uae_2_month_demand'] > 0) |
        (df['qat_2_month_demand'] > 0)
    ]

    df.insert(1, 'brand_code', df['item_code'].str[:4])

    df = df[
        [
            'item_code',
            'brand_code',
            'item_name',
            'valuation_rate',
            'brand',
            'uae_monthly_forecast',
            'qat_monthly_forecast',
            'uae_2_month_demand',
            'qat_2_month_demand',
            'uae_stock',
            'qat_stock'
        ]
    ]

    df = df.sort_values(by='item_code')

    df.to_csv(f'danger_zone_{datetime.now().date()}.csv', index=False)
    print(f'UPDATED: danger_zone_{datetime.now().date()}.csv')

    return df

# Prepare data for firebase and option to push them to firebase
# (pushing is deleting entire collections and pushing fresh for accurate data)
def prepare_firebase_data():
    df_uae = get_uae_forecast()
    df_qat = get_qat_forecast()
    df_stock = actual_stock()
    df_items = get_item_details()

    df = (df_uae[['item_code', 'uae_monthly_forecast']]
          .merge(df_qat[['item_code', 'qat_monthly_forecast']],
                 on='item_code', how='outer')
          .merge(df_stock, on='item_code', how='left')
          .merge(df_items, on='item_code', how='left'))

    df = df.rename(columns={
        'uae_total_qty': 'uae_stock',
        'qat_total_qty': 'qat_stock'
    })

    df['uae_stock'] = df['uae_stock'].fillna(0)
    df['qat_stock'] = df['qat_stock'].fillna(0)


    df['uae_2_month_forecast'] = (df['uae_monthly_forecast'] * 2).apply(math.ceil)
    df['qat_2_month_forecast'] = (df['qat_monthly_forecast'] * 2).apply(math.ceil)

    df['uae_2_month_demand'] = (df['uae_2_month_forecast'] - df['uae_stock']).fillna(0)
    df['qat_2_month_demand'] = (df['qat_2_month_forecast'] - df['qat_stock']).fillna(0)

    #Totals of Values of Demand
    df['total_value'] = ((df['uae_2_month_demand'] + df['qat_2_month_demand']) * df['valuation_rate'])

    df['combined_demand'] = df['uae_2_month_demand'] + df['qat_2_month_demand']

    df = df[df['combined_demand'] >= 1]

    df.insert(1, 'brand_code', df['item_code'].str[:4])

    df = df[
        [
            'item_code',
            'brand_code',
            'item_name',
            'brand',
            'valuation_rate',
            'uae_2_month_demand',
            'qat_2_month_demand',
            'total_value'
        ]
    ]

    df = df.sort_values(by='item_code')

    df.to_csv('firebase_data_sku.csv', index=False)
    print('UPDATED: firebase_data_sku.csv')

    df['item_code'] = df['item_code'].str.replace('/', '-', regex=False)

    while True:
        print("\n--- Would you like to push to firebase? ---")
        print("1. Yes")
        print("2. No")

        choice = input("\nSelect option: ")

        if choice == "1":
            push_to_firebase(df, "demand_items", "item_code")
            db.collection("meta").document("last_updated").set({
                "timestamp": datetime.now().isoformat()
            })
            print("Updated last_updated timestamp") 
            break
        elif choice == "2":
            break
        else:
            print("Invalid choice, try again.")

    df2 = df.groupby('brand_code').agg({
        'total_value' : 'sum',
        'brand' : 'first'
    }).reset_index()

    df2 = df2[
        [
            'brand_code',
            'brand',
            'total_value'
        ]
    ].sort_values(by='total_value', ascending=False)

    df2.to_csv('firebase_data_brand.csv', index=False)
    print('UPDATED: firebase_data_brand.csv')

    while True:
        print("\n--- Would you like to push to firebase? ---")
        print("1. Yes")
        print("2. No")

        choice = input("\nSelect option: ")

        if choice == "1":
            push_to_firebase(df2, "brands", "brand_code")
            db.collection("meta").document("last_updated").set({
                "timestamp": datetime.now().isoformat()
            })
            print("Updated last_updated timestamp")            
            break
        elif choice == "2":
            break
        else:
            print("Invalid choice, try again.")

# run the entire process
def run_full_cycle():
    print("\n--- Running Full Cycle ---")

    print("\n1. Updating Items...")
    data_to_db(fetch_all_items(), "items")

    print("\n2. Updating Stock...")
    data_to_db(fetch_stock(), "stock")
    actual_stock()

    print("\n3. Generating UAE Demand...")
    uae_current_demand()

    print("\n4. Generating QAT Demand...")
    qat_current_demand()

    print("\n5. Generating Combined Forecast...")
    combined_forecast_report()

    print("\n6. Generating Danger Zone CSV...")
    danger_zone_report()

    print("\n7. Initiating Firebase Data Actions...")
    prepare_firebase_data()

    print("\nFull cycle completed.")








###########################################################
####################### MENU LOOP #########################
###########################################################

# Menu for selecting which countries to generate demand for
def country_selection_demand():
    while True:
        print("\n--- Demand Reports ---")
        print("1. UAE Demand")
        print("2. Qatar Demand")
        print("3. Both")
        print("0: Back")

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

# Menu for stock data pull, clean, or both
def stock_fetching_selection():
    print("\n --- What would you like to do with stock? ---")
    print("1. Pull and Clean")
    print("2. Pull Only")
    print("3. Clean Only")
    print("4. Exit")
    
    choice = input("\nEnter option: ")

    if choice == "1":
        data_to_db(fetch_stock(), "stock")
        actual_stock()
    elif choice == "2":
        data_to_db(fetch_stock(), "stock")
    elif choice == "3":
        actual_stock()
    elif choice == "4":
        print("Goodbye!")
    else:
        print("Invalid choice, try again.")


# main menu
def main():
    while True:
        print("\n--- Menu ---")
        print("1. Update Current Demand")
        print("2. Update Stock Levels (API Pull / DB push / CSV Clean)")
        print("3. Update Item List")
        print("4. Generate Combined Forecast Report")
        print("5. Danger Zone Report")
        print("6. Firebase")
        print("7. Run Full Cycle")
        print("0: Exit")

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
        elif choice == "0":
            print("Goodbye!")
            break
        else:
            print("Invalid choice, try again.")



# ----MAIN----- #
if __name__ == "__main__":
    main()

    pass