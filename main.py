import os
from datetime import datetime
import math
import pandas as pd
import requests
import re
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

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

## UPLOADING TO DATABASE
def data_to_db(df, table_name, if_exists="replace"):
    count = pd.read_sql(text(f"""
    SELECT COUNT(*) FROM {table_name}
    """), engine).iloc[0,0]
    print(f"Wiping: {count} rows from {table_name}")

    df.to_sql(table_name, engine, if_exists=if_exists, index=False)
    print(f"Uploaded {len(df)} rows to '{table_name}'")


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
        item_code,
        item_name,
        brand
    FROM items
    """), engine)
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

#GET ACTUAL STOCK LEVELS FROM DATABASE (SEPARATED BY COUNTRY)
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


def uae_current_demand():
    df_stock = actual_stock()
    df_forecast = get_uae_forecast()
    df_items = get_item_details()

    df = (df_forecast
          .merge(df_stock, on='item_code', how='left')
          .merge(df_items, on='item_code', how='inner'))

    #rename total_qty to uae_stock
    df= df.rename(columns={'uae_total_qty' : 'uae_stock'})
    # get more forecast ranges
    df['uae_3_month_forecast'] = (df['uae_monthly_forecast'] * 3).apply(math.ceil)
    df['uae_5_month_forecast'] = (df['uae_monthly_forecast'] * 5).apply(math.ceil)
    df['uae_6_month_forecast'] = (df['uae_monthly_forecast'] * 6).apply(math.ceil)
    df['uae_9_month_forecast'] = (df['uae_monthly_forecast'] * 9).apply(math.ceil)

    # get demands
    df['uae_3_month_demand'] = df['uae_3_month_forecast'] - df['uae_stock']
    df['uae_4_month_demand'] = df['uae_4_month_forecast'] - df['uae_stock']
    df['uae_5_month_demand'] = df["uae_5_month_forecast"] - df['uae_stock']
    df['uae_6_month_demand'] = df["uae_6_month_forecast"] - df['uae_stock']
    df['uae_9_month_demand'] = df["uae_9_month_forecast"] - df['uae_stock']

    # add brand codes
    df.insert(1, 'brand_code', df['item_code'].str[:4])

    #Filter Out Un-Orderable
    #df = df[(df['uae_4_month_demand'] > 0) | (df['uae_5_month_demand'] > 0) | (df['uae_6_month_demand'] > 0) | (df['uae_9_month_demand'] > 0)]

    #sort descending by 6 months forecast, and sort by brand
    df = df.sort_values(by=['brand_code', 'uae_6_month_demand'], ascending=[True, False])
    
    df.to_csv(f'uae_current_demand_{datetime.now().date()}.csv', index=False)
    print(f'UPDATED: uae_current_demand_{datetime.now().date()}.csv')
    return df

def qat_current_demand():
    df_stock = actual_stock()
    df_forecast = get_qat_forecast()
    df_items = get_item_details()

    df = (df_forecast
          .merge(df_stock, on='item_code', how='left')
          .merge(df_items, on='item_code', how='inner'))

    #rename total_qty to qat_stock
    df= df.rename(columns={'qat_total_qty' : 'qat_stock'})
    # get more forecast ranges
    df['qat_3_month_forecast'] = (df['qat_monthly_forecast'] * 3).apply(math.ceil)
    df['qat_5_month_forecast'] = (df['qat_monthly_forecast'] * 5).apply(math.ceil)
    df['qat_6_month_forecast'] = (df['qat_monthly_forecast'] * 6).apply(math.ceil)
    df['qat_9_month_forecast'] = (df['qat_monthly_forecast'] * 9).apply(math.ceil)

    # get demands
    df['qat_3_month_demand'] = df['qat_3_month_forecast'] - df['qat_stock']
    df['qat_4_month_demand'] = df['qat_4_month_forecast'] - df['qat_stock']
    df['qat_5_month_demand'] = df["qat_5_month_forecast"] - df['qat_stock']
    df['qat_6_month_demand'] = df["qat_6_month_forecast"] - df['qat_stock']
    df['qat_9_month_demand'] = df["qat_9_month_forecast"] - df['qat_stock']

    # add brand codes
    df.insert(1, 'brand_code', df['item_code'].str[:4])

    #Filter Out Un-Orderable
    #df = df[(df['qat_4_month_demand'] > 0) | (df['qat_5_month_demand'] > 0) | (df['qat_6_month_demand'] > 0) | (df['qat_9_month_demand'] > 0)]

    #sort descending by 6 months forecast, and sort by brand
    df = df.sort_values(by=['brand_code', 'qat_6_month_demand'], ascending=[True, False])
    
    df.to_csv(f'qat_current_demand_{datetime.now().date()}.csv', index=False)
    print(f'UPDATED: qat_current_demand_{datetime.now().date()}.csv')
    return df

###########################################################
####################### MENU LOOP #########################
###########################################################

def country_selection_demand():
    while True:
        print("\n--- Select Country ---")
        print("1. United Arab Emirates")
        print("2. Qatar")
        print("0: Exit")

        choice = input("\nSelect country: ")

        if choice == "1":
            uae_current_demand()
        elif choice == "2":
            qat_current_demand()
        elif choice == "0":
            print("Goodbye!")
            break
        else:
            print("Invalid choice, try again.")

def stock_fectching_seleciton():
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



def main():
    while True:
        print("\n--- Menu ---")
        print("1. Update Current Demand (Sales Upated)")
        print("2. Update Stock Levels (API Pull / DB push / CSV Clean)")
        print("3. Update Item List")
        print("4. Run Full Cycle")
        print("0: Exit")

        choice = input("\nEnter option: ")

        if choice == "1":
            country_selection_demand()
        elif choice == "2":
            stock_fectching_seleciton()
        elif choice == "0":
            print("Goodbye!")
            break
        else:
            print("Invalid choice, try again.")



# ----MAIN----- #
if __name__ == "__main__":
    main()

    pass