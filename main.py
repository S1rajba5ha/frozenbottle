# main.py

import os
import io
import time
import jwt
import requests
import pandas as pd
import psycopg2
from datetime import datetime, timedelta

# =====================================================
# CONFIG
# =====================================================

API_KEY = os.getenv("API_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT", 5432)

TABLE_NAME = "sales"   # change if needed

# yesterday business load by default
LOAD_DATE = (datetime.today() - timedelta(days=0)).strftime("%Y-%m-%d")

# =====================================================
# AUTH TOKEN
# =====================================================

def get_token():
    payload = {
        "iss": API_KEY,
        "iat": int(time.time())
    }
    return jwt.encode(payload, SECRET_KEY, algorithm="HS256")


def headers():
    return {
        "x-api-key": API_KEY,
        "x-api-token": get_token(),
        "content-type": "application/json"
    }


# =====================================================
# API FETCH
# =====================================================

def get_branches():
    print("Fetching branch list...")

    url = "https://api.ristaapps.com/v1/branch/list"

    response = requests.get(
        url,
        headers=headers(),
        timeout=60
    )

    response.raise_for_status()

    branches = [
        b["branchCode"]
        for b in response.json()
        if "branchCode" in b
    ]

    print(f"Total branches found: {len(branches)}")

    return branches


def fetch_sales_summary(branch, day):
    url = "https://api.ristaapps.com/v1/sales/summary"

    params = {
        "branch": branch,
        "day": day
    }

    all_data = []

    while True:
        response = requests.get(
            url,
            headers=headers(),
            params=params,
            timeout=120
        )

        response.raise_for_status()

        js = response.json()

        all_data.extend(js.get("data", []))

        if not js.get("lastKey"):
            break

        params["lastKey"] = js["lastKey"]

    return all_data


def get_all_sales(day):
    all_sales = []
    branches = get_branches()

    for branch in branches:
        try:
            print(f"Fetching sales for: {branch}")

            branch_data = fetch_sales_summary(branch, day)
            all_sales.extend(branch_data)

            print(f"Rows fetched: {len(branch_data)}")

        except Exception as e:
            print(f"Failed branch {branch}: {str(e)}")

    df = pd.DataFrame(all_sales)

    print(f"Total rows fetched: {len(df)}")

    return df


# =====================================================
# DATA CLEANING
# =====================================================

def process_data(df):
    print("Processing data...")

    if df.empty:
        raise Exception("No data fetched from API")

    # remove unwanted columns if exists
    drop_cols = ["saleBy", "label", "tipAmount"]
    existing_drop_cols = [c for c in drop_cols if c in df.columns]
    df = df.drop(columns=existing_drop_cols)

    # datetime handling
    df["invoiceDate"] = pd.to_datetime(
        df["invoiceDate"],
        errors="coerce"
    )

    df["Hour"] = df["invoiceDate"].dt.hour.astype("Int64")

    # business date logic (till 6AM previous day)
    df["BusinessDate"] = (
        df["invoiceDate"] - pd.Timedelta(hours=6)
    ).dt.date

    df["date"] = df["invoiceDate"].dt.date

    # lowercase channel safely
    cl = df["channel"].fillna("").astype(str).str.lower()

    df["BusinessBrand"] = cl.apply(
        lambda x:
            "Frozen Bottle" if "frozen bottle" in x else
            "Madno" if "madno" in x else
            "Lubov" if "lubov" in x else
            "Boba Bar" if "boba bar" in x else
            "Unknown"
    )

    df["Source"] = cl.apply(
        lambda x:
            "In-Store" if "store" in x else
            "Swiggy" if "swiggy" in x else
            "Zomato" if "zomato" in x else
            "Ownly" if "ownly" in x else
            "Other"
    )

    df["Channel_Category"] = cl.apply(
        lambda x:
            "Offline" if "store" in x else
            "Online"
    )

    correct_cols = [
        "branchName",
        "branchCode",
        "invoiceNumber",
        "invoiceDate",
        "invoiceType",
        "directChargeAmount",
        "chargeAmount",
        "grossAmount",
        "discountAmount",
        "taxAmount",
        "netAmount",
        "roundOffAmount",
        "totalAmount",
        "channel",
        "status",
        "sessionLabel",
        "customerId",
        "customerName",
        "Hour",
        "BusinessDate",
        "date",
        "BusinessBrand",
        "Source",
        "Channel_Category"
    ]

    df = df.reindex(columns=correct_cols)

    df = df.replace("", None)

    print("Data processing completed.")

    return df


# =====================================================
# DB LOAD
# =====================================================

def get_connection():
    print("Connecting to Neon DB...")

    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
        sslmode="require"
    )

    print("Database connected successfully.")

    return conn


def load_to_postgres(df):
    conn = None
    cursor = None

    try:
        conn = get_connection()
        cursor = conn.cursor()

        load_date = df["BusinessDate"].iloc[0]

        print(f"Deleting existing data for: {load_date}")

        delete_query = f"""
            DELETE FROM {TABLE_NAME}
            WHERE businessdate = %s;
        """

        cursor.execute(delete_query, (load_date,))
        print(f"Deleted rows: {cursor.rowcount}")

        print("Starting COPY insert...")

        buffer = io.StringIO()
        df.to_csv(
            buffer,
            index=False,
            header=False,
            sep=",",
            quoting=1
        )
        buffer.seek(0)

        copy_query = f"""
            COPY {TABLE_NAME} (
                branchName,
                branchCode,
                invoiceNumber,
                invoiceDate,
                invoiceType,
                directChargeAmount,
                chargeAmount,
                grossAmount,
                discountAmount,
                taxAmount,
                netAmount,
                roundOffAmount,
                totalAmount,
                channel,
                status,
                sessionLabel,
                customerId,
                customerName,
                Hour,
                BusinessDate,
                date,
                BusinessBrand,
                Source,
                Channel_Category
            )
            FROM STDIN WITH (FORMAT CSV)
        """

        cursor.copy_expert(copy_query, buffer)

        print("Running store master update...")

        cursor.execute(
            "SELECT update_sales_from_master(%s);",
            (load_date,)
        )

        conn.commit()

        print(f"Inserted rows: {len(df)}")
        print("ETL completed successfully.")

    except Exception as e:
        if conn:
            conn.rollback()

        print(f"Load failed: {str(e)}")
        raise

    finally:
        if cursor:
            cursor.close()

        if conn:
            conn.close()

        print("Database connection closed.")


# =====================================================
# MAIN
# =====================================================

def main():
    print("====================================")
    print("STARTING DAILY SALES ETL")
    print("====================================")

    print(f"Load Date: {LOAD_DATE}")

    df = get_all_sales(LOAD_DATE)

    if df.empty:
        raise Exception("No sales data received from API")

    df = process_data(df)

    load_to_postgres(df)

    print("====================================")
    print("ETL FINISHED SUCCESSFULLY")
    print("====================================")


if __name__ == "__main__":
    main()
