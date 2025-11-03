#!/usr/bin/env python
import os
import sys
import requests
import shutil
import sqlite3
import pandas as pd

def download_and_prepare_db():
    """Download and prepare the database with proper error handling."""
    # Define paths
    db_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 
                          "customer_support_chat", "data", "travel2.sqlite")
    db_dir = os.path.dirname(db_file)
    
    print(f"Database file path: {db_file}")
    
    # Create directory if it doesn't exist
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)
        print(f"Created directory: {db_dir}")
    
    # Remove existing empty database file if it exists
    if os.path.exists(db_file) and os.path.getsize(db_file) == 0:
        os.remove(db_file)
        print("Removed empty database file")
    
    # Download database if it doesn't exist
    db_url = "https://storage.googleapis.com/benchmarks-artifacts/travel-db/travel2.sqlite"
    if not os.path.exists(db_file):
        print(f"Downloading database from {db_url}...")
        try:
            response = requests.get(db_url)
            response.raise_for_status()
            with open(db_file, "wb") as f:
                f.write(response.content)
            print(f"Database downloaded successfully: {os.path.getsize(db_file)} bytes")
        except Exception as e:
            print(f"Error downloading database: {e}")
            sys.exit(1)
    else:
        print(f"Database already exists: {os.path.getsize(db_file)} bytes")
    
    # Update dates in the database
    try:
        update_dates(db_file)
        print("Database dates updated successfully")
    except Exception as e:
        print(f"Error updating database dates: {e}")
        sys.exit(1)
    
    # Verify database tables
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        print(f"Database tables: {[table[0] for table in tables]}")
        conn.close()
    except Exception as e:
        print(f"Error verifying database tables: {e}")
        sys.exit(1)

def update_dates(db_file):
    """Update dates in the database to be current."""
    backup_file = db_file + '.backup'
    if not os.path.exists(backup_file):
        shutil.copy(db_file, backup_file)
        print(f"Created backup at {backup_file}")

    conn = sqlite3.connect(db_file)

    tables = pd.read_sql(
        "SELECT name FROM sqlite_master WHERE type='table';", conn
    ).name.tolist()
    print(f"Updating dates for tables: {tables}")
    
    tdf = {}
    for t in tables:
        tdf[t] = pd.read_sql(f"SELECT * from {t}", conn)

    # Check if flights table exists and has data
    if "flights" in tdf and not tdf["flights"].empty and "actual_departure" in tdf["flights"].columns:
        example_time = pd.to_datetime(
            tdf["flights"]["actual_departure"].replace("\\N", pd.NaT)
        ).max()
        current_time = pd.to_datetime("now").tz_localize(example_time.tz)
        time_diff = current_time - example_time

        # Update bookings dates if table exists
        if "bookings" in tdf and "book_date" in tdf["bookings"].columns:
            tdf["bookings"]["book_date"] = (
                pd.to_datetime(tdf["bookings"]["book_date"].replace("\\N", pd.NaT), utc=True)
                + time_diff
            )

        # Update flights dates
        datetime_columns = [
            "scheduled_departure",
            "scheduled_arrival",
            "actual_departure",
            "actual_arrival",
        ]
        for column in datetime_columns:
            if column in tdf["flights"].columns:
                tdf["flights"][column] = (
                    pd.to_datetime(tdf["flights"][column].replace("\\N", pd.NaT)) + time_diff
                )

        # Save all tables back to database
        for table_name, df in tdf.items():
            df.to_sql(table_name, conn, if_exists="replace", index=False)
            print(f"Updated table: {table_name}")

    conn.commit()
    conn.close()

if __name__ == "__main__":
    print("Starting database preparation...")
    download_and_prepare_db()
    print("Database preparation completed successfully!")