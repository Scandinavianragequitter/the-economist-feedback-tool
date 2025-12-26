import sqlite3
import os
import datetime
from google_play_scraper import Sort, reviews_all

# --- Configuration ---
APP_ID = 'com.economist.lamarr' 
DATA_DIR = os.environ.get("PERSISTENT_STORAGE_PATH", "data")
DB_FILE = os.path.join(DATA_DIR, 'google_play_reviews.db')
TABLE_NAME = 'google_play_reviews'
DAYS_TO_FETCH = 30 

def initialize_db(db_path):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} (review_id TEXT PRIMARY KEY, user_name TEXT, review_date TEXT, review_text TEXT, rating INTEGER, device TEXT, url TEXT)")
    conn.commit()
    conn.close()

def fetch_and_store_reviews():
    initialize_db(DB_FILE)
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    threshold = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=DAYS_TO_FETCH)
    try:
        all_reviews = reviews_all(APP_ID, lang='en', country='us', sort=Sort.NEWEST)
    except: return
    for r in all_reviews:
        review_date_utc = r['at'].replace(tzinfo=datetime.timezone.utc)
        if review_date_utc < threshold: break 
        data = (r['reviewId'], r['userName'], review_date_utc.strftime('%Y-%m-%d %H:%M:%S'), r['content'], r['score'], r.get('userDevice', 'N/A'), f"https://play.google.com/store/apps/details?id={APP_ID}")
        cursor.execute(f"INSERT OR IGNORE INTO {TABLE_NAME} VALUES (?, ?, ?, ?, ?, ?, ?)", data)
    conn.commit()
    conn.close()

if __name__ == '__main__':
    fetch_and_store_reviews()