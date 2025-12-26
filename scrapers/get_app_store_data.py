import pandas as pd
import sqlite3
from app_store_web_scraper import AppStoreEntry, AppStoreSession
from datetime import datetime, timedelta
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DATA_DIR = os.environ.get("PERSISTENT_STORAGE_PATH", "data")
DB_NAME = os.path.join(DATA_DIR, "app_reviews.db")
TABLE_NAME = "economist_reviews"

def scrape_and_filter_reviews(app_name, app_id, country_code='us', days_to_look_back=30, max_reviews=5000):
    os.makedirs(os.path.dirname(DB_NAME), exist_ok=True)
    cutoff_date = datetime.now() - timedelta(days=days_to_look_back)
    all_reviews = []
    base_url = f"https://apps.apple.com/{country_code}/app/{app_name.lower().replace(' ', '-')}/id{app_id}"
    try:
        session = AppStoreSession(delay=5, delay_jitter=2)
        app_entry = AppStoreEntry(app_id=app_id, country=country_code, session=session)
        for review in app_entry.reviews():
            if len(all_reviews) >= max_reviews: break
            all_reviews.append({
                'Review Date': review.date, 'User Name': review.user_name, 'Rating': review.rating, 'Review Title': review.title, 'Review Text': review.content,
                'Review URL': f"{base_url}?see-all=reviews&id={app_id}#review/{review.id}", 'Review ID': review.id, 'version': 'N/A' 
            })
    except: return
    if not all_reviews: return
    df = pd.DataFrame(all_reviews)
    df['Review Date'] = pd.to_datetime(df['Review Date']).dt.tz_localize(None)
    recent_df = df[df['Review Date'] >= cutoff_date]
    try:
        conn = sqlite3.connect(DB_NAME)
        recent_df.to_sql(TABLE_NAME, conn, if_exists='replace', index=False)
        conn.close()
    except: pass

if __name__ == "__main__":
    scrape_and_filter_reviews(app_name="The Economist", app_id="1239397626")