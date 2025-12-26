import sqlite3
import json
import sys
import os
from typing import List, Dict, Any, Optional

# ==============================================================================
# CONFIGURATION - UPDATED TO USE RELATIVE PATHS FOR PORTABILITY
# ==============================================================================
# This looks for the 'data' folder in the project directory
DATA_DIR = "data"

REDDIT_DB = os.path.join(DATA_DIR, "reddit_data.db")
YOUTUBE_DB = os.path.join(DATA_DIR, "youtube_comments.db")
APP_STORE_DB = os.path.join(DATA_DIR, "app_reviews.db")
GOOGLE_PLAY_DB = os.path.join(DATA_DIR, "google_play_reviews.db")

# --- Quantitative Limits ---
REDDIT_POST_LIMIT = 200      
YT_COMMENT_LIMIT = 200      
APP_REVIEW_LIMIT = 500      
GP_REVIEW_LIMIT = 500 

# --- Output Configuration ---
OUTPUT_FILENAME = os.path.join(DATA_DIR, "curated_data_for_llm.json") 
# ==============================================================================

def connect_db(db_name: str) -> Optional[sqlite3.Connection]:
    """Establishes a connection to a specific database."""
    if not os.path.exists(db_name):
        return None
    try:
        conn = sqlite3.connect(db_name)
        return conn
    except sqlite3.OperationalError:
        return None

def get_top_reddit_data(conn: Optional[sqlite3.Connection]) -> List[Dict[str, str]]:
    if not conn:
        print(f"Skipping Reddit: '{REDDIT_DB}' not found.")
        return []
    cursor = conn.cursor()
    flattened_reddit_comments = []
    cursor.execute("SELECT post_id FROM reddit_posts ORDER BY score DESC LIMIT ?", (REDDIT_POST_LIMIT,))
    top_post_ids = [p[0] for p in cursor.fetchall()]
    if not top_post_ids: return []
    placeholders = ','.join('?' for _ in top_post_ids)
    cursor.execute(f"SELECT comment_id, post_id, body FROM reddit_comments WHERE post_id IN ({placeholders}) ORDER BY score DESC", tuple(top_post_ids))
    for comment_id, post_id, body in cursor.fetchall():
        flattened_reddit_comments.append({"id": f"R_{post_id}:{comment_id}", "text": body.strip()})
    print(f"✅ Extracted {len(flattened_reddit_comments)} Reddit comments.")
    return flattened_reddit_comments

def get_top_youtube_data(conn: Optional[sqlite3.Connection]) -> List[Dict[str, str]]:
    if not conn:
        print(f"Skipping YouTube: '{YOUTUBE_DB}' not found.")
        return []
    cursor = conn.cursor()
    flattened_youtube_comments = []
    cursor.execute("SELECT text_display, comment_id FROM youtube_comments ORDER BY like_count DESC LIMIT ?", (YT_COMMENT_LIMIT,))
    for body, comment_id in cursor.fetchall():
        flattened_youtube_comments.append({"id": f"YT_{comment_id}", "text": body.strip()})
    print(f"✅ Extracted {len(flattened_youtube_comments)} YouTube comments.")
    return flattened_youtube_comments

def get_app_store_reviews(conn: Optional[sqlite3.Connection]) -> List[Dict[str, str]]:
    if not conn:
        print(f"Skipping App Store: '{APP_STORE_DB}' not found.")
        return []
    cursor = conn.cursor()
    flattened_reviews = []
    cursor.execute('SELECT "Review ID", "Review Title", "Review Text" FROM economist_reviews ORDER BY "Review Date" DESC LIMIT ?', (APP_REVIEW_LIMIT,))
    for review_id, title, text in cursor.fetchall():
        combined_text = f"{(title.strip() if title else '')}\n\n{(text.strip() if text else '')}".strip()
        flattened_reviews.append({"id": f"AS_{review_id}", "text": combined_text})
    print(f"✅ Extracted {len(flattened_reviews)} App Store reviews.")
    return flattened_reviews

def get_google_play_reviews(conn: Optional[sqlite3.Connection]) -> List[Dict[str, str]]:
    if not conn:
        print(f"Skipping Google Play: '{GOOGLE_PLAY_DB}' not found.")
        return []
    cursor = conn.cursor()
    flattened_reviews = []
    cursor.execute("SELECT review_id, review_text, rating FROM google_play_reviews ORDER BY review_date DESC LIMIT ?", (GP_REVIEW_LIMIT,))
    for review_id, text, rating in cursor.fetchall():
        combined_text = f"Rating: {rating}/5\n\n{(text.strip() if text else '')}".strip()
        flattened_reviews.append({"id": f"GP_{review_id}", "text": combined_text})
    print(f"✅ Extracted {len(flattened_reviews)} Google Play reviews.")
    return flattened_reviews

def main():
    print("--- Starting Final Curation Pipeline ---")
    reddit_conn = connect_db(REDDIT_DB)
    youtube_conn = connect_db(YOUTUBE_DB)
    app_store_conn = connect_db(APP_STORE_DB)
    google_play_conn = connect_db(GOOGLE_PLAY_DB)
    
    if not any([reddit_conn, youtube_conn, app_store_conn, google_play_conn]):
        print("❌ FATAL: No database files were found in the 'data/' folder.")
        sys.exit(1)

    all_data_for_llm = (get_top_reddit_data(reddit_conn) + 
                        get_top_youtube_data(youtube_conn) + 
                        get_app_store_reviews(app_store_conn) + 
                        get_google_play_reviews(google_play_conn))
    
    with open(OUTPUT_FILENAME, 'w', encoding='utf-8') as f:
        json.dump(all_data_for_llm, f, indent=2)
    
    print(f"✅ Exported {len(all_data_for_llm)} items to: {OUTPUT_FILENAME}")
    for c in [reddit_conn, youtube_conn, app_store_conn, google_play_conn]:
        if c: c.close()

if __name__ == "__main__":
    main()