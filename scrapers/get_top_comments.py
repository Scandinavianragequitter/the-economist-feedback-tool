import sqlite3
import json
import sys
import os
from typing import List, Dict, Any, Optional

# ==============================================================================
# CONFIGURATION - RENDER PERSISTENT DISK PATHS
# ==============================================================================
DATA_DIR = os.environ.get("PERSISTENT_STORAGE_PATH", "data")

REDDIT_DB = os.path.join(DATA_DIR, "reddit_data.db")
YOUTUBE_DB = os.path.join(DATA_DIR, "youtube_comments.db")
APP_STORE_DB = os.path.join(DATA_DIR, "app_reviews.db")
GOOGLE_PLAY_DB = os.path.join(DATA_DIR, "google_play_reviews.db")

# --- Quantitative Limits ---
REDDIT_POST_LIMIT = 200      
YT_COMMENT_LIMIT = 200      
APP_REVIEW_LIMIT = 500      
GP_REVIEW_LIMIT = 500 

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

def get_top_reddit_data(conn):
    """Fetches top Reddit posts and their comments, including timestamps."""
    if not conn:
        print(f"Skipping Reddit: DB not found.")
        return []

    cursor = conn.cursor()
    flattened_reddit_comments = []

    cursor.execute("""
        SELECT post_id FROM reddit_posts
        ORDER BY score DESC LIMIT ?
    """, (REDDIT_POST_LIMIT,))
    top_post_ids = [p[0] for p in cursor.fetchall()]
    
    if not top_post_ids:
        return []
    
    placeholders = ','.join('?' for _ in top_post_ids)
    # CHANGE 1: Select created_utc column
    cursor.execute(f"""
        SELECT comment_id, post_id, body, created_utc
        FROM reddit_comments
        WHERE post_id IN ({placeholders})
        ORDER BY score DESC
    """, tuple(top_post_ids))
    
    for comment_id, post_id, body, created_utc in cursor.fetchall():
        full_verifiable_id = f"R_{post_id}:{comment_id}"
        flattened_reddit_comments.append({
            "id": full_verifiable_id,
            "text": body.strip(),
            "date": created_utc # Pass date to intermediate JSON
        })

    print(f"✅ Extracted {len(flattened_reddit_comments)} Reddit comments.")
    return flattened_reddit_comments

def get_top_youtube_data(conn):
    if not conn: return []
    cursor = conn.cursor()
    flattened_youtube_comments = []
    cursor.execute("SELECT text_display, comment_id FROM youtube_comments ORDER BY like_count DESC LIMIT ?", (YT_COMMENT_LIMIT,))
    for body, cid in cursor.fetchall():
        flattened_youtube_comments.append({"id": f"YT_{cid}", "text": body.strip()})
    print(f"✅ Extracted {len(flattened_youtube_comments)} Youtube comments.")
    return flattened_youtube_comments

def get_app_store_reviews(conn):
    if not conn: return []
    cursor = conn.cursor()
    flattened_reviews = []
    cursor.execute('SELECT "Review ID", "Review Title", "Review Text" FROM economist_reviews ORDER BY "Review Date" DESC LIMIT ?', (APP_REVIEW_LIMIT,))
    for rid, title, text in cursor.fetchall():
        flattened_reviews.append({"id": f"AS_{rid}", "text": f"{title}\n\n{text}".strip()})
    print(f"✅ Extracted {len(flattened_reviews)} App Store reviews.")
    return flattened_reviews

def get_google_play_reviews(conn):
    if not conn: return []
    cursor = conn.cursor()
    flattened_reviews = []
    cursor.execute("SELECT review_id, review_text, score FROM google_play_reviews ORDER BY review_date DESC LIMIT ?", (GP_REVIEW_LIMIT,))
    for rid, text, rating in cursor.fetchall():
        combined_text = f"Rating: {rating}/5\n\n{(text.strip() if text else '')}".strip()
        flattened_reviews.append({"id": f"GP_{rid}", "text": combined_text})
    print(f"✅ Extracted {len(flattened_reviews)} Google Play reviews.")
    return flattened_reviews

def main():
    print("--- Starting Final Curation Pipeline ---")
    os.makedirs(DATA_DIR, exist_ok=True)
    
    reddit_conn = connect_db(REDDIT_DB)
    youtube_conn = connect_db(YOUTUBE_DB)
    app_store_conn = connect_db(APP_STORE_DB)
    google_play_conn = connect_db(GOOGLE_PLAY_DB)
    
    if not any([reddit_conn, youtube_conn, app_store_conn, google_play_conn]):
        print(f"❌ FATAL: No database files found in: {DATA_DIR}")
        sys.exit(1)

    all_data = (get_top_reddit_data(reddit_conn) + 
                get_top_youtube_data(youtube_conn) + 
                get_app_store_reviews(app_store_conn) + 
                get_google_play_reviews(google_play_conn))
    
    with open(OUTPUT_FILENAME, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, indent=2)
    
    for c in [reddit_conn, youtube_conn, app_store_conn, google_play_conn]:
        if c: c.close()
    print(f"✅ Pipeline finished. Exported to {OUTPUT_FILENAME}")

if __name__ == "__main__":
    main()