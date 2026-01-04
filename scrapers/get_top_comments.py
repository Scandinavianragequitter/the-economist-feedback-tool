import sqlite3
import json
import sys

# ==============================================================================
# CONFIGURATION
# ==============================================================================
REDDIT_DB = "reddit_data.db"
YOUTUBE_DB = "youtube_comments.db"
APP_STORE_DB = "app_reviews.db"

# --- Quantitative Limits ---
REDDIT_POST_LIMIT = 200      
YT_COMMENT_LIMIT = 200      
APP_REVIEW_LIMIT = 500      

# --- Output Configuration ---
OUTPUT_FILENAME = "curated_data_for_llm.json" 
# ==============================================================================


def connect_db(db_name):
    """Establishes a connection to a specific database."""
    try:
        conn = sqlite3.connect(db_name)
        return conn
    except sqlite3.OperationalError:
        return None

def get_top_reddit_data(conn):
    """
    Fetches the top N posts and their comments, returning a FLATTENED list of
    only {id, text} for all associated comments.
    """
    if not conn:
        print(f"Skipping Reddit: '{REDDIT_DB}' not found.")
        return []

    cursor = conn.cursor()
    flattened_reddit_comments = []

    # 1. Identify the top N posts by score (upvotes)
    cursor.execute("""
        SELECT post_id
        FROM reddit_posts
        ORDER BY score DESC
        LIMIT ?
    """, (REDDIT_POST_LIMIT,))
    top_post_ids = [p[0] for p in cursor.fetchall()]
    
    if not top_post_ids:
        print("No high-scoring Reddit posts found.")
        return []
    
    # 2. Fetch ALL comments associated with these top N posts
    placeholders = ','.join('?' for _ in top_post_ids)
    
    cursor.execute(f"""
        SELECT comment_id, post_id, body
        FROM reddit_comments
        WHERE post_id IN ({placeholders})
        ORDER BY score DESC
    """, tuple(top_post_ids))
    
    all_comments = cursor.fetchall()
    
    # 3. Restructure: Flatten and extract ONLY ID and body
    for comment_id, post_id, body in all_comments:
        # Construct the full, verifiable ID here (R_[post_id]:[comment_id])
        full_verifiable_id = f"R_{post_id}:{comment_id}"

        flattened_reddit_comments.append({
            "id": full_verifiable_id,
            "text": body.strip()
        })

    print(f"✅ Extracted and flattened {len(flattened_reddit_comments)} Reddit comments.")
    return flattened_reddit_comments

def get_top_youtube_data(conn):
    """
    Fetches the top N most liked comments from the YouTube database, 
    returning a FLATTENED list of only {id, text}.
    """
    if not conn:
        print(f"Skipping YouTube: '{YOUTUBE_DB}' not found.")
        return []

    cursor = conn.cursor()
    flattened_youtube_comments = []

    # SQL Query: Filters top N comments by like_count
    query = f"""
        SELECT 
            T1.text_display,
            T1.comment_id          
        FROM youtube_comments T1
        ORDER BY T1.like_count DESC
        LIMIT ?
    """
    
    cursor.execute(query, (YT_COMMENT_LIMIT,))
    
    for body, comment_id in cursor.fetchall():
        # Construct the full, verifiable ID here (YT_[comment_id])
        full_verifiable_id = f"YT_{comment_id}"

        flattened_youtube_comments.append({
            "id": full_verifiable_id,
            "text": body.strip()
        })

    print(f"✅ Extracted and flattened {len(flattened_youtube_comments)} top YouTube comments.")
    return flattened_youtube_comments

# --- NEW FUNCTION FOR APP STORE REVIEWS ---
def get_app_store_reviews(conn):
    """
    Fetches recent app store reviews, returning a FLATTENED list of only {id, text}
    (concatenating title and review text).
    """
    if not conn:
        print(f"Skipping App Store: '{APP_STORE_DB}' not found.")
        return []

    cursor = conn.cursor()
    flattened_reviews = []
    
    query = f"""
        SELECT 
            "Review ID", 
            "Review Title", 
            "Review Text"
        FROM economist_reviews
        ORDER BY "Review Date" DESC
        LIMIT ?
    """
    
    cursor.execute(query, (APP_REVIEW_LIMIT,))
    
    for review_id, title, text in cursor.fetchall():
        # CRITICAL: Construct the full, verifiable ID (AS_[review_id])
        full_verifiable_id = f"AS_{review_id}"
        
        # Combine Title and Text into a single body for the LLM
        combined_text = f"{title.strip()}\n\n{text.strip()}"

        flattened_reviews.append({
            "id": full_verifiable_id,
            "text": combined_text
        })

    print(f"✅ Extracted and flattened {len(flattened_reviews)} recent App Store reviews.")
    return flattened_reviews


def main():
    """Main function to combine filtering and output JSON as a single flat list."""
    
    print("--- Starting Final Curation Pipeline ---")
    
    # 1. Connect to all databases
    reddit_conn = connect_db(REDDIT_DB)
    youtube_conn = connect_db(YOUTUBE_DB)
    app_store_conn = connect_db(APP_STORE_DB)
    
    
    if not reddit_conn and not youtube_conn and not app_store_conn:
        print("❌ FATAL: No database files were found. Cannot proceed.")
        sys.exit(1)

    # 2. Get flattened data from all sources (lists of {id, text})
    reddit_data = get_top_reddit_data(reddit_conn)
    youtube_nuggets = get_top_youtube_data(youtube_conn)
    app_store_reviews = get_app_store_reviews(app_store_conn)
    
    # 3. Combine all flattened lists into a single master list
    all_data_for_llm = reddit_data + youtube_nuggets + app_store_reviews
    
    total_items = len(all_data_for_llm)
    print(f"\nTotal curated items ready for LLM: {total_items}")

    # 4. Prepare the final JSON output string (now a list of objects)
    json_output = json.dumps(all_data_for_llm, indent=2)

    # 5. Write the JSON string to the file
    try:
        with open(OUTPUT_FILENAME, 'w', encoding='utf-8') as f:
            f.write(json_output)
        print(f"✅ Success! Data exported to JSON file: {OUTPUT_FILENAME}")
    except Exception as e:
        print(f"❌ Error writing JSON file: {e}")

    # Clean up connections
    if reddit_conn: reddit_conn.close()
    if youtube_conn: youtube_conn.close()
    if app_store_conn: app_store_conn.close()
    print("\nDatabase connections closed. Pipeline finished.")

if __name__ == "__main__":
    main()