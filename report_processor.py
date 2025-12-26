import re
import json
import sqlite3
import datetime
import os
from typing import List, Dict, Optional, Any

# --- PERSISTENCE CONFIGURATION ---
DATA_BASE_DIR = os.environ.get("PERSISTENT_STORAGE_PATH", "data")
os.makedirs(DATA_BASE_DIR, exist_ok=True)

INPUT_FILE_PATH = os.path.join(DATA_BASE_DIR, "llm_analysis_output.txt") 
OUTPUT_FILENAME = os.path.join(DATA_BASE_DIR, "report_with_sources.json")

DB_CONFIG = {
    "R": {"db_path": os.path.join(DATA_BASE_DIR, "reddit_data.db"), "platform_name": "Reddit"},
    "YT": {"db_path": os.path.join(DATA_BASE_DIR, "youtube_comments.db"), "platform_name": "Youtube"},
    "AS": {"db_path": os.path.join(DATA_BASE_DIR, "app_reviews.db"), "platform_name": "App Store"},
    "GP": {
        "db_path": os.path.join(DATA_BASE_DIR, "google_play_reviews.db"), 
        "platform_name": "Google Play",
        "table": "google_play_reviews",
        "id_col": "review_id",
        "text_col": "review_text",
        "url_col": "url",
        "date_col": "review_date"
    },
}

def get_db_connection(db_path: str) -> Optional[sqlite3.Connection]:
    """Establishes a connection to the SQLite database."""
    if not os.path.exists(db_path):
        return None
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row 
        return conn
    except sqlite3.Error as e:
        print(f"Error connecting to {db_path}: {e}")
        return None

def fetch_citation_details(citation_id: str) -> Dict[str, Any]:
    """
    Fetches the full comment text and metadata for a given citation ID.
    Constructs direct links for Reddit and YouTube comments.
    """
    prefix_match = re.match(r"(R|YT|AS|GP)_", citation_id)
    if not prefix_match:
        return {"id": citation_id, "comment_text": "Source not found", "comment_url": "#"}

    platform_key = prefix_match.group(1)
    config = DB_CONFIG.get(platform_key)
    conn = get_db_connection(config["db_path"])
    
    if not conn:
        return {"id": citation_id, "comment_text": "DB connection failed", "comment_url": "#"}

    sql_query = ""
    if platform_key == "R":
        db_id = citation_id.split(":")[-1]
        sql_query = f"SELECT body AS comment_text, created_utc AS date, post_id FROM reddit_comments WHERE comment_id = '{db_id}'"
    elif platform_key == "YT":
        db_id = citation_id.split("_", 1)[1]
        sql_query = f"SELECT text_display AS comment_text, published_at AS date, video_id FROM youtube_comments WHERE comment_id = '{db_id}'"
    elif platform_key == "AS":
        db_id = citation_id.split("_")[-1] 
        sql_query = f'SELECT "Review Text" AS comment_text, "Review Date" AS date, "Review URL" AS comment_url FROM economist_reviews WHERE "Review ID" = {db_id}'
    elif platform_key == "GP":
        db_id = citation_id.split("_", 1)[1]
        sql_query = f"SELECT {config['text_col']} AS comment_text, {config['date_col']} AS date, {config['url_col']} AS comment_url FROM {config['table']} WHERE {config['id_col']} = '{db_id}'"

    try:
        cursor = conn.execute(sql_query)
        row = cursor.fetchone()
        if row:
            result = dict(row)
            url = result.get('comment_url', '#')
            
            # Direct link to Reddit comment
            if platform_key == "R":
                post_id = result.get('post_id')
                comment_id = citation_id.split(":")[-1]
                url = f"https://www.reddit.com/comments/{post_id}/_/{comment_id}/"
            
            # Direct link to YouTube comment
            elif platform_key == "YT":
                raw_comment_id = citation_id.split('_')[-1] 
                url = f"https://www.youtube.com/watch?v={result['video_id']}&lc={raw_comment_id}"
            
            date_val = result.get('date')
            if platform_key == "R" and isinstance(date_val, (int, float)):
                 date_val = datetime.datetime.fromtimestamp(date_val).strftime('%Y-%m-%d')
            else:
                 date_val = str(date_val).split(' ')[0] if date_val else None
            
            return {
                "id": citation_id,
                "comment_text": result.get('comment_text', 'N/A'),
                "comment_url": url,
                "source_platform": config['platform_name'],
                "date": date_val
            }
    except Exception as e:
        print(f"SQL Error: {e}")
    finally:
        conn.close()
    return {"id": citation_id, "comment_text": "Source not found in database", "comment_url": "#"}

def parse_and_enrich_report(raw_text: str) -> List[Dict[str, Any]]:
    """
    Splits the report into paragraphs and extracts citations from each.
    Removes citation tags from the insight text for a clean dashboard display.
    """
    parsed_report = []
    # Split by double newlines as per prompt instructions
    paragraphs = [para.strip() for para in raw_text.split('\n\n') if para.strip()]

    for p in paragraphs:
        # Find all citation blocks [[...]] in this paragraph
        citation_matches = re.findall(r"\[\[(.*?)\]\]", p)
        
        # Gather all unique IDs from all blocks in this paragraph
        citation_ids = []
        for match in citation_matches:
            ids = [cid.strip() for cid in match.split(',') if cid.strip()]
            citation_ids.extend(ids)
        
        # Remove all [[...]] tags from the text to be displayed in the scroller
        insight_text = re.sub(r"\s*\[\[.*?\]\]", "", p).strip()
        
        # Fetch details for the unique set of IDs
        citations_data = [fetch_citation_details(cid) for cid in sorted(set(citation_ids))]

        parsed_report.append({
            "insight": insight_text,
            "citations": citations_data
        })

    return parsed_report

def main():
    print("--- Starting Report Generator ---")
    if not os.path.exists(INPUT_FILE_PATH):
        print(f"❌ Error: {INPUT_FILE_PATH} not found.")
        return

    with open(INPUT_FILE_PATH, 'r', encoding='utf-8') as f:
         raw_text = f.read()

    final_report_data = parse_and_enrich_report(raw_text)
    
    if not final_report_data:
        print("Error: No insights were parsed.")
        return

    with open(OUTPUT_FILENAME, 'w', encoding='utf-8') as f:
        json.dump(final_report_data, f, indent=4)
    print(f"✅ Success: Report data saved to {OUTPUT_FILENAME}")

if __name__ == "__main__":
    main()