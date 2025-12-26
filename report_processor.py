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
    if not os.path.exists(db_path):
        return None
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error as e:
        print(f"❌ DB Error: {e}")
        return None

def fetch_citation_details(citation_id: str) -> Dict[str, Any]:
    prefix_match = re.match(r"(R|YT|AS|GP)_", citation_id)
    if not prefix_match:
        return {"id": citation_id, "comment_text": "Not found", "comment_url": "#"}

    platform_key = prefix_match.group(1)
    config = DB_CONFIG.get(platform_key)
    conn = get_db_connection(config["db_path"])
    
    if not conn:
        return {"id": citation_id, "comment_text": "Data source unavailable", "comment_url": "#"}

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
            if platform_key == "R":
                p_cursor = conn.execute(f"SELECT post_url FROM reddit_posts WHERE post_id = '{result['post_id']}'")
                p_row = p_cursor.fetchone()
                if p_row: url = f"https://www.reddit.com{p_row['post_url']}"
            elif platform_key == "YT":
                url = f"https://www.youtube.com/watch?v={result['video_id']}&lc={citation_id.split('_')[-1]}"
            
            date_val = str(result.get('date')).split(' ')[0] if result.get('date') else None
            
            return {
                "id": citation_id,
                "comment_text": result.get('comment_text', 'N/A'),
                "comment_url": url,
                "source_platform": config['platform_name'],
                "date": date_val
            }
    except Exception as e:
        print(f"❌ Fetch Error: {e}")
    finally:
        if conn: conn.close()

    return {"id": citation_id, "comment_text": "Source not found.", "comment_url": "#"}

def parse_and_enrich_report(raw_text: str) -> List[Dict[str, Any]]:
    """
    Parses short bullet points and enriches them with citations.
    """
    raw_text = raw_text.strip()
    parsed_report = []
    citation_cache: Dict[str, Dict[str, Any]] = {}

    # Look for bullet-point lines containing citation brackets
    pattern = re.compile(r'(.*?)\s*\[\[([^\]]+)\]\]', re.DOTALL)
    
    for match in pattern.finditer(raw_text):
        insight_text = match.group(1).strip().replace("\n", " ")
        raw_ids = match.group(2).strip()
        
        citation_ids = [id.strip() for id in raw_ids.split(',') if id.strip()]
        citations = []
        
        for cid in citation_ids:
            if cid not in citation_cache:
                citation_cache[cid] = fetch_citation_details(cid)
            citations.append(citation_cache[cid])
            
        parsed_report.append({
            "insight": insight_text,
            "citations": citations
        })

    return parsed_report

def main():
    if not os.path.exists(INPUT_FILE_PATH):
        print(f"❌ Missing: {INPUT_FILE_PATH}")
        return

    with open(INPUT_FILE_PATH, 'r', encoding='utf-8') as f:
         raw_text = f.read()

    final_report_data = parse_and_enrich_report(raw_text)
    
    with open(OUTPUT_FILENAME, 'w', encoding='utf-8') as f:
        json.dump(final_report_data, f, indent=4)
    print(f"✅ Enhanced bullet-point report saved to {OUTPUT_FILENAME}")

if __name__ == "__main__":
    main()