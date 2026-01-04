import re
import json
import sqlite3
import datetime
import os
from typing import List, Dict, Optional, Any

DATA_BASE_DIR = os.environ.get("PERSISTENT_STORAGE_PATH", "data")
INPUT_FILE_PATH = os.path.join(DATA_BASE_DIR, "llm_analysis_output.txt") 
OUTPUT_FILENAME = os.path.join(DATA_BASE_DIR, "report_with_sources.json")

DB_CONFIG = {
    "R": {"db_path": os.path.join(DATA_BASE_DIR, "reddit_data.db"), "platform_name": "Reddit"},
    "YT": {"db_path": os.path.join(DATA_BASE_DIR, "youtube_comments.db"), "platform_name": "Youtube"},
    "AS": {"db_path": os.path.join(DATA_BASE_DIR, "app_reviews.db"), "platform_name": "App Store"},
    "GP": {"db_path": os.path.join(DATA_BASE_DIR, "google_play_reviews.db"), "platform_name": "Google Play", "table": "google_play_reviews", "id_col": "review_id", "text_col": "review_text", "url_col": "url", "date_col": "review_date"},
}

def get_db_connection(db_path: str) -> Optional[sqlite3.Connection]:
    if not os.path.exists(db_path): return None
    try:
        conn = sqlite3.connect(db_path); conn.row_factory = sqlite3.Row; return conn
    except: return None

def fetch_citation_details(citation_id: str) -> Dict[str, Any]:
    prefix_match = re.match(r"(R|YT|AS|GP)_", citation_id)
    if not prefix_match: 
        return {"id": citation_id, "comment_text": "Not found", "comment_url": "#", "source_platform": "Unknown", "date": "Recent"}
    
    platform_key = prefix_match.group(1)
    config = DB_CONFIG.get(platform_key)
    conn = get_db_connection(config["db_path"])
    
    if not conn: 
        return {"id": citation_id, "comment_text": "DB missing", "comment_url": "#", "source_platform": config['platform_name'], "date": "Recent"}
    
    sql_query = ""
    db_id_part = citation_id.split("_", 1)[1] if "_" in citation_id else citation_id
    
    if platform_key == "R":
        db_id = citation_id.split(":")[-1]
        sql_query = f"SELECT body AS comment_text, created_utc AS date, 'https://reddit.com/comments/' || post_id || '/_/' || comment_id AS comment_url FROM reddit_comments WHERE comment_id = '{db_id}'"
    elif platform_key == "YT":
        sql_query = f"SELECT text_display AS comment_text, published_at AS date, 'https://youtube.com/watch?v=' || video_id AS comment_url FROM youtube_comments WHERE comment_id = '{db_id_part}'"
    elif platform_key == "AS":
        sql_query = f'SELECT "Review Text" AS comment_text, "Review Date" AS date, "Review URL" AS comment_url FROM economist_reviews WHERE "Review ID" = {db_id_part.split("_")[-1]}'
    elif platform_key == "GP":
        sql_query = f"SELECT {config['text_col']} AS comment_text, {config['date_col']} AS date, {config['url_col']} AS comment_url FROM {config['table']} WHERE {config['id_col']} = '{db_id_part}'"

    try:
        cursor = conn.execute(sql_query); row = cursor.fetchone()
        if row:
            result = dict(row)
            raw_date = result.get('date')
            formatted_date = "Recent"
            
            try:
                # CHANGE 2: Robust Unix timestamp check (handles decimals)
                val_str = str(raw_date).strip()
                if val_str and val_str.replace('.', '', 1).isdigit():
                    formatted_date = datetime.datetime.fromtimestamp(float(val_str)).strftime('%Y-%m-%d')
                else:
                    formatted_date = val_str.split(' ')[0] if val_str else "Recent"
            except: 
                formatted_date = "Recent"
            
            return {
                "id": citation_id, 
                "comment_text": result.get('comment_text', 'N/A'), 
                "comment_url": result.get('comment_url', '#'), 
                "source_platform": config['platform_name'], 
                "date": formatted_date
            }
    except: pass
    finally:
        if conn: conn.close()
    
    # CHANGE 3: Expanded fallback dictionary keys
    return {"id": citation_id, "comment_text": "Not found", "comment_url": "#", "source_platform": config['platform_name'], "date": "Recent"}

def parse_report(raw_text):
    paragraphs = [p.strip() for p in raw_text.split('\n\n') if p.strip()]
    parsed = []
    for p in paragraphs:
        citation_matches = re.findall(r"\[\[(.*?)\]\]", p)
        ids = []
        for match in citation_matches: ids.extend([cid.strip() for cid in match.split(',')])
        clean_text = re.sub(r"\[\[.*?\]\]", "", p).strip()
        
        if ":" in clean_text[:25]:
            topic_part, insight_part = clean_text.split(":", 1)
            topic = topic_part.strip().upper()
            insight = insight_part.strip()
        else:
            topic = "GENERAL"; insight = clean_text

        parsed.append({
            "topic": topic,
            "insight": insight,
            "citations": [fetch_citation_details(cid) for cid in sorted(list(set(ids)))],
            "count": len(set(ids))
        })
    return parsed

def main():
    if not os.path.exists(INPUT_FILE_PATH): return
    with open(INPUT_FILE_PATH, 'r', encoding='utf-8') as f:
        data = parse_report(f.read())
    with open(OUTPUT_FILENAME, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)

if __name__ == "__main__":
    main()