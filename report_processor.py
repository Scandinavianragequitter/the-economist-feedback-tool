import re
import json
import sqlite3
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
    if not prefix_match: return {"id": citation_id, "comment_text": "Not found", "comment_url": "#"}
    platform_key = prefix_match.group(1)
    config = DB_CONFIG.get(platform_key)
    conn = get_db_connection(config["db_path"])
    if not conn: return {"id": citation_id, "comment_text": "DB missing", "comment_url": "#"}
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
        cursor = conn.execute(sql_query); row = cursor.fetchone()
        if row:
            result = dict(row); url = result.get('comment_url', '#')
            if platform_key == "R":
                p_row = conn.execute(f"SELECT post_url FROM reddit_posts WHERE post_id = '{result['post_id']}'").fetchone()
                if p_row: url = f"https://www.reddit.com{dict(p_row)['post_url']}"
            elif platform_key == "YT":
                url = f"https://www.youtube.com/watch?v={result['video_id']}&lc={citation_id.split('_')[-1]}"
            return {"id": citation_id, "comment_text": result.get('comment_text', 'N/A'), "comment_url": url, "source_platform": config['platform_name'], "date": str(result.get('date')).split(' ')[0]}
    except: pass
    finally: conn.close()
    return {"id": citation_id, "comment_text": "Not found", "comment_url": "#"}

def parse_and_enrich_report(raw_text: str) -> List[Dict[str, Any]]:
    # Normalize numbering if LLM added it
    if re.search(r"^\d+\.\s", raw_text, re.MULTILINE):
        raw_text = re.sub(r"^\d+\.\s*", "\n\n", raw_text, flags=re.MULTILINE)
    
    paragraphs = [p.strip() for p in raw_text.split('\n\n') if p.strip()]
    parsed_report = []
    
    for p in paragraphs:
        citation_matches = re.findall(r"\[\[(.*?)\]\]", p)
        if not citation_matches: continue
        
        # Aggregate all IDs from potentially multiple citation brackets in one paragraph
        all_ids = []
        for match_str in citation_matches:
            ids = [cid.strip() for cid in match_str.split(',') if cid.strip()]
            all_ids.extend(ids)
        unique_ids = sorted(list(set(all_ids)))

        # Strip only the citation tags, PRESERVE bold markdown
        clean_text = re.sub(r"\s*\[\[.*?\]\]", "", p).strip()
        
        topic, insight = "General", clean_text
        if ":" in clean_text[:30]:
            parts = clean_text.split(":", 1)
            topic, insight = parts[0].strip(), parts[1].strip()
        
        parsed_report.append({
            "topic": topic,
            "insight": insight,
            "citations": [fetch_citation_details(cid) for cid in unique_ids],
            "count": len(unique_ids)
        })
    return parsed_report

def main():
    if not os.path.exists(INPUT_FILE_PATH): return
    with open(INPUT_FILE_PATH, 'r', encoding='utf-8') as f:
         raw_text = f.read()
    final_report_data = parse_and_enrich_report(raw_text)
    with open(OUTPUT_FILENAME, 'w', encoding='utf-8') as f:
        json.dump(final_report_data, f, indent=4)

if __name__ == "__main__":
    main()