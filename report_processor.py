import re
import json
import sqlite3
import datetime
import os
from typing import List, Dict, Optional, Any

# --- CONFIGURATION (PATHS) ---
# Ensure these paths match your project structure
INPUT_FILE_PATH = "llm_analysis_output.txt" 
DATA_BASE_DIR = "data" 
OUTPUT_FILENAME = "report_with_sources.json"

# Database configuration: Paths point to the 'data/' folder
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

# --- UTILITIES ---

def get_db_connection(db_path: str) -> Optional[sqlite3.Connection]:
    """Establishes a connection to the SQLite database."""
    if not os.path.exists(db_path):
        print(f"❌ Database file not found at: {db_path}")
        return None
        
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row  # Access columns by name
        return conn
    except sqlite3.Error as e:
        print(f"❌ Database connection error for {db_path}: {e}")
        return None

def fetch_citation_details(citation_id: str) -> Dict[str, Any]:
    """
    Fetches the full comment text and metadata for a given citation ID based on platform schema.
    This version correctly handles prefix stripping for YouTube and Reddit URL lookups.
    """
    
    # 1. Determine platform and config
    prefix_match = re.match(r"(R|YT|AS|GP)_", citation_id)
    if not prefix_match:
        return {"id": citation_id, "comment_text": "Source not found (Invalid ID format)", "comment_url": "#", "source_platform": "Unknown", "date": None}

    platform_key = prefix_match.group(1)
    config = DB_CONFIG.get(platform_key)
    
    conn = get_db_connection(config["db_path"])
    
    if not conn:
        return {"id": citation_id, "comment_text": "Source not found (DB connection failed).", "comment_url": "#", "source_platform": config['platform_name'], "date": None}

    sql_query = ""
    
    # 2. Build platform-specific query
    if platform_key == "R":
        # Format: R_[post_id]:[comment_id]
        db_id = citation_id.split(":")[-1]
        sql_query = f"""
            SELECT 
                T1.body AS comment_text,
                T1.created_utc AS date,
                T1.post_id
            FROM reddit_comments AS T1
            WHERE T1.comment_id = '{db_id}'
            LIMIT 1
        """
    elif platform_key == "YT":
        # FIX: Strip the "YT_" prefix to query the raw database ID
        db_id = citation_id.split("_", 1)[1]
        sql_query = f"""
            SELECT 
                text_display AS comment_text,
                published_at AS date,
                video_id
            FROM youtube_comments
            WHERE comment_id = '{db_id}'
            LIMIT 1
        """
    elif platform_key == "AS":
        # Format: AS_[Review ID]
        db_id = citation_id.split("_")[-1] 
        sql_query = f"""
            SELECT 
                "Review Text" AS comment_text,
                "Review Date" AS date,
                "Review URL" AS comment_url
            FROM economist_reviews
            WHERE "Review ID" = {db_id}
            LIMIT 1
        """
    elif platform_key == "GP":
        # Format: GP_[review_id]
        db_id = citation_id.split("_", 1)[1]
        sql_query = f"""
            SELECT 
                {config['text_col']} AS comment_text,
                {config['date_col']} AS date,
                {config['url_col']} AS comment_url
            FROM {config['table']}
            WHERE {config['id_col']} = '{db_id}'
            LIMIT 1
        """

    # 3. Execute query and process result
    try:
        cursor = conn.execute(sql_query)
        row = cursor.fetchone()

        if row:
            result = dict(row)
            
            # --- URL Formatting ---
            url = result.get('comment_url', '#')
            if platform_key == "R":
                # For Reddit, we must look up the post_url from the posts table
                post_id = result.get('post_id')
                p_cursor = conn.execute(f"SELECT post_url FROM reddit_posts WHERE post_id = '{post_id}'")
                p_row = p_cursor.fetchone()
                if p_row:
                    url = f"https://www.reddit.com{p_row['post_url']}"
            elif platform_key == "YT":
                # Deep link directly to the specific comment
                raw_comment_id = citation_id.split('_')[-1] 
                url = f"https://www.youtube.com/watch?v={result['video_id']}&lc={raw_comment_id}"
            elif not url:
                url = "#"
            
            # --- Date Formatting ---
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
        
    except sqlite3.Error as e:
        print(f"❌ SQL Error for ID {citation_id}: {e}")
    finally:
        if conn:
            conn.close()

    return {"id": citation_id, "comment_text": "Source not found in database.", "comment_url": "#", "source_platform": config['platform_name'], "date": None}


def parse_and_enrich_report(raw_text: str) -> List[Dict[str, Any]]:
    """
    Parses the LLM output to find all cited blocks and treat text in between as insights.
    """
    raw_text = raw_text.strip()
    parsed_report = []
    citation_cache: Dict[str, Dict[str, Any]] = {}

    # Pattern to find insight text followed by [[IDs]]
    pattern = re.compile(r'(.*?)\s*\[\[([^\]]+)\]\]', re.DOTALL)
    last_end = 0

    for match in pattern.finditer(raw_text):
        # 1. Handle uncited text (e.g., headers)
        if match.start() > last_end:
            uncited_text = raw_text[last_end:match.start()].strip()
            if uncited_text:
                 for block in [b.strip() for b in uncited_text.split('\n\n') if b.strip()]:
                     parsed_report.append({"insight": block, "citations": []})
        
        # 2. Process cited insight
        insight_text_raw = match.group(1).strip()
        raw_ids_block = match.group(2).strip()
        
        citation_ids = [id.strip() for id in raw_ids_block.split(',') if id.strip()]
        citations_for_this_insight = []
        
        for citation_id in citation_ids:
            if citation_id not in citation_cache:
                details = fetch_citation_details(citation_id)
                citation_cache[citation_id] = details
            citations_for_this_insight.append(citation_cache[citation_id])
            
        parsed_report.append({
            "insight": insight_text_raw,
            "citations": citations_for_this_insight
        })
        last_end = match.end()

    # 3. Handle remaining text
    remaining_text = raw_text[last_end:].strip()
    if remaining_text:
        for block in [b.strip() for b in remaining_text.split('\n\n') if b.strip()]:
            parsed_report.append({"insight": block, "citations": []})

    return parsed_report

def main():
    """Main function to run the report generation process."""
    print("--- Starting Report Generator ---")
    
    if not os.path.exists(DATA_BASE_DIR):
        os.makedirs(DATA_BASE_DIR)
    
    try:
        with open(INPUT_FILE_PATH, 'r', encoding='utf-8') as f:
             raw_text = f.read()
        print(f"✅ Successfully read input from {INPUT_FILE_PATH}.")
    except Exception as e:
        print(f"❌ Error reading input: {e}")
        return

    final_report_data = parse_and_enrich_report(raw_text)
    
    if not final_report_data:
        print("Error: No insights were parsed.")
        return

    try:
        with open(OUTPUT_FILENAME, 'w', encoding='utf-8') as f:
            json.dump(final_report_data, f, indent=4)
        print(f"\n✅ Success: Report data saved to {OUTPUT_FILENAME}")
    except IOError as e:
        print(f"\n❌ Error writing file: {e}")

if __name__ == "__main__":
    main()