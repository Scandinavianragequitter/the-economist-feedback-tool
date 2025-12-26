import os
import json
import requests
import sqlite3
import re
import datetime
import time
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS 
from typing import Optional, List, Dict, Any 
import logging
from dotenv import load_dotenv

# --- Load Environment Variables ---
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration ---
DATA_DIR = os.environ.get("PERSISTENT_STORAGE_PATH", "data")
API_KEY = os.environ.get("OPENROUTER_API_KEY") 
LARGE_CONTEXT_MODEL = "x-ai/grok-4.1-fast" 
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

os.makedirs(DATA_DIR, exist_ok=True)

# --- Database Paths ---
REDDIT_DB = os.path.join(DATA_DIR, "reddit_data.db")
YOUTUBE_DB = os.path.join(DATA_DIR, "youtube_comments.db")
APP_STORE_DB = os.path.join(DATA_DIR, "app_reviews.db")
GOOGLE_PLAY_DB = os.path.join(DATA_DIR, "google_play_reviews.db")

# --- Database Schemas ---
DB_SCHEMAS = {
    "Reddit": {
        "db": REDDIT_DB,
        "table": "reddit_comments",
        "text_col": "body",
        "id_col_db": "comment_id",
        "date_col": "created_utc",
        "prefix": "R_",
    },
    "YouTube": {
        "db": YOUTUBE_DB,
        "table": "youtube_comments",
        "text_col": "text_display",
        "id_col_db": "comment_id",
        "date_col": "published_at",
        "prefix": "YT_",
    },
    "AppStore": {
        "db": APP_STORE_DB,
        "table": "economist_reviews",
        "text_col": '"Review Text"',
        "id_col_db": '"Review ID"',
        "date_col": '"Review Date"',
        "prefix": "AS_",
    },
    "GooglePlay": {
        "db": GOOGLE_PLAY_DB,
        "table": "google_play_reviews",
        "text_col": "review_text",
        "id_col_db": "review_id",
        "date_col": "review_date",
        "prefix": "GP_",
    },
}

# ====================================================================
# HELPER FUNCTIONS
# ====================================================================

def get_db_connection(db_path):
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.OperationalError as e:
        logging.error(f"[DB CONNECTION ERROR] {db_path}: {e}")
        return None

def call_llm_api_large_context(messages: List[Dict], model: str) -> Optional[str]:
    headers = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}
    payload = {"model": model, "messages": messages, "temperature": 0.0, "top_p": 1}
    try:
        response = requests.post(OPENROUTER_URL, headers=headers, json=payload, timeout=180)
        response.raise_for_status()
        content = response.json()['choices'][0]['message']['content']
        content = re.sub(r'<think>.*?</think>', '', content, flags=re.DOTALL).strip()
        content = content.replace('```json', '').replace('```', '').strip()
        return content
    except Exception as e:
        logging.error(f"❌ LLM API Error: {e}")
        return None

def fetch_entire_dataset(platforms: Optional[List[str]] = None, time_period: str = "all") -> List[Dict]:
    """
    Aggregates text data with platform and time-frame filtering.
    """
    all_data = []
    target_platforms = platforms if platforms else list(DB_SCHEMAS.keys())
    
    # Calculate cutoff
    now = datetime.datetime.now(datetime.timezone.utc)
    cutoff_dt = None
    if time_period == "day": cutoff_dt = now - datetime.timedelta(days=1)
    elif time_period == "week": cutoff_dt = now - datetime.timedelta(days=7)
    elif time_period == "month": cutoff_dt = now - datetime.timedelta(days=30)

    for platform in target_platforms:
        if platform not in DB_SCHEMAS: continue
        config = DB_SCHEMAS[platform]
        if not os.path.exists(config['db']): continue
        
        conn = get_db_connection(config['db'])
        if not conn: continue
        
        try:
            query = f"SELECT {config['id_col_db']} as id, {config['text_col']} as text, {config['date_col']} as date FROM {config['table']} WHERE text IS NOT NULL AND text != ''"
            cursor = conn.cursor()
            cursor.execute(query)
            
            for row in cursor.fetchall():
                row_date = row['date']
                is_valid = True
                
                # Apply Time Filtering
                if cutoff_dt:
                    try:
                        if platform == "Reddit":
                            # Reddit uses Unix Timestamps
                            dt = datetime.datetime.fromtimestamp(float(row_date), tz=datetime.timezone.utc)
                        else:
                            # Others use ISO strings
                            dt = datetime.datetime.fromisoformat(str(row_date).replace('Z', '+00:00'))
                        
                        if dt < cutoff_dt: is_valid = False
                    except Exception:
                        pass # If date parsing fails, keep the record
                
                if is_valid:
                    all_data.append({
                        "id": f"{config['prefix']}{row['id']}",
                        "t": str(row['text'])[:1000]
                    })
        except Exception as e:
            logging.warning(f"Error reading {platform}: {e}")
        finally:
            conn.close()
    return all_data

def llm_scan_full_dataset(user_prompt: str, dataset: List[Dict]) -> List[str]:
    data_str = "\n".join([f"{d['id']}|{d['t']}" for d in dataset])
    system_prompt = (
        "You are a Semantic Search Engine. "
        "I will provide a dataset of comments in the format: `ID|Text`.\n"
        "Your Task:\n"
        "1. Identify comments that match the User's Query.\n"
        "2. Return a JSON list of IDs: [\"ID1\", \"ID2\"]"
    )
    user_message = f"User Query: '{user_prompt}'\n\nDATASET:\n{data_str}"
    response = call_llm_api_large_context([
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_message}
    ], LARGE_CONTEXT_MODEL)
    if not response: return []
    try:
        return json.loads(response)
    except:
        return re.findall(r'(R_|YT_|AS_|GP_)[a-zA-Z0-9_\-\.:]+', response)

def fetch_details_for_ids(relevant_ids: List[str]) -> List[Dict]:
    results = []
    ids_by_plat = {"Reddit": [], "YouTube": [], "AppStore": [], "GooglePlay": []}
    for rid in relevant_ids:
        if not isinstance(rid, str): continue
        rid = rid.strip()
        for plat, cfg in DB_SCHEMAS.items():
            if rid.startswith(cfg['prefix']):
                raw_id = rid[len(cfg['prefix']):] 
                ids_by_plat[plat].append(raw_id)
                break
    for plat, ids in ids_by_plat.items():
        if not ids: continue
        conn = get_db_connection(DB_SCHEMAS[plat]['db'])
        if not conn: continue
        try:
            placeholders = ','.join(['?'] * len(ids))
            q = f"SELECT * FROM {DB_SCHEMAS[plat]['table']} WHERE {DB_SCHEMAS[plat]['id_col_db']} IN ({placeholders})"
            cursor = conn.execute(q, tuple(ids))
            for row in cursor:
                formatted = format_row(plat, dict(row), conn)
                if formatted: results.append(formatted)
        except Exception as e:
            logging.error(f"Error fetching details for {plat}: {e}")
        finally:
            conn.close()
    return results

def format_row(plat, row, conn):
    text, date, url, meta = "", "", "#", ""
    if plat == "Reddit":
        text = row.get('body', '')
        date = datetime.datetime.fromtimestamp(row.get('created_utc', 0)).strftime('%Y-%m-%d')
        try:
            p = conn.execute("SELECT title FROM reddit_posts WHERE post_id=?", (row.get('post_id'),)).fetchone()
            if p: 
                url = f"https://www.reddit.com/comments/{row.get('post_id')}/_/{row.get('comment_id')}/"
                meta = p['title']
        except: pass
    elif plat == "YouTube":
        text = row.get('text_display', '')
        date = row.get('published_at', '')
        url = f"https://youtube.com/watch?v={row.get('video_id','')}&lc={row.get('comment_id','')}"
    elif plat == "AppStore":
        text = row.get('Review Text', '')
        date = row.get('Review Date', '')
        url = row.get('Review URL', '')
    elif plat == "GooglePlay":
        text = row.get('review_text', '')
        date = row.get('review_date', '')
        url = row.get('url', '')
    return {"platform": plat, "text": text, "date": date.split('T')[0] if date else "", "url": url, "meta": meta}

# ====================================================================
# API ENDPOINTS
# ====================================================================

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

@app.route('/')
def index():
    return send_from_directory('.', 'final_design.html')

@app.route('/<path:filename>')
def serve_static_html(filename):
    if filename.endswith(".html"):
        return send_from_directory('.', filename)
    return jsonify({"error": "File not found"}), 404

@app.route('/report_with_sources.json')
def serve_report():
    return send_from_directory(DATA_DIR, 'report_with_sources.json')

@app.route('/api/nl_sql_search', methods=['POST'])
def nl_sql_search():
    data = request.get_json()
    nl_prompt = data.get('nl_prompt', '').strip()
    platforms = data.get('platforms', [])
    time_period = data.get('time_period', 'all')

    if not nl_prompt: return jsonify({"error": "No prompt"}), 400
        
    full_dataset = fetch_entire_dataset(platforms=platforms, time_period=time_period)
    
    if not full_dataset: 
        return jsonify({"results": [], "msg": "No data found for the selected time frame."})
        
    relevant_ids = llm_scan_full_dataset(nl_prompt, full_dataset)
    if not relevant_ids: return jsonify({"results": []})
        
    final_results = fetch_details_for_ids(relevant_ids)
    return jsonify({"results": final_results})

@app.route('/api/source_counts', methods=['GET'])
def source_counts():
    key_mapping = {"Reddit": "Reddit", "YouTube": "YouTube", "AppStore": "iOS", "GooglePlay": "GP"}
    counts = {}
    for key, config in DB_SCHEMAS.items():
        platform_key = key_mapping.get(key, key)
        if not os.path.exists(config['db']):
            counts[platform_key] = 0
            continue
        conn = get_db_connection(config['db'])
        if conn:
            try:
                cur = conn.cursor()
                cur.execute(f"SELECT COUNT(*) FROM {config['table']}")
                counts[platform_key] = cur.fetchone()[0]
            except Exception as e:
                counts[platform_key] = 0
            finally: conn.close()
        else: counts[platform_key] = 0
    return jsonify(counts)

if __name__ == '__main__':
    app.run(debug=True, port=5000)