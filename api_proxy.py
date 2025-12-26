import os
import json
import requests
import sqlite3
import re
import datetime
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
# PERSISTENCE FIX: This must match the mount path in render.yaml
DATA_DIR = os.environ.get("PERSISTENT_STORAGE_PATH", "data")
API_KEY = os.environ.get("OPENROUTER_API_KEY") 
LARGE_CONTEXT_MODEL = "x-ai/grok-4.1-fast" 
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

# Ensure the data directory exists
os.makedirs(DATA_DIR, exist_ok=True)

# --- Database Paths ---
REDDIT_DB = os.path.join(DATA_DIR, "reddit_data.db")
YOUTUBE_DB = os.path.join(DATA_DIR, "youtube_comments.db")
APP_STORE_DB = os.path.join(DATA_DIR, "app_reviews.db")
GOOGLE_PLAY_DB = os.path.join(DATA_DIR, "google_play_reviews.db")

# --- Database Schemas ---
DB_SCHEMAS = {
    "Reddit": {"db": REDDIT_DB, "table": "reddit_comments", "text_col": "body", "id_col_db": "comment_id", "prefix": "R_"},
    "YouTube": {"db": YOUTUBE_DB, "table": "youtube_comments", "text_col": "text_display", "id_col_db": "comment_id", "prefix": "YT_"},
    "AppStore": {"db": APP_STORE_DB, "table": "economist_reviews", "text_col": '"Review Text"', "id_col_db": '"Review ID"', "prefix": "AS_"},
    "GooglePlay": {"db": GOOGLE_PLAY_DB, "table": "google_play_reviews", "text_col": "review_text", "id_col_db": "review_id", "prefix": "GP_"},
}

def get_db_connection(db_path):
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as e:
        logging.error(f"[DB CONNECTION ERROR] {db_path}: {e}")
        return None

def fetch_entire_dataset() -> List[Dict]:
    all_data = []
    for platform, config in DB_SCHEMAS.items():
        if not os.path.exists(config['db']): continue
        conn = get_db_connection(config['db'])
        if not conn: continue
        try:
            query = f"SELECT {config['id_col_db']} as id, {config['text_col']} as text FROM {config['table']}"
            cursor = conn.cursor()
            cursor.execute(query)
            for row in cursor.fetchall():
                all_data.append({"id": f"{config['prefix']}{row['id']}", "t": row['text'][:1000]})
        except: pass
        finally: conn.close()
    return all_data

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

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

@app.route('/')
def index():
    return send_from_directory('.', 'final_design.html')

@app.route('/report_with_sources.json')
def serve_report():
    """FIX: Serves from persistent storage."""
    return send_from_directory(DATA_DIR, 'report_with_sources.json')

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
            except:
                counts[platform_key] = 0
            finally: conn.close()
        else: counts[platform_key] = 0
    return jsonify(counts)

@app.route('/api/nl_sql_search', methods=['POST'])
def nl_sql_search():
    data = request.get_json()
    nl_prompt = data.get('nl_prompt', '').strip()
    if not nl_prompt: return jsonify({"error": "No prompt"}), 400
    full_dataset = fetch_entire_dataset()
    if not full_dataset: return jsonify({"results": []})
    relevant_ids = llm_scan_full_dataset(nl_prompt, full_dataset)
    if not relevant_ids: return jsonify({"results": []})
    # Basic fetch details for search result IDs would go here (omitted for speed)
    return jsonify({"results": []})

if __name__ == '__main__':
    app.run(debug=True, port=5000)