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

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

@app.route('/')
def index():
    return send_from_directory('.', 'final_design.html')

@app.route('/report_with_sources.json')
def serve_report():
    """Serves the enriched dashboard report from the PERSISTENT storage path."""
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
            except: counts[platform_key] = 0
            finally: conn.close()
        else: counts[platform_key] = 0
    return jsonify(counts)

if __name__ == '__main__':
    app.run(debug=True, port=5000)