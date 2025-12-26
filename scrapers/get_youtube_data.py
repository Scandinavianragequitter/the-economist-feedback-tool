import sqlite3
import datetime
import time
import os
import sys

# --- CRITICAL PATH FIX ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from googleapiclient.discovery import build

# --- DATABASE PATH ---
DATA_DIR = os.environ.get("PERSISTENT_STORAGE_PATH", "data")
DATABASE_NAME = os.path.join(DATA_DIR, "youtube_comments.db")

# --- CREDENTIALS ---
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY")

try:
    from config.youtube_config import CHANNEL_ID, TIME_FILTER_DAYS, MAX_COMMENTS_PER_PAGE
except ImportError:
    CHANNEL_ID = os.environ.get("YOUTUBE_CHANNEL_ID", "UC0p5jTq6Xx_DosDFxVXnWaQ")
    TIME_FILTER_DAYS = int(os.environ.get("YOUTUBE_FILTER_DAYS", 30))
    MAX_COMMENTS_PER_PAGE = 50

YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION = "youtube", "v3"

def initialize_database(db_name):
    os.makedirs(os.path.dirname(db_name), exist_ok=True)
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS youtube_videos (
            video_id TEXT PRIMARY KEY, title TEXT NOT NULL, published_at TEXT, 
            view_count INTEGER, comment_count INTEGER
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS youtube_comments (
            comment_id TEXT PRIMARY KEY, video_id TEXT, author_display_name TEXT, 
            text_display TEXT, like_count INTEGER, published_at TEXT,
            FOREIGN KEY (video_id) REFERENCES youtube_videos (video_id)
        )
    """)
    conn.commit()
    return conn, cursor

def get_youtube_service():
    if not YOUTUBE_API_KEY:
        print("❌ ERROR: YOUTUBE_API_KEY not set in environment.")
        return None
    return build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=YOUTUBE_API_KEY)

def get_recent_videos(youtube, channel_id, days):
    time_cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days)
    channel_response = youtube.channels().list(id=channel_id, part='contentDetails').execute()
    uploads_id = channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    playlist_items = youtube.playlistItems().list(playlistId=uploads_id, part='contentDetails', maxResults=50).execute()
    video_ids = []
    for item in playlist_items.get('items', []):
        v_id = item['contentDetails']['videoId']
        v_details = youtube.videos().list(id=v_id, part='snippet,statistics').execute()
        if not v_details.get('items'): continue
        v_item = v_details['items'][0]
        published_at = datetime.datetime.fromisoformat(v_item['snippet']['publishedAt'].replace('Z', '+00:00'))
        if published_at > time_cutoff: video_ids.append(v_item)
    return video_ids

def scrape_comments(youtube, conn, cursor, video_item):
    v_id, title = video_item['id'], video_item['snippet']['title']
    stats = video_item['statistics']
    cursor.execute("INSERT OR IGNORE INTO youtube_videos VALUES (?, ?, ?, ?, ?)", 
                   (v_id, title, video_item['snippet']['publishedAt'], int(stats.get('viewCount', 0)), int(stats.get('commentCount', 0))))
    try:
        response = youtube.commentThreads().list(part='snippet', videoId=v_id, textFormat='plainText', order='relevance', maxResults=MAX_COMMENTS_PER_PAGE).execute()
        for item in response.get('items', []):
            snippet = item['snippet']['topLevelComment']['snippet']
            cursor.execute("INSERT OR IGNORE INTO youtube_comments VALUES (?, ?, ?, ?, ?, ?)", 
                           (item['id'], v_id, snippet['authorDisplayName'], snippet['textDisplay'], snippet.get('likeCount', 0), snippet['publishedAt']))
        time.sleep(1)
    except: pass

def main():
    service = get_youtube_service()
    if not service: return
    conn, cursor = initialize_database(DATABASE_NAME)
    videos = get_recent_videos(service, CHANNEL_ID, TIME_FILTER_DAYS)
    for v in videos: scrape_comments(service, conn, cursor, v)
    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()