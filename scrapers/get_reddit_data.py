import praw
import sqlite3
import time
from datetime import datetime
import sys
import os

# --- CRITICAL PATH FIX ---
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

# --- DATABASE PATH ---
DATA_DIR = os.environ.get("PERSISTENT_STORAGE_PATH", "data")
DATABASE_NAME = os.path.join(DATA_DIR, "reddit_data.db")

# --- CREDENTIALS & CONFIGURATION ---
# Load secrets from environment variables for security
CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID")
CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET")
USER_AGENT = os.environ.get("REDDIT_USER_AGENT", "Subreddit Scraper v1.0")

# Load non-sensitive parameters from config or environment defaults
try:
    from config.reddit_config import (
        SUBREDDIT_NAME, LIMIT_POSTS, RATE_LIMIT_PER_MINUTE, 
        MAX_RETRIES, BACKOFF_FACTOR
    )
except ImportError:
    # Fallback defaults if config file is missing
    SUBREDDIT_NAME = os.environ.get("REDDIT_SUBREDDIT", "theeconomist")
    LIMIT_POSTS = int(os.environ.get("REDDIT_LIMIT_POSTS", 100))
    RATE_LIMIT_PER_MINUTE = int(os.environ.get("REDDIT_RATE_LIMIT", 80))
    MAX_RETRIES = 5
    BACKOFF_FACTOR = 2

MIN_DELAY_SECONDS = 60 / RATE_LIMIT_PER_MINUTE 

def initialize_reddit():
    if not CLIENT_ID or not CLIENT_SECRET:
        print("❌ ERROR: REDDIT_CLIENT_ID or REDDIT_CLIENT_SECRET not set in environment.")
        return None
    print("-> Initializing Reddit API connection...")
    try:
        reddit = praw.Reddit(
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
            user_agent=USER_AGENT
        )
        reddit.read_only = True
        return reddit
    except Exception as e:
        print(f"Error initializing PRAW: {e}")
        return None

def initialize_database(db_name):
    os.makedirs(os.path.dirname(db_name), exist_ok=True)
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS reddit_posts (
            post_id TEXT PRIMARY KEY, subreddit TEXT NOT NULL, title TEXT NOT NULL,
            score INTEGER, upvote_ratio REAL, num_comments INTEGER,
            created_utc REAL, post_url TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS reddit_comments (
            comment_id TEXT PRIMARY KEY, post_id TEXT, parent_id TEXT,
            author TEXT, body TEXT, score INTEGER, created_utc REAL,
            FOREIGN KEY (post_id) REFERENCES reddit_posts (post_id)
        )
    """)
    conn.commit()
    return conn, cursor

def process_comments(comment_list, post_id, cursor, conn):
    comments_inserted = 0
    for comment in comment_list:
        if isinstance(comment, praw.models.MoreComments): continue
        try:
            author_name = comment.author.name if comment.author else "[deleted]"
            comment_data = (comment.id, post_id, comment.parent_id, author_name, comment.body, comment.score, comment.created_utc)
            cursor.execute("INSERT OR IGNORE INTO reddit_comments VALUES (?, ?, ?, ?, ?, ?, ?)", comment_data)
            comments_inserted += 1
            if comment.replies:
                comments_inserted += process_comments(comment.replies, post_id, cursor, conn)
        except: pass 
    return comments_inserted

def run_scraper(reddit, conn, cursor):
    if not reddit: return
    subreddit = reddit.subreddit(SUBREDDIT_NAME)
    retry_count = 0
    while retry_count < MAX_RETRIES:
        try:
            submissions = list(subreddit.top(time_filter="month", limit=LIMIT_POSTS))
            for submission in submissions:
                post_data = (submission.id, SUBREDDIT_NAME, submission.title, submission.score, submission.upvote_ratio, submission.num_comments, submission.created_utc, submission.url)
                try:
                    cursor.execute("INSERT INTO reddit_posts VALUES (?, ?, ?, ?, ?, ?, ?, ?)", post_data)
                    submission.comments.replace_more(limit=None) 
                    process_comments(submission.comments.list(), submission.id, cursor, conn)
                except sqlite3.IntegrityError: pass
                time.sleep(MIN_DELAY_SECONDS)
            conn.commit()
            break
        except Exception as e:
            retry_count += 1
            time.sleep(BACKOFF_FACTOR ** retry_count)

if __name__ == "__main__":
    reddit_instance = initialize_reddit()
    db_connection, db_cursor = initialize_database(DATABASE_NAME)
    try:
        run_scraper(reddit_instance, db_connection, db_cursor)
    finally:
        db_connection.close()