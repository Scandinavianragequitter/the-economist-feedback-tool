import os
import subprocess
import threading
import time
import sys
import logging

# Set up logging for the master script
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# --- CONFIGURATION ---
# Define the order of execution for your pipeline scripts
PIPELINE_SCRIPTS = [
    # 1. Scraping (Update Databases)
    "scrapers/get_app_store_data.py", 
    "scrapers/get_reddit_data.py",
    "scrapers/get_youtube_data.py",
    # ADDED: Google Play Scraper
    "scrapers/get_google_play_data.py", 
    
    # 2. Curation (Create curated_data_for_llm.json)
    "scrapers/get_top_comments.py",
    
    # 3. Analysis (Create llm_analysis_output.txt)
    "llm_analysis.py",
    
    # 4. Processing (Create report_with_sources.json)
    "report_processor.py",
]

BACKEND_SCRIPT = "api_proxy.py"
# ---------------------

def run_script(script_path):
    """Executes a single Python script and handles errors."""
    logging.info(f"🚀 Starting step: {script_path}")
    
    # Use python3 if python command fails, which is common on macOS
    command = [sys.executable, script_path]
    
    try:
        # Check=True raises an exception for non-zero exit codes (script failure)
        subprocess.run(command, check=True, text=True, capture_output=True)
        logging.info(f"✅ Success: {script_path}")
    except subprocess.CalledProcessError as e:
        logging.error(f"❌ FAILED: {script_path} exited with error code {e.returncode}")
        logging.error(f"--- Stdout ---\n{e.stdout}")
        logging.error(f"--- Stderr ---\n{e.stderr}")
        return False
    except FileNotFoundError:
        logging.error(f"❌ FAILED: Script not found at {script_path}. Check your path.")
        return False
    return True

def start_backend():
    """Starts the Flask server in a separate thread."""
    def run_flask():
        # The Flask app will use its internal run configuration (port 5000)
        try:
            # Setting environment variable to disable Flask's interactive debugger in thread
            os.environ['FLASK_ENV'] = 'production'
            # Use 'python3' for consistent execution
            subprocess.run([sys.executable, BACKEND_SCRIPT], check=True)
        except Exception as e:
            logging.error(f"❌ Backend server failed to start: {e}")

    logging.info(f"🌐 Starting backend server: {BACKEND_SCRIPT} on http://127.0.0.1:5000...")
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    
    # Give the server a moment to start up
    time.sleep(3) 
    return flask_thread

def main():
    """Orchestrates the sequential pipeline execution and starts the backend."""
    
    # 1. Execute sequential pipeline
    logging.info("--- Starting Data & Analysis Pipeline ---")
    for script in PIPELINE_SCRIPTS:
        if not run_script(script):
            logging.critical("🛑 Pipeline halted due to critical error in previous step.")
            sys.exit(1)
            
    # 2. Start the backend proxy server (non-blocking)
    backend_thread = start_backend()
    
    logging.info("\n--- ORCHESTRATION COMPLETE ---")
    logging.info("The report files are updated. The backend is running.")
    logging.info("Open 'report_viewer.html' in your browser.")
    logging.info("Press Ctrl+C to stop the backend server.")

    # Keep the main thread alive so the daemon Flask thread continues to run
    try:
        while backend_thread.is_alive():
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("\nReceived keyboard interrupt. Shutting down...")
        # Since the thread is a daemon, it should exit with the main program
        sys.exit(0)

if __name__ == "__main__":
    main()