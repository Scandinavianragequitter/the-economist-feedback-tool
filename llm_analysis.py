import requests
import sys
import os
import time
import re

# --- PERSISTENCE & CONFIG ---
DATA_DIR = os.environ.get("PERSISTENT_STORAGE_PATH", "data")
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY")

try:
    from config.openrouter_config import OPENROUTER_API_URL, MODEL_NAME, HTTP_REFERER
except ImportError:
    OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions"
    MODEL_NAME = "tngtech/deepseek-r1t2-chimera:free"
    HTTP_REFERER = "https://github.com/my-economist-report-app"

# Absolute paths for Render Persistent Disk
INPUT_JSON_FILE = os.path.join(DATA_DIR, "curated_data_for_llm.json") 
LLM_TEXT_OUTPUT = os.path.join(DATA_DIR, "llm_analysis_output.txt")

MAX_RETRIES, INITIAL_DELAY = 5, 5

def process_data_with_llm(json_data):
    if not OPENROUTER_API_KEY:
        return "Error: OPENROUTER_API_KEY environment variable not set."

    # --- ENHANCED PROMPT FOR PUNCHY INSIGHTS & DYNAMIC TOPICS ---
    SYSTEM_INSTRUCTION = (
        "You are a Senior Product Analyst. Your goal is to synthesize user feedback into a condensed executive dashboard. "
        "You must identify high-intent technical or UX topics and describe the friction points with extreme brevity."
    )

    CUSTOM_PROMPT = (
        "Analyze the input data and identify the 10-15 most specific pain points. "
        "\n\nSTRICT RULES:\n"
        "1. DYNAMIC TOPICS: Every topic must be descriptive and specific to the issue (e.g., 'Subscription Friction', 'Audio Playback', 'iPad UI Scaling', 'Ad-Free Expectation'). "
        "NEVER use generic topics like 'General', 'Feedback', 'App Issues', or 'Comments'.\n"
        "2. CONCiseness: The insight text MUST be a single punchy sentence, maximum 15-20 words.\n"
        "3. BOLDING: Use **bolding** to highlight the core problem within the sentence.\n"
        "4. FORMAT: Each line must follow: 'TOPIC: Detailed punchy sentence [[ID1, ID2]]'.\n"
        "5. SEPARATION: Put TWO blank lines between every entry.\n"
        "6. NO BOLDING TOPICS: Do not bold the Topic name itself, only words inside the sentence.\n\n"
        "--- INPUT DATA ---\n"
    )
    
    user_message = CUSTOM_PROMPT + json_data

    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "Referer": HTTP_REFERER,
        "X-Title": "The Economist Feedback Analyzer"
    }

    payload = {
        "model": MODEL_NAME,
        "messages": [
            {"role": "system", "content": SYSTEM_INSTRUCTION},
            {"role": "user", "content": user_message}
        ],
        "temperature": 0.0001
    }

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(OPENROUTER_API_URL, headers=headers, json=payload, timeout=300)
            response.raise_for_status()
            content = response.json()['choices'][0]['message']['content'].strip()
            # Clean DeepSeek/Reasoning tags
            content = re.sub(r'<think>.*?</think>', '', content, flags=re.DOTALL).strip()
            return content
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(INITIAL_DELAY * (2 ** attempt))
                continue
            return f"Error: {e}"
    return "Error: Unknown failure."

def main():
    if not os.path.exists(INPUT_JSON_FILE):
        print(f"❌ Input file not found: {INPUT_JSON_FILE}")
        sys.exit(1)
    with open(INPUT_JSON_FILE, 'r', encoding='utf-8') as f:
        json_data = f.read()
    analysis = process_data_with_llm(json_data)
    with open(LLM_TEXT_OUTPUT, 'w', encoding='utf-8') as out_f:
        out_f.write(analysis)
    print(f"✅ Thematic analysis saved to {LLM_TEXT_OUTPUT}")

if __name__ == "__main__":
    main()