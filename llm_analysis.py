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

INPUT_JSON_FILE = os.path.join(DATA_DIR, "curated_data_for_llm.json") 
LLM_TEXT_OUTPUT = os.path.join(DATA_DIR, "llm_analysis_output.txt")
MAX_RETRIES, INITIAL_DELAY = 5, 5

def process_data_with_llm(json_data):
    if not OPENROUTER_API_KEY:
        return "Error: OPENROUTER_API_KEY environment variable not set."

    SYSTEM_INSTRUCTION = (
        "You are a Product Analyst specializing in identifying user friction. "
        "Your goal is to extract concrete pain points and categorize them by topic.\n\n"
        "STRICT FORMATTING RULES:\n"
        "1. FORMAT: Each line must follow: 'TOPIC: Single objective sentence describing the problem [[ID1, ID2]]'.\n"
        "2. TOPICS: Decide on topics dynamically based on the data (e.g., App Stability, Audio, Pricing).\n"
        "3. NO NUMBERING: Do not use '1.', '2.', or bullets. Start directly with the Topic.\n"
        "4. NO BOLDING: Use plain text only.\n"
        "5. SEPARATION: Put exactly TWO blank lines between every insight.\n"
        "6. CONCRETE ONLY: Prioritize technical bugs or specific UX friction points."
    )

    CUSTOM_PROMPT = (
        "\n\n--- CORE TASK: CATEGORIZED PAIN POINTS ---\n"
        "Identify the 10-15 most specific pain points. Group them by dynamic topics.\n\n"
        "FORMAT EXAMPLE:\n"
        "Connectivity: Login failures persist across multiple reinstall attempts [[R_123, AS_456]].\n\n\n"
        "App Stability: Users on iPad mini devices report total app crashes [[GP_789]]."
        "\n\n--- INPUT DATA ---\n"
    )
    
    user_message = CUSTOM_PROMPT + json_data
    headers = {"Authorization": f"Bearer {OPENROUTER_API_KEY}", "Content-Type": "application/json", "Referer": HTTP_REFERER, "X-Title": "The Economist Analyzer"}
    payload = {"model": MODEL_NAME, "messages": [{"role": "system", "content": SYSTEM_INSTRUCTION}, {"role": "user", "content": user_message}], "temperature": 0.0001}

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(OPENROUTER_API_URL, headers=headers, json=payload, timeout=300)
            response.raise_for_status()
            content = response.json()['choices'][0]['message']['content'].strip()
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
        print(f"❌ Input file not found: {INPUT_JSON_FILE}"); sys.exit(1)
    with open(INPUT_JSON_FILE, 'r', encoding='utf-8') as f:
        json_data = f.read()
    analysis = process_data_with_llm(json_data)
    with open(LLM_TEXT_OUTPUT, 'w', encoding='utf-8') as out_f:
        out_f.write(analysis)
    print(f"✅ Categorized analysis saved to {LLM_TEXT_OUTPUT}")

if __name__ == "__main__":
    main()