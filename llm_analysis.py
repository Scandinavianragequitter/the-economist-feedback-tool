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

    # --- BEHAVIORAL ANALYST PROMPT ---
    SYSTEM_INSTRUCTION = (
        "You are a Behavior-Focused Product Analyst. Your goal is to synthesize user feedback into a dashboard "
        "describing how users interact with the service and the tactics they employ to navigate perceived friction. "
        "You must avoid stating user complaints as objective facts about the service."
    )

    CUSTOM_PROMPT = (
        "Analyze the input data to identify 10-15 specific user behaviors or friction points. "
        "\n\nSTRICT RULES:\n"
        "1. DYNAMIC TOPICS: Assign a specific, 2-3 word topic to each insight (e.g., 'SUBSCRIPTION PRICING', 'IPAD UI SCALING'). "
        "NEVER use generic topics like 'GENERAL', 'APP ISSUES', or 'FEEDBACK'.\n"
        "2. BEHAVIORAL PERSPECTIVE: Describe what users *do* or how they *respond* to the service. "
        "Instead of 'The price is high', use 'Users utilize various tactics like VPNs or resellers to bypass standard renewal pricing'.\n"
        "3. AVOID SERVICE 'FACTS': Do not validate user opinions as technical truth (e.g., avoid 'The app is broken'). "
        "Instead, use 'Users encounter friction during login and resort to multiple reinstall attempts'.\n"
        "4. BREVITY: Each insight must be a single punchy sentence, maximum 20 words.\n"
        "5. FORMAT: Each line must follow: 'TOPIC: Behavioral insight sentence [[ID1, ID2]]'.\n"
        "6. SEPARATION: Put exactly TWO blank lines between every entry.\n\n"
        "--- INPUT DATA ---\n"
    )
    
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}", 
        "Content-Type": "application/json", 
        "Referer": HTTP_REFERER, 
        "X-Title": "The Economist Behavioral Analyzer"
    }
    
    payload = {
        "model": MODEL_NAME, 
        "messages": [
            {"role": "system", "content": SYSTEM_INSTRUCTION}, 
            {"role": "user", "content": CUSTOM_PROMPT + json_data}
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
        print(f"❌ Input file not found: {INPUT_JSON_FILE}"); sys.exit(1)
    with open(INPUT_JSON_FILE, 'r', encoding='utf-8') as f:
        json_data = f.read()
    analysis = process_data_with_llm(json_data)
    with open(LLM_TEXT_OUTPUT, 'w', encoding='utf-8') as out_f:
        out_f.write(analysis)
    print(f"✅ Behavioral analysis saved to {LLM_TEXT_OUTPUT}")

if __name__ == "__main__":
    main()