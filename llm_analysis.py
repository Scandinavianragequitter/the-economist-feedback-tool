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
        "Your goal is to synthesize user feedback into a condensed, executive-level dashboard.\n\n"
        "STRICT FORMATTING RULES:\n"
        "1. GROUPING: Combine all related specific feedback into a single cohesive paragraph per dynamic topic.\n"
        "2. FORMAT: Each item must follow: 'TOPIC: Cohesive paragraph text containing all related issues [[ID1, ID2]].'\n"
        "3. BOLDING: Use **bolding** to highlight the most critical pain points or technical bugs within the paragraph.\n"
        "4. NO LISTS: Do not use numbering or bullet points. Use standard prose within each topic paragraph.\n"
        "5. SEPARATION: Put exactly TWO blank lines between every Topic paragraph.\n"
        "6. CONCISE BUT DETAILED: Be punchy. Do not suggest solutions; only describe the pain points accurately."
    )

    CUSTOM_PROMPT = (
        "\n\n--- CORE TASK: THEMATIC PAIN POINT CONDENSING ---\n"
        "Group all granular feedback points into themes. For each theme, write a paragraph "
        "that captures the gist. Use bolding for the key issues.\n\n"
        "FORMAT EXAMPLE:\n"
        "App Stability: Users on **iPad mini** devices report **total app crashes** on launch, "
        "while others experience intermittent freezing during article transitions [[R_123, AS_456, GP_789]]."
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
    print(f"✅ Thematic analysis saved to {LLM_TEXT_OUTPUT}")

if __name__ == "__main__":
    main()