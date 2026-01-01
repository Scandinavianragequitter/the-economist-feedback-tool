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

    # --- STRATEGIC PRODUCT ANALYST PROMPT ---
    SYSTEM_INSTRUCTION = (
        "You are a Strategic Product Analyst for The Economist. Your goal is to translate raw user feedback "
        "into actionable product insights that a Product Manager can use to drive roadmaps. "
        "You must look past user vocabulary to identify the underlying product friction or business risk."
    )

    CUSTOM_PROMPT = (
        "Analyze the input data to identify 10-15 distinct product insights. "
        "\n\nSTRICT ANALYTICAL RULES:\n"
        "1. THE 'SO WHAT?' FILTER: For every comment, ask: 'What does a PM need to fix here?' "
        "Ignore the user's emotional tone and identify the root cause (e.g., instead of 'cancellation threats', "
        "identify 'Manual retention workflows' or 'High renewal price sensitivity').\n"
        "2. DYNAMIC TOPICS: Use 2-3 word technical/business categories (e.g., 'RETENTION LOGIC', 'DEVICE OPTIMIZATION', 'RENEWAL UI'). "
        "NEVER use generic labels.\n"
        "3. ACTIONABLE DESCRIPTION: Describe the friction point in a way that implies a product requirement. "
        "Use **bolding** for the core technical or business issue.\n"
        "4. AVOID REPEATING EMOTION: Do not use words like 'threaten', 'hate', or 'scam'. Use neutral terms like "
        "'Users circumvent standard workflows' or 'Perceived value-to-price misalignment'.\n"
        "5. BREVITY: One punchy sentence per insight. Max 22 words.\n"
        "6. FORMAT: 'TOPIC: Insight sentence with **bolded friction** [[ID1, ID2]]'.\n"
        "7. SEPARATION: Exactly TWO blank lines between entries.\n\n"
        "--- INPUT DATA ---\n"
    )
    
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}", 
        "Content-Type": "application/json", 
        "Referer": HTTP_REFERER, 
        "X-Title": "Economist Strategic Analyst"
    }
    
    payload = {
        "model": MODEL_NAME, 
        "messages": [
            {"role": "system", "content": SYSTEM_INSTRUCTION}, 
            {"role": "user", "content": CUSTOM_PROMPT + json_data}
        ], 
        "temperature": 0.1 # Low temperature for consistent, analytical output
    }

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
    print(f"✅ Strategic analysis saved to {LLM_TEXT_OUTPUT}")

if __name__ == "__main__":
    main()