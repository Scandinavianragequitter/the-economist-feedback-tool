import requests
import sys
import os
import time
import re

# --- CONFIG ---
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

def process_data_with_llm(json_data):
    if not OPENROUTER_API_KEY:
        return "Error: OPENROUTER_API_KEY environment variable not set."

    SYSTEM_INSTRUCTION = (
        "You are a Senior Product Research Lead. Your goal is to identify repeating behavioral patterns "
        "and friction points within user feedback. You synthesize raw comments into strategic gists."
    )

    CUSTOM_PROMPT = (
        "Analyze the input data and perform the following:\n"
        "1. CLUSTER: Group related feedback into 8-12 thematic clusters based on the specific product area or user behavior.\n"
        "2. DISTILL THE GIST: For each cluster, write one cohesive paragraph (max 30 words) describing the collective behavior. "
        "Prioritize describing the *tactics* users use or the specific *friction* they face.\n"
        "3. PM PERSPECTIVE: Focus on insights that help a Product Manager understand why users are behaving this way.\n"
        "\nSTRICT FORMATTING RULES:\n"
        "- TOPIC: Use a dynamic 2-4 word title (e.g., 'Subscription Price Sensitivity', 'iPad UI Optimization').\n"
        "- BOLDING: Use **bolding** to highlight the core friction or behavior within the paragraph.\n"
        "- NO REPEATING USER SLANG: Do not use words like 'threaten', 'scam', or 'hate'. Use neutral, analytical terms.\n"
        "- NO NUMBERING: Start each entry with the TOPIC name.\n"
        "- CITATIONS: Include all relevant unique IDs at the end of the paragraph in double brackets [[ID1, ID2]].\n"
        "- SEPARATION: Put exactly TWO blank lines between every entry.\n"
        "\n--- INPUT DATA ---\n"
    )
    
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "Referer": HTTP_REFERER,
        "X-Title": "Strategic Feedback Analyzer"
    }

    payload = {
        "model": MODEL_NAME,
        "messages": [
            {"role": "system", "content": SYSTEM_INSTRUCTION},
            {"role": "user", "content": CUSTOM_PROMPT + json_data}
        ],
        "temperature": 0.3 # Slightly higher to allow for better thematic grouping
    }

    for attempt in range(5):
        try:
            response = requests.post(OPENROUTER_API_URL, headers=headers, json=payload, timeout=300)
            response.raise_for_status()
            content = response.json()['choices'][0]['message']['content'].strip()
            content = re.sub(r'<think>.*?</think>', '', content, flags=re.DOTALL).strip()
            return content
        except Exception as e:
            time.sleep(5 * (2 ** attempt))
    return "Error: LLM Request Failed."

def main():
    if not os.path.exists(INPUT_JSON_FILE): return
    with open(INPUT_JSON_FILE, 'r', encoding='utf-8') as f:
        json_data = f.read()
    analysis = process_data_with_llm(json_data)
    with open(LLM_TEXT_OUTPUT, 'w', encoding='utf-8') as out_f:
        out_f.write(analysis)

if __name__ == "__main__":
    main()