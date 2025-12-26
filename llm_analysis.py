import requests
import sys
import os
import time

# --- CREDENTIALS & CONFIG ---
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY")

try:
    from config.openrouter_config import OPENROUTER_API_URL, MODEL_NAME, HTTP_REFERER
except ImportError:
    OPENROUTER_API_URL = os.environ.get("OPENROUTER_API_URL", "https://openrouter.ai/api/v1/chat/completions")
    MODEL_NAME = os.environ.get("LLM_MODEL_NAME", "tngtech/deepseek-r1t2-chimera:free")
    HTTP_REFERER = os.environ.get("HTTP_REFERER", "https://github.com/my-economist-report-app")

INPUT_JSON_FILE = "data/curated_data_for_llm.json" 
LLM_TEXT_OUTPUT = "llm_analysis_output.txt"
MAX_RETRIES, INITIAL_DELAY = 5, 5

def process_data_with_llm(json_data):
    if not OPENROUTER_API_KEY:
        return "Error: OPENROUTER_API_KEY environment variable not set."

    SYSTEM_INSTRUCTION = "You are an assistant that digests user feedback data and synthesizes an executive-level summary. Return plain text only."
    CUSTOM_PROMPT = "--- TASK: INSIGHTS FROM USER FEEDBACK ---\nPerform strategic analysis and include citations as [[ID]].\n\nINPUT DATA:\n"
    user_message = CUSTOM_PROMPT + json_data

    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "Referer": HTTP_REFERER,
        "X-Title": "The Economist Feedback Analyzer"
    }

    payload = {
        "model": MODEL_NAME,
        "messages": [{"role": "system", "content": SYSTEM_INSTRUCTION}, {"role": "user", "content": user_message}],
        "temperature": 0.0001
    }

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(OPENROUTER_API_URL, headers=headers, json=payload, timeout=300)
            response.raise_for_status()
            result = response.json()
            analysis = result['choices'][0]['message']['content']
            return analysis.strip()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429 and attempt < MAX_RETRIES - 1:
                time.sleep(INITIAL_DELAY * (2 ** attempt))
                continue
            return f"Error: HTTP {e.response.status_code}. {e.response.text[:200]}"
        except Exception as e:
            return f"Error: {e}"
    return "Error: Unknown failure."

def main():
    if not os.path.exists(INPUT_JSON_FILE): sys.exit(1)
    with open(INPUT_JSON_FILE, 'r', encoding='utf-8') as f: json_data = f.read()
    llm_analysis = process_data_with_llm(json_data)
    with open(LLM_TEXT_OUTPUT, 'w', encoding='utf-8') as out_f: out_f.write(llm_analysis)

if __name__ == "__main__":
    main()