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
    HTTP_REFERER = "https://github.com/scandinavianragequitter/economist-feedback"

# Absolute paths for Render Persistent Disk
INPUT_JSON_FILE = os.path.join(DATA_DIR, "curated_data_for_llm.json") 
LLM_TEXT_OUTPUT = os.path.join(DATA_DIR, "llm_analysis_output.txt")

MAX_RETRIES, INITIAL_DELAY = 5, 5

def process_data_with_llm(json_data):
    if not OPENROUTER_API_KEY:
        return "Error: OPENROUTER_API_KEY environment variable not set."

    SYSTEM_INSTRUCTION = (
        "You are an assistant that digests user feedback data (Reddit threads, YouTube comments, App Store reviews, and Google Play reviews) "
        "and synthesizes an executive-level, concise summary. Return plain text only."
    )

    CUSTOM_PROMPT = (
        "\n\n--- CORE TASK: INSIGHTS FROM USER FEEDBACK ---\n"
        "Based on the input data, which includes comments from Reddit (R_), YouTube (YT_), App Store (AS_), and Google Play (GP_), perform the following strategic analysis:\n"
        "1. Identify the main themes of the audience's feedback. CRITICAL: Translate all emotional, sarcastic, or outraged language into neutral, distanced language (e.g., change 'greedy scam' to 'high price sensitivity').\n"
        "2. Write a clear, concise report, using complete sentences, where each paragraph contains a measured account of one feedback topic.\n"
        "   - **Prioritize Actionable Value Creation:** Focus the majority of the report on insights where user and business incentives align (features, recognizing audience habits, utility). Give these the most detail.\n"
        "   - **Summarize Value Capture:** For feedback where incentives collide (pricing, costs, subscriptions)\n"
        "3. CRITICAL REQUIREMENT: Immediately after *each* insight you MUST insert a list of the unique `ID`s from the input that support and relate thematically to that specific point. The ID list must be enclosed in double square brackets `[[...]]` and separated by commas.\n"
        "\nCRITICAL FORMATTING NOTE: Ensure there are always two blank lines (\\n\\n) between the end of one insight's citation block (]]) and the start of the next insight's text.\n"
        "\n--- FORMAT EXAMPLE ---\n"
        "The analysis suggests a high demand for a dark mode feature to improve readability at night [[R_1a2b, YT_11j2, GP_6r7s]].\n\n"
        "While users appreciate the content, a significant segment expresses friction regarding the current subscription pricing structure [[AS_8t9u, YT_5m6n]].\n"
        "\n--- INPUT DATA ---\n"
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
            # Remove DeepSeek thinking tags
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
    print(f"✅ Success: LLM output saved to {LLM_TEXT_OUTPUT}")

if __name__ == "__main__":
    main()