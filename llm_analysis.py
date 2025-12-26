import requests
import sys
import os
import time

# --- PERSISTENCE & CONFIG ---
DATA_DIR = os.environ.get("PERSISTENT_STORAGE_PATH", "data")
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY")

try:
    from config.openrouter_config import OPENROUTER_API_URL, MODEL_NAME, HTTP_REFERER
except ImportError:
    OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions"
    MODEL_NAME = "tngtech/deepseek-r1t2-chimera:free"
    HTTP_REFERER = "https://github.com/my-economist-report-app"

# FIX: Use absolute paths in the persistent disk
INPUT_JSON_FILE = os.path.join(DATA_DIR, "curated_data_for_llm.json") 
LLM_TEXT_OUTPUT = os.path.join(DATA_DIR, "llm_analysis_output.txt")
MAX_RETRIES, INITIAL_DELAY = 5, 5

def process_data_with_llm(json_data):
    if not OPENROUTER_API_KEY:
        return "Error: OPENROUTER_API_KEY environment variable not set."

    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "Referer": HTTP_REFERER,
        "X-Title": "The Economist Feedback Analyzer"
    }

    # Enhanced System Prompt for Actionable and Concrete Insights
    system_prompt = (
        "You are an expert Product Analyst and UX Researcher for The Economist. "
        "Your goal is to digest user feedback and produce a high volume of granular, actionable insights. "
        "\n\nCRITICAL INSTRUCTIONS:"
        "\n1. PRIORITIZE CONCRETE FEEDBACK: Focus on specific technical bugs, UI/UX friction points, missing features, "
        "or content delivery issues."
        "\n2. BE ACTIONABLE: Every insight must be framed as a specific problem followed by a suggested solution or improvement."
        "\n3. VOLUME: Aim to identify at least 10-15 distinct insights if the data allows. Do not aggregate unique issues into vague categories."
        "\n4. CITATIONS: You MUST include citations for every insight in the format: [[ID1, ID2]]. The ID must match the data exactly."
        "\n5. FORMAT: Provide each insight as a standalone paragraph or bullet point. Do not include an intro or outro."
    )

    user_content = (
        "Analyze the following user feedback data from Reddit, YouTube, and App Stores. "
        "Identify specific pain points and provide concrete, actionable recommendations for each. "
        "Ensure specific feedback is prioritized over general praise or generic complaints.\n\n"
        f"INPUT DATA:\n{json_data}"
    )

    payload = {
        "model": MODEL_NAME,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content}
        ],
        "temperature": 0.2 # Slightly increased for more diverse insight discovery while maintaining focus
    }

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(OPENROUTER_API_URL, headers=headers, json=payload, timeout=300)
            response.raise_for_status()
            return response.json()['choices'][0]['message']['content'].strip()
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

if __name__ == "__main__":
    main()