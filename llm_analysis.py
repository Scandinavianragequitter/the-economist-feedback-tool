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
    """
    Sends data to OpenRouter with a prompt optimized for identifying pain points.
    Forcibly prevents numbering and ensures clean paragraph separation.
    """
    if not OPENROUTER_API_KEY:
        return "Error: OPENROUTER_API_KEY environment variable not set."

    SYSTEM_INSTRUCTION = (
        "You are a Product Analyst specializing in identifying user friction. "
        "Your goal is to extract specific, concrete pain points from user feedback."
        "Example topics could be app-related technical issues, suggestions for improvement, reactions to new products or editorial. \n\n"

        "STRICT FORMATTING RULES:\n"
        "1. NO NUMBERING: Do not use '1.', '2.', or any list markers. Start sentences directly with text.\n"
        "2. NO BOLDING: Do not use asterisks (**) or markdown formatting.\n"
        "3. SEPARATION: You MUST put exactly TWO blank lines between every insight.\n"
        "4. ONE SENTENCE: Each insight must be a single, punchy, objective sentence.\n"
        "5. CITATIONS: End every sentence with its source IDs in double brackets [[ID1, ID2]].\n"
        "6. NO SOLUTIONS: Describe only the problem, not the fix."
    )

    CUSTOM_PROMPT = (
        "\n\n--- CORE TASK: IDENTIFY PAIN POINTS ---\n"
        "Analyze the input data. Identify up to 20 frequent and specific pain points. "
        "Frame each as a single objective sentence. "
        "\n\n--- INPUT DATA ---\n"
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
    print(f"✅ Pain-point analysis saved to {LLM_TEXT_OUTPUT}")

if __name__ == "__main__":
    main()