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

def process_data_with_llm(json_data):
    if not OPENROUTER_API_KEY:
        return "Error: OPENROUTER_API_KEY environment variable not set."

    # --- GENERALIZED ANALYTICAL FRAMEWORK ---
    SYSTEM_INSTRUCTION = (
        "You are a Product Intelligence Analyst. Your role is to identify recurring patterns in user feedback. "
        "You categorize feedback into broad functional areas and describe the core friction or request "
        "objectively and concisely."
    )

    CUSTOM_PROMPT = (
        "Analyze the provided feedback data to generate 8-12 strategic insights. "
        "\n\nANALYSIS STEPS:"
        "\n1. PATTERN RECOGNITION: Group independent comments that share the same root cause or functional request."
        "\n2. THEMATIC CATEGORIZATION: Assign each group a broad category (e.g., the high-level system involved)."
        "\n3. DISTILLATION: For each group, write one or two sentences that capture the collective requirement or friction point."
        
        "\n\nSTRICT CONSTRAINTS:"
        "\n- FORMAT: 'CATEGORY: Insight sentence with **bolded core issue** [[ID1, ID2]]'."
        "\n- NO EXAMPLES: Do not use the examples from these instructions in your output unless they exist in the data."
        "\n- NO EMOTION: Strip away user hyperbole; focus on the technical or business hurdle."
        "\n- NO META-COMMENTARY: Provide only the list of insights. No introductions."
        "\n- TOPIC STYLE: Use short, uppercase headers (e.g., AUDIO, CONNECTIVITY, PRICING, INTERFACE)."
        "\n- BOLDING: Highlight the specific feature, bug, or business logic mentioned."
        "\n- SEPARATION: Use exactly TWO blank lines between every entry."
        "\n\n--- INPUT DATA ---\n"
    )
    
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "Referer": HTTP_REFERER,
        "X-Title": "Generalized Feedback Analyzer"
    }

    payload = {
        "model": MODEL_NAME,
        "messages": [
            {"role": "system", "content": SYSTEM_INSTRUCTION},
            {"role": "user", "content": CUSTOM_PROMPT + json_data}
        ],
        "temperature": 0.1 # Very low to ensure structural adherence and less "drift"
    }

    for attempt in range(5):
        try:
            response = requests.post(OPENROUTER_API_URL, headers=headers, json=payload, timeout=300)
            response.raise_for_status()
            content = response.json()['choices'][0]['message']['content'].strip()
            # Remove reasoning tags if present
            content = re.sub(r'<think>.*?</think>', '', content, flags=re.DOTALL).strip()
            return content
        except Exception:
            time.sleep(5)
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