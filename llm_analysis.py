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

    # --- THE SKEPTIC'S ANCHOR (Absolute Truths) ---
    # These prevent the AI from agreeing with users that "Ads are a bug" or "Teaser rates are scams"
    PRODUCT_GROUND_TRUTH = """
    ABSOLUTE PRODUCT TRUTHS (Use these to verify user claims):
    1. ADS: Both Standard and Premium subscriptions ARE NOT ad-free. They include third-party ads and Economist promotions.
    2. PRICING: Introductory 'teaser' rates ($1/week) are strictly time-bound. Renewal at the 'List Price' is the intended behavior.
    3. RETENTION: Cancellation usually requires a manual workflow or support interaction; this is a business choice, not a technical bug.
    4. ARCHIVE: Basic digital subscriptions do not include full historical archive access.
    """

    SYSTEM_INSTRUCTION = (
        "You are a Senior Strategic Analyst. You operate in two phases:\n"
        "PHASE 1 (The Skeptic): Review the user feedback against the provided 'Absolute Product Truths'. "
        "Identify claims that are actually intended product behaviors rather than technical failures.\n"
        "PHASE 2 (The Dashboard): Synthesize only the most actionable insights for a Product Manager."
    )

    CUSTOM_PROMPT = f"""
    {PRODUCT_GROUND_TRUTH}

    --- CORE TASK ---
    Identify 10-15 strategic insights. Look past user vocabulary (like 'threaten', 'scam', 'broken') to find 
    the underlying product friction or business opportunity.

    STRICT ANALYTICAL RULES:
    1. SKEPTICISM: If a user complains about intended behavior (e.g., ads or pricing), do NOT report it as a bug. 
       Reframe it as an **Expectation Gap** or **Communication Failure**.
    2. BEHAVIORAL SHIFT: Describe what users *do* (e.g., 'Users use VPNs to access lower regional pricing') 
       rather than what they *say* (e.g., 'The app is expensive').
    3. NO BOLDING TOPICS: Do not bold the Topic Name.
    4. STRATEGIC BOLDING: **Bold** only the core product feature or business metric requiring PM attention.
    5. BREVITY: Max 20 words per insight. One sentence only.
    6. FORMAT: 'TOPIC: Insight sentence with **bolded component** [[ID1, ID2]]'.
    7. SEPARATION: Put TWO blank lines between every entry.

    --- INPUT DATA ---
    {json_data}
    """
    
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
            {"role": "user", "content": CUSTOM_PROMPT}
        ], 
        "temperature": 0.0001 # Keep it strictly logical
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
    print(f"✅ Skeptic-corrected analysis saved to {LLM_TEXT_OUTPUT}")

if __name__ == "__main__":
    main()