#!/bin/bash

# Exit immediately if any command fails. This is CRITICAL for production scripts.
set -e 

echo "--- Starting Daily Audience Feedback Pipeline: $(date) ---"

# Ensure we are in the correct directory (project root)
# The scripts are expected to be relative to this root.

# 1. ACQUISITION PHASE: Run all scrapers to update the four source databases in the persistent storage path
echo "1.1 Running Reddit Scraper..."
python scrapers/get_reddit_data.py

echo "1.2 Running YouTube Scraper..."
python scrapers/get_youtube_data.py

echo "1.3 Running App Store Scraper..."
python scrapers/get_app_store_data.py

echo "1.4 Running Google Play Scraper..."
python scrapers/get_google_play_data.py


# 2. CURATION PHASE: Consolidate data, apply final sampling, and prepare LLM input
# Based on your master orchestration script, 'get_top_comments.py' handles this phase.
echo "2.0 Running Curation Pipeline (Generates curated_data_for_llm.json)..."
python scrapers/get_top_comments.py


# 3. ANALYSIS PHASE: Call the LLM to synthesize the report and generate citations
echo "3.0 Running LLM Analysis (Generates llm_analysis_output.txt)..."
python llm_analysis.py


# 4. REPORT PROCESSING PHASE: Parse LLM text and enrich citations with DB details
echo "4.0 Running Report Processor (Generates report_with_sources.json)..."
python report_processor.py


echo "--- Pipeline Successfully Completed ---"
echo "The dashboard data (report_with_sources.json) is now fresh."