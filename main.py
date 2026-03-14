import os
import time
import sys
from datetime import datetime


RAW_WET_FILE = r"data\raw\sample.wet"
STAGE1_OUT = r"data\processed\x.jsonl"
ROUTED_DIR = r"data\routed"
ZH_ROUTED = r"data\routed\zh\data.jsonl"
DEDUPED_OUT = r"data\deduped\zh_deduped.jsonl"
REFINED_OUT = r"data\cleaned\zh_refined_tagged.jsonl"
BLACKLIST_CONFIG = r"config\blacklist.txt"
FINAL_TRAIN_SET = r"data\final\train_set.jsonl"
SYNTHETIC_OUT = r"data\distilled\gemini_synthetic_data.jsonl"

def run_stage_1():
    
    from step1_lang_router import language_router
    if os.path.exists(ZH_ROUTED): return "Skipped"
    if not os.path.exists(RAW_WET_FILE): raise FileNotFoundError(f"file cannot found: {RAW_WET_FILE}")
    language_router(STAGE1_OUT, ROUTED_DIR)
    return "Done"

def run_stage_2():
    from step2_deduplication_zh import deduplicate_zh_data
    if os.path.exists(DEDUPED_OUT): return "Skipped"
    if not os.path.exists(ZH_ROUTED): raise FileNotFoundError("lack of language diversion data")
    deduplicate_zh_data(ZH_ROUTED, DEDUPED_OUT)
    return "Done"

def run_stage_3():
    from step3_zh_filtering_and_tagging import process_refining
    if os.path.exists(REFINED_OUT): return "Skipped"
    if not os.path.exists(DEDUPED_OUT): raise FileNotFoundError("deduplicated data missing")
    process_refining(DEDUPED_OUT, REFINED_OUT, BLACKLIST_CONFIG)
    return "Done"

def run_stage_4():
    from step4_data_mixing import mix_data
    from step5_synthesis_gemini import distill_with_gemini
    if not os.path.exists(REFINED_OUT): raise FileNotFoundError("label data Missing")
    recipe = {"code": 0.4, "finance": 0.3, "general": 0.3}
    mix_data(REFINED_OUT, FINAL_TRAIN_SET, target_total=100, recipe=recipe)
    distill_with_gemini(FINAL_TRAIN_SET, SYNTHETIC_OUT, sample_limit=5)
    return "Done"