import os
import json
import time
import google.generativeai as genai
from google.api_core import exceptions

# 1. API Configuration
# Note: In a production environment, consider using os.environ.get("GEMINI_API_KEY")
genai.configure(api_key="YOUR_API_KEY_HERE")

def distill_with_gemini(input_file, output_file, sample_limit=5):
    """
    Automated data distillation pipeline using Gemini 1.5 Flash.
    Transforms raw text into high-quality synthetic Q&A pairs.
    """
    try:
        # Standard model selection for stable free-tier usage
        model = genai.GenerativeModel('gemini-2.5-flash')
    except Exception as e:
        print(f"❌ Model initialization failed: {e}")
        return
    
    print("🌟 Initializing Gemini Data Distillation Pipeline...")
    
    synthesized_data = []
    
    if not os.path.exists(input_file):
        print(f"❌ Input file error: {input_file} does not exist.")
        return

    with open(input_file, 'r', encoding='utf-8') as f:
        # Load sample lines for processing
        lines = f.readlines()[:sample_limit]

    for i, line in enumerate(lines):
        try:
            data = json.loads(line)
            # Context window management: selecting relevant snippet
            raw_text = data.get('raw_content', '')[:1000] 
            
            prompt = f"""
            You are a senior data engineer specializing in LLM training sets. 
            Analyze the raw web text below, remove noise/artifacts, and 
            distill the core information into a structured JSON object.
            
            Schema requirements:
            {{
                "instruction": "A specific, high-intent question derived from the content",
                "output": "A detailed, factual, and logically sound response"
            }}
            
            Raw Content:
            {raw_text}
            """

            # 3. Execution with automated retry logic for rate limits
            while True:
                try:
                    response = model.generate_content(
                        prompt,
                        generation_config={"response_mime_type": "application/json"}
                    )
                    break 
                except exceptions.ResourceExhausted:
                    # Specific handling for 429 Daily/RPM Quota errors
                    print(f"⚠️ Quota limit reached for item {i+1}. Cooling down for 60s...")
                    time.sleep(60)
                except Exception as e:
                    print(f"❌ API Error: {e}")
                    raise e

            # 4. Success handling and parsing
            result = json.loads(response.text)
            result['source_id'] = data.get('id', i)
            synthesized_data.append(result)
            
            print(f"✅ Record {i+1}/{sample_limit} processed successfully.")
            
            # Standard delay to maintain RPM (Requests Per Minute) health
            time.sleep(4) 

        except Exception as e:
            print(f"❌ Failed to process record {i+1}: {e}")

    # 5. Persistent Storage
    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with open(output_file, 'w', encoding='utf-8') as out_f:
        for item in synthesized_data:
            out_f.write(json.dumps(item, ensure_ascii=False) + '\n')

    print(f"🎉 Pipeline complete. Distilled dataset saved at: {output_file}")

if __name__ == "__main__":
    # Standardized file pathing
    INPUT_PATH = os.path.join("data", "cleaned", "zh_filtered.jsonl")
    OUTPUT_PATH = os.path.join("data", "distilled", "gemini_distilled_data.jsonl")
    
    distill_with_gemini(INPUT_PATH, OUTPUT_PATH)