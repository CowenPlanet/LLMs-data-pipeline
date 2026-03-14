import json
import os
from openai import OpenAI

# 1. API Configuration
# Note: DeepSeek uses the OpenAI-compatible SDK
client = OpenAI(
    api_key="YOUR_DEEPSEEK_API_KEY", 
    base_url="https://api.deepseek.com"
)

def distill_and_synthesize(input_file, output_file, sample_limit=5):
    """
    Reads raw data and uses DeepSeek to synthesize high-quality training pairs.
    """
    print(f"Reading from: {input_file}")
    synthesized_data = []
    
    if not os.path.exists(input_file):
        print("Error: Input file does not exist.")
        return

    # Load data and filter empty lines
    with open(input_file, 'r', encoding='utf-8') as f:
        lines = [line for line in f if line.strip()][:sample_limit]
        
    print(f"Preparing to distill {len(lines)} records...")

    for i, line in enumerate(lines):
        try:
            data = json.loads(line)
            # Limit context to first 1000 characters for efficiency
            raw_text = data.get('raw_content', '')[:1000]
            
            prompt = f"""
            You are a professional LLM training data engineer.
            Please analyze the raw web text below, filter out noise (ads, navigation bars, etc.), 
            and rewrite the core knowledge into a high-quality JSON object.
            
            Output Requirements:
            {{
                "instruction": "An in-depth question based on the content",
                "output": "A professional, detailed, and logically structured answer"
            }}
            
            Raw Content:
            {raw_text}
            """
            
            # DeepSeek-Chat request
            response = client.chat.completions.create(
                model="deepseek-chat",
                messages=[{"role": "user", "content": prompt}],
                # Enforce structured JSON output
                response_format={"type": "json_object"}
            )
            
            content = response.choices[0].message.content
            result = json.loads(content)
            
            # Metadata tracking
            result['source_url'] = data.get('source_url', 'n/a')
            synthesized_data.append(result)
            print(f"✅ Successfully synthesized record {i+1}")
            
        except Exception as e:
            print(f"❌ Record {i+1} failed: {e}")

    # Output Directory Management
    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Exporting results to JSONL
    with open(output_file, 'w', encoding='utf-8') as out_f:
        for item in synthesized_data:
            out_f.write(json.dumps(item, ensure_ascii=False) + '\n')
    
    print(f"🏁 Task complete. Output saved to: {output_file}")

if __name__ == "__main__":
    # Standardized paths for data pipeline
    INPUT_PATH = os.path.join("data", "cleaned", "zh_filtered.jsonl")
    OUTPUT_PATH = os.path.join("data", "distilled", "deepseek_synthetic_data.jsonl")
    
    distill_and_synthesize(INPUT_PATH, OUTPUT_PATH)