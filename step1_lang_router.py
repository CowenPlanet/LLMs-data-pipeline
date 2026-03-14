import json
import os
import langid

def language_router(input_file, output_dir):
    """
    Identifies the language of each record and routes it to the appropriate directory.
    Standardizes language codes and updates metadata for downstream processing.
    """
    print(f"🚀 Initializing Language Router for: {input_file}")
    
    # 1. Configuration & Mapping
    # Standardizing langid outputs to primary categories
    lang_map = {
        'zh': 'zh',
        'en': 'en'
    }

    # Pre-create directory structure for clean I/O
    for folder in ['zh', 'en', 'others']:
        os.makedirs(os.path.join(output_dir, folder), exist_ok=True)

    count = 0
    stats = {'zh': 0, 'en': 0, 'others': 0}

    if not os.path.exists(input_file):
        print(f"❌ Error: Input file not found at {input_file}")
        return

    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                line = line.strip()
                if not line:
                    continue
                
                data = json.loads(line)
                # Ensure compatibility by checking for the core content field
                text = data.get('raw_content', '')
                if not text:
                    continue

                # 2. Language Classification
                # Sampling the first 1000 characters for high-performance inference
                lang, score = langid.classify(text[:1000]) 

                # 3. Routing Logic
                target = lang_map.get(lang, 'others')
                stats[target] += 1
                
                # Enrich metadata with classification confidence
                if 'metadata' not in data:
                    data['metadata'] = {}
                data['metadata']['language'] = lang
                data['metadata']['lang_score'] = float(score)

                # 4. Append to target JSONL
                output_path = os.path.join(output_dir, target, "data.jsonl")
                # Using append mode 'a' for memory-efficient large file handling
                with open(output_path, 'a', encoding='utf-8') as out_f:
                    out_f.write(json.dumps(data, ensure_ascii=False) + '\n')

                count += 1
                if count % 100 == 0:
                    print(f"📊 Processed {count} records... Distribution: {stats}")

            except Exception as e:
                # Log specific line errors without halting the pipeline
                print(f"⚠️ Error processing record: {e}")
                continue

    # Final Summary Report
    print(f"\n✅ Routing Task Complete!")
    print(f"📂 Chinese (zh): {stats['zh']} records")
    print(f"📂 English (en): {stats['en']} records")
    print(f"📂 Others: {stats['others']} records")

if __name__ == "__main__":
    # Standardized paths for data lifecycle management
    INPUT_PATH = os.path.join("data", "processed", "raw_dump.jsonl") 
    OUTPUT_BASE = os.path.join("data", "routed")
    
    language_router(INPUT_PATH, OUTPUT_BASE)