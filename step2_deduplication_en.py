import json
import os
import re
from datasketch import MinHash, MinHashLSH

def get_english_shingles(text, n=3):
    """
    Converts English text into a set of n-grams (Shingles).
    Example: "Data is gold" with n=2 -> {"Data is", "is gold"}
    """
    # Pre-processing: Lowercase and remove non-alphanumeric characters
    text = re.sub(r'[^a-zA-Z0-9\s]', '', text.lower())
    words = text.split()
    
    # Generate n-grams
    return [" ".join(words[i:i+n]) for i in range(len(words)-n+1)]

def deduplicate_en_data(input_file, output_file, threshold=0.85):
    """
    Performs MinHash + LSH deduplication specifically optimized for English text.
    """
    # Initialize LSH index with 128 permutations
    lsh = MinHashLSH(threshold=threshold, num_perm=128)
    
    unique_count = 0
    total_count = 0
    
    print(f"🧹 Starting English Deduplication: {input_file}")
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    with open(input_file, 'r', encoding='utf-8') as f, \
         open(output_file, 'w', encoding='utf-8') as out_f:
        
        for line in f:
            total_count += 1
            try:
                data = json.loads(line)
                content = data.get('raw_content', '')
                if not content:
                    continue
                
                # 1. Generate MinHash signature using 3-gram Shingles
                m = MinHash(num_perm=128)
                shingles = get_english_shingles(content, n=3)
                
                # Filter out records with insufficient content
                if not shingles:
                    continue
                
                for s in shingles:
                    m.update(s.encode('utf8'))
                
                # 2. Query LSH for near-duplicates
                result = lsh.query(m)
                
                if not result:
                    # 3. If unique, insert into index and save
                    doc_id = f"en_doc_{unique_count}"
                    lsh.insert(doc_id, m)
                    
                    if 'metadata' not in data:
                        data['metadata'] = {}
                    data['metadata']['process_stage'] = "en_deduplicated"
                    
                    out_f.write(json.dumps(data, ensure_ascii=False) + '\n')
                    unique_count += 1
                
                if total_count % 100 == 0:
                    print(f"🔄 Processed {total_count} records | Retained {unique_count} unique...")
            except Exception as e:
                continue

    retention_rate = (unique_count / total_count) * 100 if total_count > 0 else 0
    print(f"\n✅ English Deduplication Complete!")
    print(f"📊 Retention Rate: {retention_rate:.2f}% ({unique_count}/{total_count})")

if __name__ == "__main__":
    # Standardized system paths
    INPUT_EN = os.path.join("data", "routed", "en", "data.jsonl")
    OUTPUT_EN = os.path.join("data", "deduped", "en_deduped.jsonl")
    
    if os.path.exists(INPUT_EN):
        deduplicate_en_data(INPUT_EN, OUTPUT_EN)
    else:
        print(f"❌ Error: English routed data not found at {INPUT_EN}")