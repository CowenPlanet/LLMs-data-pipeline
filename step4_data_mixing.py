import json
import random
import os
from collections import defaultdict

def mix_dataset(input_file, output_file, target_total=1000, recipe=None):
    """
    Mixes data from different domains based on a specified ratio (recipe).
    
    Args:
        input_file (str): Path to the source JSONL file.
        output_file (str): Path where the mixed dataset will be saved.
        target_total (int): Total number of records for the final dataset.
        recipe (dict): Domain distribution ratio, e.g., {"code": 0.5, "finance": 0.3}
    """
    if recipe is None:
        recipe = {"general": 1.0}
    
    # 1. Indexing: Categorize data by domain
    pool = defaultdict(list)
    
    if not os.path.exists(input_file):
        print(f"❌ Error: Source file {input_file} not found.")
        return

    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                data = json.loads(line)
                # Default to 'general' if domain is missing
                domain = data.get('metadata', {}).get('domain', 'general')
                pool[domain].append(line)
            except json.JSONDecodeError:
                continue
            
    print(f"📊 Current Source Distribution: { {k: len(v) for k, v in pool.items()} }")

    # 2. Sampling: Extract records based on recipe ratios
    final_data = []
    for domain, ratio in recipe.items():
        count_to_sample = int(target_total * ratio)
        available_data = pool.get(domain, [])
        
        # Take all available data if the pool is smaller than the target sample size
        sampled = random.sample(available_data, min(count_to_sample, len(available_data)))
        final_data.extend(sampled)
        print(f"🎯 Domain [{domain}]: Target {count_to_sample}, Sampled {len(sampled)}")

    # 3. Shuffling: Randomize order to prevent model ordering bias
    random.shuffle(final_data)

    # 4. Export: Write the final training set
    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with open(output_file, 'w', encoding='utf-8') as out_f:
        for item in final_data:
            out_f.write(item)

    print(f"✅ Success: Dataset synthesized at {output_file} with {len(final_data)} total records.")

if __name__ == "__main__":
    # Example recipe for a "Tech + Finance" focused assistant
    MY_RECIPE = {
        "code": 0.4,
        "finance": 0.3,
        "general": 0.3
    }
    
    # Standardized system paths
    INPUT_PATH = os.path.join("data", "cleaned", "zh_filtered.jsonl")
    OUTPUT_PATH = os.path.join("data", "final", "train_set.jsonl")
    
    mix_dataset(INPUT_PATH, OUTPUT_PATH, target_total=500, recipe=MY_RECIPE)