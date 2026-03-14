import json
import os
import re

class DataRefiner:
    """
    Handles quality filtering and domain classification for raw datasets.
    Implements length checks, symbol density analysis, and blacklist filtering.
    """
    def __init__(self, blacklist_path=None):
        # 1. Define Domain Classification Rules (Regex)
        self.domain_rules = {
            'code': r'(python|java|javascript|<html>|import pandas|def |function\()',
            'finance': r'(stock|fund|investment|interest rate|earnings report|wall street|market share)',
            'medical': r'(diagnosis|treatment|clinical|symptoms|doctor|vaccine|hospital)',
            'edu': r'(assignment|curriculum|exam|knowledge point|textbook|university|learning)',
            'entertainment': r'(entertainment|celebrity|gossip|singer|actor|movie|tv show)'
        }
        
        # 2. Dynamic Blacklist Loading
        self.blacklist = []
        if blacklist_path and os.path.exists(blacklist_path):
            with open(blacklist_path, 'r', encoding='utf-8') as f:
                # Sanitize lines and remove empty entries
                self.blacklist = [line.strip() for line in f if line.strip()]
            print(f"📖 Successfully loaded {len(self.blacklist)} blacklist keywords.")
        else:
            print("⚠️ Warning: Blacklist file not found. Keyword filtering will be skipped.")

    def is_high_quality(self, text):
        """
        Heuristic quality filtering logic.
        """
        # Rule A: Minimum Length Filter (Discard stubs/short text)
        if len(text) < 150: 
            return False
        
        # Rule B: Symbol Density Filter (Detects garbled text or code-heavy noise)
        # Calculates the ratio of non-alphanumeric characters
        special_chars = len(re.findall(r'[^\u4e00-\u9fa5a-zA-Z0-9\s]', text))
        if special_chars / len(text) > 0.3: 
            return False
        
        # Rule C: Dynamic Blacklist Matching
        for word in self.blacklist:
            if word in text: 
                return False
            
        return True

    def get_domain(self, text):
        """Identifies the domain category based on regex rules."""
        text_lower = text.lower()
        for domain, pattern in self.domain_rules.items():
            if re.search(pattern, text_lower):
                return domain
        return "general"

def process_refining(input_file, output_file, blacklist_file):
    """Execution wrapper for the refining process."""
    refiner = DataRefiner(blacklist_path=blacklist_file)
    stats = {}
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    print(f"🚀 Processing: {input_file}")
    
    with open(input_file, 'r', encoding='utf-8') as f, \
         open(output_file, 'w', encoding='utf-8') as out_f:
        
        for i, line in enumerate(f):
            try:
                data = json.loads(line)
                content = data.get('raw_content', '')
                
                # Apply Quality Filters
                if not refiner.is_high_quality(content):
                    continue
                
                # Apply Domain Classification
                domain = refiner.get_domain(content)
                
                # Update Metadata
                if 'metadata' not in data:
                    data['metadata'] = {}
                data['metadata']['domain'] = domain
                
                # Track statistics
                stats[domain] = stats.get(domain, 0) + 1
                
                out_f.write(json.dumps(data, ensure_ascii=False) + '\n')
            except Exception as e:
                # Silent skip for corrupted lines to keep pipeline running
                continue
            
    print(f"✅ Refining complete! Domain distribution: {stats}")

if __name__ == "__main__":
    # Standardized Configuration Paths
    BLACKLIST_PATH = os.path.join("config", "blacklist.txt")
    INPUT_PATH = os.path.join("data", "deduped", "zh_deduped.jsonl")
    OUTPUT_PATH = os.path.join("data", "cleaned", "zh_filtered.jsonl")
    
    process_refining(INPUT_PATH, OUTPUT_PATH, BLACKLIST_PATH)