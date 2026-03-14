import json
import os
import re

class DataRefiner:
    def __init__(self, blacklist_path=None):
        # 1. 定义领域关键词
        self.domain_rules = {
            'code': r'(python|java|javascript|<html>|import pandas|def |function\()',
            'finance': r'(股票|基金|投资|利率|财报|华尔街|market share)',
            'medical': r'(诊断|治疗|临床|症状|医生|疫苗|hospital|clinical)',
            'edu': r'(作业|课程|考试|知识点|教材|university|learning)',
            'entertainment':r'(娱乐|吃瓜|爆料|明星|歌手|艺人|演员|电影|电视剧)'
        }
        
        # 2. 从文件动态加载黑名单
        self.blacklist = []
        if blacklist_path and os.path.exists(blacklist_path):
            with open(blacklist_path, 'r', encoding='utf-8') as f:
                # 过滤掉空行和空格
                self.blacklist = [line.strip() for line in f if line.strip()]
            print(f"📖 已成功加载 {len(self.blacklist)} 个黑名单关键词。")
        else:
            print("⚠️ 警告：未找到黑名单文件，将跳过关键词过滤。")

    def is_high_quality(self, text):
        """
        启发式质量过滤逻辑。
        """
        # 规则 A: 长度过滤
        if len(text) < 150: return False
        
        # 规则 B: 符号密度过滤
        special_chars = len(re.findall(r'[^\u4e00-\u9fa5a-zA-Z0-9\s]', text))
        if special_chars / len(text) > 0.3: return False
        
        # 规则 C: 动态黑名单匹配
        for word in self.blacklist:
            if word in text: 
                return False
            
        return True

    def get_domain(self, text):
        """领域识别"""
        text_lower = text.lower()
        for domain, pattern in self.domain_rules.items():
            if re.search(pattern, text_lower):
                return domain
        return "general"

def process_refining(input_file, output_file, blacklist_file):
    refiner = DataRefiner(blacklist_path=blacklist_file)
    stats = {}
    
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    with open(input_file, 'r', encoding='utf-8') as f, \
         open(output_file, 'w', encoding='utf-8') as out_f:
        
        for line in f:
            try:
                data = json.loads(line)
                content = data.get('raw_content', '')
                
                if not refiner.is_high_quality(content):
                    continue
                    
                domain = refiner.get_domain(content)
                data['metadata']['domain'] = domain
                stats[domain] = stats.get(domain, 0) + 1
                
                out_f.write(json.dumps(data, ensure_ascii=False) + '\n')
            except:
                continue
            
    print(f"✅ 处理完成！领域分布: {stats}")

if __name__ == "__main__":
    # 配置路径
    config_path = r"config\blacklist.txt"
    input_path = r"data\deduped\zh_deduped.jsonl"
    output_path = r"data\cleaned\zh_filtered.jsonl"
    
    process_refining(input_path, output_path, config_path)