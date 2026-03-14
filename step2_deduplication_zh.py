import json
import os
import jieba
from datasketch import MinHash, MinHashLSH

def deduplicate_zh_data(input_file, output_file, threshold=0.85):
    """
    使用 MinHash + LSH 对中文数据进行模糊去重。
    threshold: 相似度阈值。0.85 表示如果两段话 85% 相似，就只保留一条。
    """
    # 1. 初始化 LSH 索引
    # num_perm (哈希置换次数) 设为 128，这是工业界平衡精度与速度的标准配置
    lsh = MinHashLSH(threshold=threshold, num_perm=128)
    
    unique_count = 0
    total_count = 0
    
    print(f"🧹 开始去重处理: {input_file}")
    
    # 确保输出目录存在
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    with open(input_file, 'r', encoding='utf-8') as f, \
         open(output_file, 'w', encoding='utf-8') as out_f:
        
        for line in f:
            total_count += 1
            try:
                data = json.loads(line)
                content = data.get('raw_content', '')
                if not content: continue
                
                # 2. 生成 MinHash 签名
                m = MinHash(num_perm=128)
                # 重要：中文必须分词后 Hash，否则按字 Hash 效果极差
                words = jieba.lcut(content)
                for word in words:
                    m.update(word.encode('utf8'))
                
                # 3. 检索 LSH 发现重复项
                # query 返回所有相似的 doc_id
                result = lsh.query(m)
                
                if not result:
                    # 4. 如果没有重复项，存入结果并插入 LSH 索引
                    doc_id = f"doc_{unique_count}"
                    lsh.insert(doc_id, m)
                    
                    # 记录一下去重后的阶段
                    data['process_stage'] = "deduplicated"
                    out_f.write(json.dumps(data, ensure_ascii=False) + '\n')
                    unique_count += 1
                
                if total_count % 100 == 0:
                    print(f"🔄 已扫描 {total_count} 条，目前保留唯一数据 {unique_count} 条...")
            
            except Exception as e:
                continue

    print(f"\n✅ 去重任务完成！")
    print(f"📊 原始数据: {total_count}")
    print(f"✨ 唯一数据: {unique_count}")
    print(f"📉 冗余率: {((total_count - unique_count) / total_count * 100):.2f}%")

if __name__ == "__main__":
    # 配置你的路径
    input_zh = r"data\routed\zh\data.jsonl"
    output_zh = r"data\deduped\zh_deduped.jsonl"
    
    if os.path.exists(input_zh):
        deduplicate_zh_data(input_zh, output_zh)
    else:
        print("❌ 错误：找不到分流后的中文数据，请确认 stage2 运行成功。")