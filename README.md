# 🌌 Auto Data Processing System

An automated LLMs data governance pipeline that transforms raw data into high-quality synthetic Q&A pairs.

### 🚀 Features
* **Stage 1-4**: Multi-stage cleaning (Language routing, MinHash deduplication, Quality tagging).
* **Stage 6**: Gemini-powered distillation into structured JSONL.
* **Dashboard**: Streamlit-based "Advanced Gray" monitoring terminal.

### 🛠️ Tech Stack
* **Engine**: Python 3.10+
* **LLM**: Gemini 2.5 Flash / deepseek 
* **UI**: Streamlit & Plotly
* **Data**: Pandas 

### 📦 Setup
1. Clone the repo.
2. Install dependencies: `pip install -r requirements.txt`.
3. Set your `API_KEY` in `main_test.py`.
4. Run: `streamlit run app.py`.
<img width="1908" height="882" alt="image" src="https://github.com/user-attachments/assets/69007fc8-dad4-48f7-9b2b-c2dd2553a58b" />
