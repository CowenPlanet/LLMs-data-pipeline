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

<img width="954" height="441" alt="2b6f1aa964494017d90cddb68e6e106" src="https://github.com/user-attachments/assets/7791bc25-10b0-4d90-a797-957a686b2984" />

<img width="954" height="441" alt="e1299e6f06d091cd98d1017c055adb2" src="https://github.com/user-attachments/assets/3d94b705-0aac-4921-b5c1-172c488d1b11" />

