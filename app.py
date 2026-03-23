import streamlit as st
import pandas as pd
import plotly.express as px
import json
import os
import logging
import main

# --- 1. ENVIRONMENT INITIALIZATION ---
if not os.path.exists("log"): os.makedirs("log")
logging.basicConfig(filename='log/error.txt', level=logging.ERROR, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

st.set_page_config(page_title="CowenPlanet Only", layout="wide", page_icon="🌌")

# --- 2. UI STYLE INJECTION (Advanced Gray + Neon Accents) ---
st.markdown("""
    <style>
    /* Global Background: Advanced Slate Gray Gradient */
    [data-testid="stAppViewContainer"], [data-testid="stHeader"] {
        background-color: #1e1e1e !important;
        background-image: radial-gradient(circle at 50% 50%, #2d2d2d 0%, #1a1a1a 100%) !important;
        color: #d1d1d1 !important;
    }

    /* Sidebar Customization */
    [data-testid="stSidebar"] {
        background-color: #151515 !important;
        border-right: 1px solid #333333;
    }

    /* Status Cards: Frosted Glass Effect */
    .status-card { 
        padding: 15px; 
        border-radius: 8px; 
        margin-bottom: 12px; 
        font-family: 'Consolas', monospace;
        border: 1px solid #3a3a3a;
        background: rgba(45, 45, 45, 0.4);
    }
    .waiting { color: #666666; border-left: 4px solid #444; }
    .running { 
        color: #00d4ff; border-left: 4px solid #00d4ff;
        box-shadow: 0 0 15px rgba(0, 212, 255, 0.1);
        animation: pulse 2s infinite; 
    }
    .success { color: #00ff88; border-left: 4px solid #00ff88; }
    .error { color: #ff4b4b; border-left: 4px solid #ff4b4b; }
    
    @keyframes pulse {
        0% { opacity: 1; }
        50% { opacity: 0.7; }
        100% { opacity: 1; }
    }

    /* Neon Metrics */
    div[data-testid="stMetric"] {
        background: rgba(255, 255, 255, 0.02) !important;
        border: 1px solid #333 !important;
        padding: 20px;
        border-radius: 12px;
    }

    /* BUTTONS RE-DESIGN: Advanced Gray Integration */
    /* Primary Action Button (Execute) */
    div.stButton > button:first-child {
        background-color: #252525 !important;
        color: #00d4ff !important;
        border: 1px solid #00d4ff !important;
        border-radius: 4px;
        font-weight: bold;
        transition: all 0.3s ease;
    }
    div.stButton > button:first-child:hover {
        background-color: #00d4ff !important;
        color: #1e1e1e !important;
        box-shadow: 0 0 15px rgba(0, 212, 255, 0.4);
    }

    /* Secondary Action Button (Reset) */
    div.stButton > button:nth-of-type(1) {
        /* Reset button typically shares the same selector if it's the only one, 
           but Streamlit renders them sequentially. */
    }
    
    /* Global Secondary Button Style (Reset) */
    [data-testid="stSidebar"] button {
        background-color: #252525 !important;
        color: #7a7a7a !important;
        border: 1px solid #444 !important;
    }
    [data-testid="stSidebar"] button:hover {
        color: #ff4b4b !important;
        border-color: #ff4b4b !important;
        background-color: rgba(255, 75, 75, 0.05) !important;
    }

    /* Terminal Headers */
    .tech-header {
        color: #00d4ff;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 3px;
        border-left: 5px solid #00d4ff;
        padding-left: 15px;
        margin-bottom: 30px;
    }

    /* Text Color Fix */
    h1, h2, h3, p, span, label { color: #e0e0e0 !important; }
    </style>
    """, unsafe_allow_html=True)

# --- 3. STATE INITIALIZATION ---
if 'steps' not in st.session_state:
    st.session_state.steps = {
        "Language Routing (ZH)": 0, 
        "MinHash Deduplication": 0, 
        "Quality Filtering/Tagging": 0, 
        "Gemini Data Distillation": 0
    }

# --- 4. SIDEBAR: REAL-TIME MONITORING ---
with st.sidebar:
    st.markdown('<h2 style="color:#00d4ff;">Pipeline</h2>', unsafe_allow_html=True)
    sidebar_placeholder = st.empty()

def update_sidebar():
    with sidebar_placeholder.container():
        for step, status in st.session_state.steps.items():
            if status == 0:
                st.markdown(f'<div class="status-card waiting">🔘 {step} : IDLE</div>', unsafe_allow_html=True)
            elif status == 1:
                st.markdown(f'<div class="status-card running">🌊 {step} : PROCESSING</div>', unsafe_allow_html=True)
            elif status == 2:
                st.markdown(f'<div class="status-card success">💎 {step} : COMPLETED</div>', unsafe_allow_html=True)
            else:
                st.markdown(f'<div class="status-card error">⚠️ {step} : HALTED</div>', unsafe_allow_html=True)

update_sidebar()
st.sidebar.divider()

# Execute Button (Neon Cyan Border)
if st.sidebar.button("🚀 INITIATE FULL PIPELINE", use_container_width=True):
    task_map = [
        ("Language Routing (ZH)", main.run_stage_1),
        ("MinHash Deduplication", main.run_stage_2),
        ("Quality Filtering/Tagging", main.run_stage_3),
        ("Gemini Data Distillation", main.run_stage_4)
    ]
    for name, func in task_map:
        st.session_state.steps[name] = 1
        update_sidebar()
        try:
            func()
            st.session_state.steps[name] = 2
            update_sidebar()
        except Exception as e:
            st.session_state.steps[name] = 3
            update_sidebar()
            logging.error(f"Error at {name}: {str(e)}")
            st.error(f"CRITICAL SYSTEM FAILURE: {name}")
            break
    st.rerun()


# --- 5. MAIN DASHBOARD ---
st.markdown('<h1 class="tech-header">Auto Data Processing System</h1>', unsafe_allow_html=True)

if os.path.exists(main.REFINED_OUT):
    try:
        with open(main.REFINED_OUT, 'r', encoding='utf-8') as f:
            df = pd.DataFrame([json.loads(line) for line in f])

        if 'metadata' in df.columns:
            df['tag'] = df['metadata'].apply(lambda x: x.get('domain', 'unknown') if isinstance(x, dict) else 'unknown')

        st.subheader("📊 PIPELINE PERFORMANCE ANALYSIS")
        m1, m2, m3 = st.columns(3)
        m1.metric("CLEANED_RETAINED", f"{len(df)} UNITS")
        if 'tag' in df.columns:
            tag_counts = df['tag'].value_counts()
            m2.metric("DOMAIN_CLUSTERS", f"{len(tag_counts)} CATS")
            m3.metric("PEAK_CONCENTRATION", tag_counts.idxmax().upper())

            st.write("")
            c1, c2 = st.columns([2, 3])
            with c1:
                st.write("📂 **DATA_INVENTORY_STREAM**")
                tag_df = tag_counts.reset_index()
                tag_df.columns = ['DOMAIN', 'COUNT']
                st.dataframe(tag_df, use_container_width=True)
            with c2:
                fig = px.bar(tag_df, x='DOMAIN', y='COUNT', color='DOMAIN', template="plotly_dark",
                             color_discrete_sequence=px.colors.sequential.Cividis_r)
                fig.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)', 
                    paper_bgcolor='rgba(0,0,0,0)', 
                    showlegend=False,
                    font=dict(color="#d1d1d1")
                )
                st.plotly_chart(fig, use_container_width=True)
    except Exception as e:
        st.error(f"⚠️ PARSE ENGINE EXCEPTION: {e}")

    st.markdown("---")
    st.markdown('<h3 style="color:#00d4ff;">✨ SYNTHETIC DATA STREAM (PREVIEW)</h3>', unsafe_allow_html=True)
    if os.path.exists(main.SYNTHETIC_OUT):
        st.dataframe(pd.read_json(main.SYNTHETIC_OUT, lines=True), use_container_width=True)
else:
    st.info("👋 TERMINAL STANDBY. AWAITING PIPELINE ACTIVATION...")