import streamlit as st
import pandas as pd
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'gravitic-macro'))

from core.synthesis.hybrid_rag import HybridRAGEngine
from core.models import EarningsReport, KPI, ExecutiveSummary, Guidance
from core.analysis.sandbagging import SandbaggingAnalyzer
from core.analysis.contagion import ContagionGraph
from core.fusion.nebula_bridge import NebulaBridge
from core.export.auto_modeler import AutoModeler

# Page Config
st.set_page_config(
    page_title="15-Min Financial Analyst",
    page_icon="📈",
    layout="wide"
)

# Custom CSS for premium look
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0.5rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        border-radius: 12px;
        padding: 1.5rem;
        margin: 0.5rem 0;
    }
    .stTextInput > div > div > input {
        background-color: #1e1e1e;
        color: white;
        border-radius: 8px;
    }
</style>
""", unsafe_allow_html=True)

# Initialize RAG Engine (singleton pattern)
@st.cache_resource
def get_rag_engine():
    engine = HybridRAGEngine()
    # Seed with demo data if empty
    try:
        if engine.collection.count() == 0:
            seed_demo_data(engine)
    except:
        seed_demo_data(engine)
    return engine

def seed_demo_data(engine):
    """Seeds demo data for testing."""
    documents = [
        {
            "id": "nvda_q3_2024_revenue",
            "text": "NVIDIA reported record revenue of $18.12 billion for Q3 FY2024, up 206% year-over-year. Data Center revenue hit an all-time high of $14.51 billion, driven by strong demand for AI infrastructure.",
            "metadata": {"ticker": "NVDA", "fiscal_period": "Q3 FY2024", "topic": "Revenue"}
        },
        {
            "id": "nvda_q3_2024_ai_outlook",
            "text": "Jensen Huang stated that NVIDIA is at the center of the generative AI revolution. Demand for H100 GPUs continues to outpace supply, with customers accelerating their AI workload deployments.",
            "metadata": {"ticker": "NVDA", "fiscal_period": "Q3 FY2024", "topic": "AI Outlook"}
        },
        {
            "id": "amd_q3_2024_datacenter",
            "text": "AMD's Data Center segment revenue was $1.6 billion, up 21% year-over-year. The MI300 AI accelerator is ramping production, with AMD targeting $2 billion in AI GPU revenue for 2024.",
            "metadata": {"ticker": "AMD", "fiscal_period": "Q3 FY2024", "topic": "Data Center"}
        },
        {
            "id": "intc_q3_2024_challenges",
            "text": "Intel reported Data Center revenue of $3.8 billion, down 10% YoY. Management acknowledged challenges in the AI accelerator market and is focusing on cost reductions and process technology improvements.",
            "metadata": {"ticker": "INTC", "fiscal_period": "Q3 FY2024", "topic": "Challenges"}
        }
    ]
    engine.add_documents(documents)

# --- MAIN APP ---
st.markdown('<p class="main-header">📈 15-Minute Financial Analyst</p>', unsafe_allow_html=True)
st.markdown("*AI-powered earnings analysis with Hybrid RAG*")

# Sidebar
st.sidebar.header("⚙️ Settings")
api_key = st.sidebar.text_input("Google API Key", type="password", help="For live Gemini 2.0 extraction")
if api_key:
    os.environ["GOOGLE_API_KEY"] = api_key
    st.sidebar.success("API Key Set!")

# Tabs
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "🔍 Competitor Search (RAG)", 
    "📄 Analyze New Report", 
    "📊 Dashboard", 
    "🔮 Predictive CFO", 
    "🕸️ Supply Chain",
    "🪐 Nebula Alpha",
    "📡 Macro Radar"
])

with tab1:
    st.subheader("Cross-Company Competitor Analysis")
    st.markdown("Search across all indexed earnings reports using Hybrid RAG (BM25 + Semantic).")
    
    query = st.text_input("Enter your query:", placeholder="e.g., What are semiconductor companies saying about AI demand?")
    
    if st.button("Search", type="primary"):
        if query:
            with st.spinner("Searching with Hybrid RAG..."):
                engine = get_rag_engine()
                results = engine.search(query, top_k=5)
                
                if results:
                    nebula = NebulaBridge()
                    for i, res in enumerate(results):
                        ticker_val = res['metadata']['ticker']
                        with st.expander(f"**{ticker_val}** - {res['metadata']['topic']} ({res['metadata']['fiscal_period']})", expanded=(i==0)):
                            st.markdown(f"> {res['text']}")
                            
                            # Fusion Logic: Pull Nebula Alt-Data if available
                            alt_data = nebula.get_company_signals(ticker_val)
                            if alt_data:
                                st.info("**Nebula Alpha Signal Found:**")
                                st.markdown(nebula.get_alpha_context(ticker_val))
                else:
                    st.warning("No results found. Try a different query.")
        else:
            st.warning("Please enter a query.")

with tab2:
    st.subheader("Analyze a New Earnings Report")
    st.markdown("Paste a press release or upload a slidedeck PDF.")
    
    col1, col2 = st.columns(2)
    with col1:
        ticker = st.text_input("Ticker Symbol", placeholder="NVDA")
    with col2:
        period = st.text_input("Fiscal Period", placeholder="Q4 FY2024")
    
    text_input = st.text_area("Paste Press Release Text:", height=200)
    
    if st.button("Analyze", type="primary"):
        if ticker and text_input:
            st.info("🔄 Extracting structured data... (This would call Gemini 2.0 in live mode)")
            # Mock KPIs for demo
            mock_kpis = [
                {"name": "Revenue", "value_actual": "$18.12B", "growth_yoy": "+206%", "source_text": "Revenue was a record $18.12B."},
                {"name": "Data Center Revenue", "value_actual": "$14.51B", "growth_yoy": "+279%", "source_text": "Data Center revenue hit $14.51B."},
                {"name": "EPS (Diluted)", "value_actual": "$4.02", "growth_yoy": "+593%", "source_text": "GAAP EPS was $4.02."}
            ]
            st.success("✅ Analysis Complete!")
            st.json({
                "ticker": ticker,
                "fiscal_period": period,
                "kpis": mock_kpis,
                "summary": "This is a mock summary. In live mode, Gemini 2.0 would extract this."
            })
            
            # Auto-Modeler Download
            st.markdown("---")
            st.subheader("⏬ Download Master Model")
            modeler = AutoModeler(ticker.upper(), period)
            excel_bytes = modeler.generate(mock_kpis)
            st.download_button(
                label="Download Excel Model",
                data=excel_bytes,
                file_name=modeler.get_filename(),
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        else:
            st.warning("Please enter ticker and press release text.")

with tab3:
    st.subheader("Indexed Reports Dashboard")
    engine = get_rag_engine()
    count = engine.collection.count()
    st.metric("Total Documents Indexed", count)
    
    st.markdown("---")
    st.markdown("*This dashboard would show historical reports, trends, and watchlist management in a full implementation.*")

with tab4:
    st.subheader("🔮 The Predictive CFO (Sandbagging Detector)")
    st.markdown("Analyze management's historical tendency to 'sandbag' (guide conservatively).")
    
    ticker_sb = st.text_input("Ticker Symbol", value="NVDA", key="sb_ticker")
    guidance_input = st.number_input("Current Revenue Guidance Midpoint ($B)", value=24.0, step=0.1)
    
    if st.button("Run Sandbagging Analysis", type="primary"):
        analyzer = SandbaggingAnalyzer()
        stats = analyzer.calculate_coefficient(ticker_sb)
        
        if stats['history'].empty:
            st.warning(f"No historical data found for {ticker_sb}.")
        else:
            st.success("Analysis Complete!")
            
            # Metrics
            c1, c2, c3 = st.columns(3)
            c1.metric("Sandbagging Coefficient", f"{stats['coefficient']*100:.1f}%", help="Avg % they beat their own guidance")
            c2.metric("Consistency Score", f"{int(stats['consistency']*100)}%", help="% of quarters they beat guidance")
            pred = analyzer.predict_actual(guidance_input, ticker_sb)
            c3.metric("Predicted Actual", f"${pred}B", delta=f"{stats['coefficient']*100:.1f}%")
            
            st.info(f"**AI Insight:** {stats['narrative']}")
            
            # Chart
            st.subheader("Historical vs. Actuals")
            df = stats['history']
            df['Fiscal_Period'] = df['Quarter']
            chart_data = df[['Fiscal_Period', 'Guidance', 'Actual']].set_index('Fiscal_Period')
            st.bar_chart(chart_data)
            
            st.dataframe(df)

with tab5:
    st.subheader("🕸️ Supply Chain Contagion Graph")
    st.markdown("Visualize upstream supplier risks helping you anticipate delays before they are reported.")
    
    ticker_chain = st.text_input("Root Ticker", value="NVDA", key="chain_ticker")
    
    if st.button("Analyze Supply Chain Risk", type="primary"):
        cg = ContagionGraph()
        
        # Risk Analysis
        status, flags = cg.analyze_risk(ticker_chain)
        
        c1, c2 = st.columns(2)
        c1.metric("Overall Contagion Risk", status, delta="-Critial" if status=="High" else "Safe", delta_color="inverse")
        c2.metric("Direct Suppliers Monitored", len(cg.get_suppliers(ticker_chain)))
        
        if flags:
            st.error("⚠️ **Active Risk Flags Detected:**")
            for flag in flags:
                st.write(f"- {flag}")
        else:
            st.success("✅ No active contagion risks detected.")
            
        # Graph
        st.subheader("Dependency Graph")
        try:
            graph = cg.generate_graph(ticker_chain)
            st.graphviz_chart(graph)
        except Exception as e:
            st.error(f"Graph generation failed: {e}")
            st.info("Ensure graphviz is installed.")

with tab6:
    st.subheader("🪐 Nebula Alpha: Alternative Data Intelligence")
    st.markdown("High-alpha insights fused from hiring velocity, shipping volume, and digital footprints.")
    
    ticker_nebula = st.text_input("Deep Dive Ticker", value="NVDA", key="nebula_ticker")
    
    if st.button("Query Nebula Signal Store", type="primary"):
        nebula = NebulaBridge()
        signals = nebula.get_company_signals(ticker_nebula)
        
        if not signals:
            st.warning(f"No alternative data cached for {ticker_nebula}. Launching Nebula scanner...")
            # In a full implementation, this could trigger a live scrape via subprocess or RPC
            st.info("Scanner running... (Mocked: 0 Firecrawl credits consumed as this would check existing caches first)")
        else:
            st.success(f"Found {len(signals)} operational signals for {ticker_nebula}")
            
            c1, c2, c3 = st.columns(3)
            
            if 'hiring' in signals:
                h = signals['hiring']
                c1.metric("Hiring Velocity", h.get('expansion_velocity', 0), help="R&D/Sales focus in job bank")
                c1.write(f"**Macro:** {h.get('total_open_roles_macro')} roles")
            
            if 'shipping' in signals:
                s = signals['shipping']
                c2.metric("Shipment Index", s.get('total_inventory_incoming_teu', 0), help="Incoming TEU volume")
                c2.write(f"**Trend:** {s.get('signal_strength')}")
                
            if 'digital' in signals:
                d = signals['digital']
                c3.metric("App Rank", d.get('current_value', "N/A"), help="Real-time app store traction")
                c3.write(f"**Sentiment:** {d.get('signal')}")

            st.markdown("---")
            st.markdown("#### Operational Narrative")
            st.markdown(nebula.get_alpha_context(ticker_nebula))

with tab7:
    st.subheader("📡 Polymarket Macro Radar")
    st.markdown("Real-time 'Market-Implied Truth' from prediction markets. Bypasses news noise.")
    
    col1, col2 = st.columns([1, 2])
    with col1:
        ticker_macro = st.text_input("Ticker Symbol", value="NVDA", key="macro_ticker")
    
    if st.button("Scan Prediction Markets", type="primary"):
        from core.fusion.macro_bridge import MacroBridge
        bridge = MacroBridge()
        
        c1, c2 = st.columns(2)
        
        # 1. Ticker Specific (LIVE SCAN)
        with c1:
            st.markdown(f"### 🎯 {ticker_macro} Predictions")
            with st.spinner(f"🔍 Deep searching local index for {ticker_macro}..."):
                # Use run_live_scan for parity with Polymarket UI
                signals = bridge.run_live_scan(ticker_macro)
                
                if not signals:
                    st.warning(f"No active prediction markets found for {ticker_macro}.")
                    st.info("Try updating the Local Index via 'gravitic-macro'.")
                else:
                    for s in signals:
                        st.markdown(f"#### {s['title']}")
                        
                        # Handle Multi-Outcome
                        if s.get('outcomes'):
                            # Create a clean dataframe for outcomes
                            outcomes_df = pd.DataFrame(s['outcomes']).sort_values(by='probability', ascending=False)
                            st.table(outcomes_df.rename(columns={'label': 'Outcome', 'probability': 'Odds'}).style.format({'Odds': '{:.1%}'}))
                        else:
                            # Standard Binary Metric
                            prob = s['probability_yes']
                            st.metric(
                                label="Market Probability",
                                value=f"{prob:.1%}",
                                delta=f"Vol: ${s['volume_usd']:,.0f}" if s.get('volume_usd') else None
                            )
                            st.progress(prob)
                        st.divider()

        # 2. Global Macro
        with c2:
            st.markdown("### 🌍 Global Macro Risks")
            with st.spinner("Fetching global context..."):
                macro = bridge.get_macro_risk_signals()
                if not macro.empty:
                    for _, row in macro.iterrows():
                        st.caption(f"{row['event_title']}")
                        st.progress(row['probability_yes'], text=f"{row['probability_yes']:.1%} Chance")
                else:
                    st.info("No global macro signals available.")

# Footer
st.markdown("---")
st.markdown("Built with ❤️ using Gemini 2.0, ChromaDB, BM25, and Streamlit | Open Source MVP")
