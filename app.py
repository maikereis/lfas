import sys
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)]
)


import time
import pandas as pd
import streamlit as st


from lfas import PySearchEngine
PySearchEngine.init_logging()

st.set_page_config(page_title="LFAS Pro", page_icon="⚡", layout="wide")
st.title("LFAS: High-Performance Address Search")

# Custom handler to capture logs for UI display
class StreamlitLogHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.logs = []
    
    def emit(self, record):
        log_entry = self.format(record)
        self.logs.append(log_entry)
        # Keep only last 100 logs
        if len(self.logs) > 100:
            self.logs.pop(0)

# Initialize session state
if 'engine' not in st.session_state:
    st.session_state['engine'] = None
if 'df' not in st.session_state:
    st.session_state['df'] = None
if 'build_time' not in st.session_state:
    st.session_state['build_time'] = None
if 'total_docs' not in st.session_state:
    st.session_state['total_docs'] = 0
if 'log_handler' not in st.session_state:
    handler = StreamlitLogHandler()
    logging.getLogger().addHandler(handler)
    st.session_state['log_handler'] = handler

# 1. Sidebar for Stats & File Upload
with st.sidebar:
    uploaded_file = st.file_uploader("Upload CSV", type="csv")
    top_k = st.number_input("Top K", value=10, min_value=1, max_value=100)
    
    # Debug info
    if st.session_state['engine'] is not None:
        st.success("✅ Index is ready!")
        if st.session_state['build_time']:
            st.metric("Build Time", f"{st.session_state['build_time']:.2f}s")
        if st.session_state['total_docs'] > 0:
            st.metric("Total Docs", f"{st.session_state['total_docs']:,}")
    
    # Timing logs expander
    log_handler = st.session_state['log_handler']
    if log_handler.logs:
        with st.expander("📊 Timing Logs", expanded=False):
            # Filter for timing-related logs
            timing_logs = [log for log in log_handler.logs if 'TIMING' in log or 'SEARCH' in log or 'PROGRESS' in log]
            for log in timing_logs[-30:]:  # Show last 30 timing logs
                st.code(log, language=None)

if uploaded_file:
    # Load the full file
    if st.session_state['df'] is None:
        with st.spinner("Reading full CSV into memory..."):
            st.session_state['df'] = pd.read_csv(uploaded_file)
    
    df = st.session_state['df']
    total_rows = len(df)
    st.info(f"Ready to index {total_rows:,} records.")

    if st.button("🔥 Index All Records"):
        engine = PySearchEngine()
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        timing_container = st.empty()
        
        # Performance metrics
        start_time = time.time()
        batch_start = start_time
        batch_size = 10000
        
        records = df.to_dict('records')
        
        # Clear previous logs
        log_handler.logs.clear()
        
        for i, row in enumerate(records):
            clean_record = {k: str(v) for k, v in row.items() if pd.notna(v)}
            engine.index_dict(i, clean_record)
            
            # Update UI every batch
            if i > 0 and i % batch_size == 0:
                batch_time = time.time() - batch_start
                docs_per_sec = batch_size / batch_time
                
                progress_bar.progress(i / total_rows)
                status_text.text(f"Processed {i:,} / {total_rows:,} records...")
                timing_container.metric(
                    "Indexing Speed", 
                    f"{docs_per_sec:,.0f} docs/sec",
                    f"{batch_time:.2f}s for last {batch_size:,} docs"
                )
                
                batch_start = time.time()
        
        engine.flush()

        # Final update
        progress_bar.progress(1.0)
        status_text.text(f"Processed {total_rows:,} / {total_rows:,} records...")
        
        build_duration = time.time() - start_time
        overall_rate = total_rows / build_duration
        
        st.session_state['engine'] = engine
        st.session_state['build_time'] = build_duration
        st.session_state['total_docs'] = total_rows
        
        st.success(f"Successfully indexed {total_rows:,} records in {build_duration:.2f}s!")
        st.metric("Overall Speed", f"{overall_rate:,.0f} docs/sec")
        st.rerun()

# 3. Multi-Field Search UI
st.subheader("🔍 Field-Aware Query")

with st.form("search_form"):
    r1c1, r1c2, r1c3 = st.columns(3)
    r2c1, r2c2, r2c3 = st.columns(3)
    r3c1, r3c2, r3c3 = st.columns(3)
    
    search_payload = {}
    search_payload["rua"] = r1c1.text_input("Rua/Logradouro")
    search_payload["municipio"] = r1c2.text_input("Município")
    search_payload["bairro"] = r1c3.text_input("Bairro")
    
    search_payload["cep"] = r2c1.text_input("CEP")
    search_payload["estado"] = r2c2.text_input("Estado (UF)")
    search_payload["tipo_logradouro"] = r2c3.text_input("Tipo (ex: Av, Rua)")
    
    search_payload["numero"] = r3c1.text_input("Número")
    search_payload["nome"] = r3c2.text_input("Nome/Identificador")
    search_payload["complemento"] = r3c3.text_input("Complemento")
    
    submitted = st.form_submit_button("Search", use_container_width=True)

# 4. Results Display
if submitted:
    if st.session_state['engine'] is None:
        st.error("⚠️ Please build the index first!")
        st.info("👆 Upload a CSV file and click 'Index All Records' in the sidebar.")
    else:
        active_query = {k: v for k, v in search_payload.items() if v.strip()}
        
        if not active_query:
            st.warning("⚠️ Please enter at least one search term.")
        else:
            try:
                # Clear search-related logs before search
                log_handler = st.session_state['log_handler']
                search_log_start = len(log_handler.logs)
                
                start_s = time.time()
                results = st.session_state['engine'].search_complex(active_query, int(top_k))
                search_time_ms = (time.time() - start_s) * 1000
                
                # Display timing breakdown
                col1, col2, col3 = st.columns(3)
                col1.metric("Search Time", f"{search_time_ms:.2f}ms")
                col2.metric("Results", len(results))
                if len(results) > 0:
                    col3.metric("Avg Score", f"{sum(s for _, s in results) / len(results):.2f}")
                
                # Show detailed timing logs from this search
                search_logs = [log for log in log_handler.logs[search_log_start:] 
                              if 'TIMING' in log or 'SEARCH' in log]
                if search_logs:
                    with st.expander("🔍 Search Timing Details", expanded=False):
                        for log in search_logs:
                            st.code(log, language=None)
                
                st.write(f"### Results ({len(results)})")
                
                if len(results) == 0:
                    st.info("No results found. Try different search terms.")
                else:
                    for rank, (doc_id, score) in enumerate(results, 1):
                        with st.container(border=True):
                            sc, info = st.columns([1, 4])
                            sc.metric("Score", f"{score:.2f}", f"Rank #{rank}")
                            
                            df = st.session_state['df']
                            record = df.iloc[doc_id]
                            
                            tipo = record.get('tipo_logradouro', '')
                            rua = record.get('rua', '')
                            numero = record.get('numero', 'S/N')
                            bairro = record.get('bairro', '')
                            municipio = record.get('municipio', '')
                            estado = record.get('estado', '')
                            cep = record.get('cep', '')
                            
                            info.write(f"**{tipo} {rua}, {numero}**")
                            info.write(f"{bairro} — {municipio}, {estado}")
                            info.caption(f"CEP: {cep} | ID: {doc_id}")
            
            except Exception as e:
                st.error(f"Error during search: {str(e)}")
                st.exception(e)