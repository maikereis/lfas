import sys
import logging
from pathlib import Path
import shutil
import gc


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


def index_exists():
    """Check if both LMDB data and metadata exist"""
    lmdb_path = Path("./lmdb_data")
    metadata_path = Path("./lmdb_data/metadata.bin")
    
    # Check if lmdb directory exists and has data files
    has_lmdb = (lmdb_path / "data.mdb").exists() and (lmdb_path / "lock.mdb").exists()
    has_metadata = metadata_path.exists()
    
    return has_lmdb and has_metadata


def delete_index():
    """Delete existing index and metadata"""
    import gc
    
    # First, clear the cached engine to close LMDB
    st.cache_resource.clear()
    
    # Force garbage collection to ensure LMDB is closed
    gc.collect()
    
    # Small delay to ensure file handles are released
    time.sleep(0.5)
    
    # Now delete the directory
    lmdb_path = Path("./lmdb_data")
    if lmdb_path.exists():
        try:
            shutil.rmtree(lmdb_path)
            logging.info("Deleted existing index")
        except Exception as e:
            logging.error(f"Failed to delete index: {e}")
            # Try again after another delay
            time.sleep(1.0)
            gc.collect()
            shutil.rmtree(lmdb_path)
    
    # Recreate empty directory
    lmdb_path.mkdir(exist_ok=True)


@st.cache_resource
def get_engine():
    """Get or create the search engine singleton"""
    engine = PySearchEngine()
    
    metadata_path = Path("./lmdb_data/metadata.bin")
    
    if metadata_path.exists():
        try:
            engine.load_metadata(str(metadata_path))
            logging.info(f"Loaded index with {engine.get_total_docs()} docs")
        except Exception as e:
            logging.error(f"Failed to load metadata: {e}")
            st.error(f"Failed to load existing index: {e}")
    
    return engine


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
if 'index_loaded' not in st.session_state:
    st.session_state['index_loaded'] = index_exists()
if 'documents_loaded' not in st.session_state:
    st.session_state['documents_loaded'] = False


# Load engine only if index exists
engine = None
if st.session_state['index_loaded']:
    engine = get_engine()
    st.session_state['total_docs'] = engine.get_total_docs()


# 1. Sidebar for Stats & File Upload
with st.sidebar:
    st.subheader("Index Management")
    
    # Show index status
    if st.session_state['index_loaded']:
        st.success("✅ Index is loaded!")
        if st.session_state['total_docs'] > 0:
            st.metric("Total Docs", f"{st.session_state['total_docs']:,}")
        if st.session_state['build_time']:
            st.metric("Build Time", f"{st.session_state['build_time']:.2f}s")
        
        # Option to delete and rebuild
        st.divider()
        if st.button("🗑️ Delete Index & Rebuild", type="secondary"):
            delete_index()
            st.session_state['index_loaded'] = False
            st.session_state['total_docs'] = 0
            st.session_state['build_time'] = None
            st.success("Index deleted! Upload a new CSV to rebuild.")
            time.sleep(1)
            st.rerun()
    else:
        st.warning("⚠️ No index found")
        st.info("Please upload a CSV file below to create an index")
    
    st.divider()
    
    # Document loading status
    st.subheader("Documents")
    if st.session_state['documents_loaded']:
        st.success("✅ Documents loaded")
        if st.session_state['df'] is not None:
            st.metric("Rows", f"{len(st.session_state['df']):,}")
    else:
        st.warning("⚠️ Documents not loaded")
        st.info("Load documents to view search results")
    
    st.divider()
    
    # Search parameters
    st.subheader("Search Parameters")
    top_k = st.number_input("Top K", value=10, min_value=1, max_value=100)
    blocking_k = st.number_input("Blocking K", value=1000, min_value=100, max_value=1_000_000)
    
    # Timing logs expander
    st.divider()
    log_handler = st.session_state['log_handler']
    if log_handler.logs:
        with st.expander("📊 Timing Logs", expanded=False):
            # Filter for timing-related logs
            timing_logs = [log for log in log_handler.logs if 'TIMING' in log or 'SEARCH' in log or 'PROGRESS' in log]
            for log in timing_logs[-30:]:  # Show last 30 timing logs
                st.code(log, language=None)


# 2. Main Content Area - Tabs for different operations
tab1, tab2, tab3 = st.tabs(["🔍 Search", "📥 Index Documents", "📂 Load Documents"])

# TAB 1: Search (only shown when index is loaded)
with tab1:
    if not st.session_state['index_loaded']:
        st.warning("⚠️ No index available. Please create an index first in the 'Index Documents' tab.")
    else:
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
        
        # Results Display
        if submitted:
            active_query = {k: v for k, v in search_payload.items() if v.strip()}
            
            if not active_query:
                st.warning("⚠️ Please enter at least one search term.")
            else:
                try:
                    # Get engine
                    engine = get_engine()
                    
                    # Clear search-related logs before search
                    log_handler = st.session_state['log_handler']
                    search_log_start = len(log_handler.logs)
                    
                    start_s = time.time()
                    results = engine.search_complex(active_query, int(top_k), int(blocking_k))
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
                        # Display results
                        if not st.session_state['documents_loaded'] or st.session_state['df'] is None:
                            st.warning("⚠️ Documents not loaded. Load documents in the 'Load Documents' tab to see full results.")
                            for rank, (doc_id, score) in enumerate(results, 1):
                                st.text(f"#{rank} - Doc ID: {doc_id}, Score: {score:.2f}")
                        else:
                            df = st.session_state['df']
                            for rank, (doc_id, score) in enumerate(results, 1):
                                with st.container(border=True):
                                    sc, info = st.columns([1, 4])
                                    sc.metric("Score", f"{score:.2f}", f"Rank #{rank}")
                                    
                                    if doc_id < len(df):
                                        record = df.iloc[doc_id]
                                        
                                        tipo = record.get('tipo_logradouro', '')
                                        rua = record.get('rua', '')
                                        numero = record.get('numero', 'S/N')
                                        bairro = record.get('bairro', '')
                                        municipio = record.get('municipio', '')
                                        estado = record.get('estado', '')
                                        cep = record.get('cep', '')
                                        
                                        info.write(f"**{tipo}, {rua}, {numero}**")
                                        info.write(f"{bairro} — {municipio}, {estado}")
                                        info.caption(f"CEP: {cep} | ID: {doc_id}")
                                    else:
                                        info.error(f"Doc ID {doc_id} out of range")
                
                except Exception as e:
                    st.error(f"Error during search: {str(e)}")
                    st.exception(e)

# TAB 2: Index Documents
with tab2:
    st.subheader("📥 Create Search Index")
    
    if st.session_state['index_loaded']:
        st.info(f"✅ Index already exists with {st.session_state['total_docs']:,} documents. Delete it in the sidebar to create a new one.")
    else:
        uploaded_file = st.file_uploader("Upload CSV for Indexing", type="csv", key="index_upload")
        
        if uploaded_file:
            # Preview the file
            if st.session_state.get('index_df') is None:
                with st.spinner("Reading CSV..."):
                    st.session_state['index_df'] = pd.read_csv(uploaded_file)
            
            df = st.session_state['index_df']
            total_rows = len(df)
            st.info(f"Ready to index {total_rows:,} records.")
            
            with st.expander("Preview Data", expanded=False):
                st.dataframe(df.head(10))
            
            if st.button("🔥 Build Index", type="primary", use_container_width=True):
                # CRITICAL: Clear cache and force garbage collection BEFORE creating new engine
                st.cache_resource.clear()
                import gc
                gc.collect()
                time.sleep(0.5)  # Give time for LMDB to close
                
                # Create new engine
                engine = get_engine()
                
                progress_bar = st.progress(0)
                status_text = st.empty()
                timing_container = st.empty()
                
                # Performance metrics
                start_time = time.time()
                chunk_size = 500_000
                
                # Clear previous logs
                log_handler = st.session_state['log_handler']
                log_handler.logs.clear()
                
                for i in range(0, total_rows, chunk_size):
                    batch_start_time = time.time()
                    
                    chunk = df.iloc[i : i + chunk_size]
                    
                    batch_data = [
                        (int(idx), {k: str(v) for k, v in zip(chunk.columns, row) if pd.notna(v)})
                        for idx, row in zip(chunk.index, chunk.values)
                    ]
                    
                    engine.index_batch(batch_data)
                    
                    elapsed_batch = time.time() - batch_start_time
                    current_count = min(i + chunk_size, total_rows)
                    docs_per_sec = len(chunk) / elapsed_batch
                    
                    progress = current_count / total_rows
                    progress_bar.progress(progress)
                    status_text.text(f"Processed {current_count:,} / {total_rows:,} records...")
                    
                    timing_container.metric(
                        "Batch Speed", 
                        f"{docs_per_sec:,.0f} docs/sec",
                        f"{elapsed_batch:.2f}s for last batch"
                    )
                
                with st.spinner("Finalizing index (LMDB Flush)..."):
                    engine.flush()
                    engine.save_metadata("./lmdb_data/metadata.bin")
                
                build_duration = time.time() - start_time
                overall_rate = total_rows / build_duration
                
                st.session_state['build_time'] = build_duration
                st.session_state['total_docs'] = total_rows
                st.session_state['index_loaded'] = True
                st.session_state['index_df'] = None  # Clear temporary data
                
                st.success(f"Successfully indexed {total_rows:,} records in {build_duration:.2f}s!")
                st.metric("Overall Average Speed", f"{overall_rate:,.0f} docs/sec")
                
                time.sleep(2)
                st.rerun()

# TAB 3: Load Documents
with tab3:
    st.subheader("📂 Load Documents for Display")
    
    if st.session_state['documents_loaded']:
        st.success(f"✅ Documents loaded: {len(st.session_state['df']):,} rows")
        
        with st.expander("Preview Loaded Documents", expanded=False):
            st.dataframe(st.session_state['df'].head(20))
        
        if st.button("🗑️ Unload Documents", type="secondary"):
            st.session_state['df'] = None
            st.session_state['documents_loaded'] = False
            st.success("Documents unloaded from memory")
            time.sleep(1)
            st.rerun()
    else:
        st.info("Load the document CSV to view full search results with address details.")
        
        uploaded_file = st.file_uploader("Upload Document CSV", type="csv", key="doc_upload")
        
        if uploaded_file:
            if st.button("📂 Load Documents", type="primary", use_container_width=True):
                with st.spinner("Loading documents into memory..."):
                    st.session_state['df'] = pd.read_csv(uploaded_file)
                    st.session_state['documents_loaded'] = True
                
                st.success(f"Loaded {len(st.session_state['df']):,} documents!")
                time.sleep(1)
                st.rerun()