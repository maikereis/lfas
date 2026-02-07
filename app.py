import streamlit as st
import pandas as pd
from lfas import PySearchEngine
import time

st.set_page_config(page_title="LFAS Pro", page_icon="⚡", layout="wide")
st.title("🇧🇷 LFAS: High-Performance Address Search")

# Initialize session state
if 'engine' not in st.session_state:
    st.session_state['engine'] = None
if 'df' not in st.session_state:
    st.session_state['df'] = None
if 'build_time' not in st.session_state:
    st.session_state['build_time'] = None

# 1. Sidebar for Stats & File Upload
with st.sidebar:
    uploaded_file = st.file_uploader("Upload CSV", type="csv")
    top_k = st.number_input("Top K", value=10, min_value=1, max_value=100)
    
    # Debug info
    if st.session_state['engine'] is not None:
        st.success("✅ Index is ready!")
        if st.session_state['build_time']:
            st.metric("Build Time", f"{st.session_state['build_time']:.2f}s")

if uploaded_file:
    # Load the full file without .head()
    if st.session_state['df'] is None:
        with st.spinner("Reading full CSV into memory..."):
            st.session_state['df'] = pd.read_csv(uploaded_file)
    
    df = st.session_state['df']
    total_rows = len(df)
    st.info(f"Ready to index {total_rows:,} records.")

    if st.button("🔥 Index All Records"):
        engine = PySearchEngine()
        start_time = time.time()
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Performance Tip: Use to_dict('records') for faster iteration than iterrows()
        records = df.to_dict('records')
        
        for i, row in enumerate(records):
            # Clean record: remove NaNs and stringify
            clean_record = {k: str(v) for k, v in row.items() if pd.notna(v)}
            
            # Send to Rust
            engine.index_dict(i, clean_record)
            
            # Update UI every 10k rows to avoid slowing down the loop
            if i % 10000 == 0:
                progress_bar.progress(i / total_rows)
                status_text.text(f"Processed {i:,} / {total_rows:,} records...")

        # Final update
        progress_bar.progress(1.0)
        status_text.text(f"Processed {total_rows:,} / {total_rows:,} records...")
        
        build_duration = time.time() - start_time
        st.session_state['engine'] = engine
        st.session_state['build_time'] = build_duration
        st.success(f"Successfully indexed {total_rows:,} records in {build_duration:.2f}s!")
        st.rerun()

# 3. Multi-Field Search UI
st.subheader("🔍 Field-Aware Query")

# Move the form logic here
with st.form("search_form"):
    # Grid for all 9 domain fields
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

# 4. Results Display (OUTSIDE the form)
if submitted:
    st.write("DEBUG: Form submitted!")  # Debug line
    
    if st.session_state['engine'] is None:
        st.error("⚠️ Please build the index first!")
        st.info("👆 Upload a CSV file and click 'Index All Records' in the sidebar.")
    else:
        # Filter out empty inputs
        active_query = {k: v for k, v in search_payload.items() if v.strip()}
        
        st.write(f"DEBUG: Active query fields: {list(active_query.keys())}")  # Debug line
        
        if not active_query:
            st.warning("⚠️ Please enter at least one search term.")
        else:
            try:
                start_s = time.time()
                results = st.session_state['engine'].search_complex(active_query, int(top_k))
                search_time_ms = (time.time() - start_s) * 1000
                
                st.write(f"### Results ({len(results)}) - {search_time_ms:.2f}ms")
                
                if len(results) == 0:
                    st.info("No results found. Try different search terms.")
                else:
                    for doc_id, score in results:
                        # Use columns to show score and the address details
                        with st.container(border=True):
                            sc, info = st.columns([1, 4])
                            sc.metric("Score", f"{score:.2f}")
                            
                            # Presenting the full Record data
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