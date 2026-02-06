import streamlit as st
import pandas as pd
from lfas import PySearchEngine
import time

st.set_page_config(page_title="LFAS Pro", page_icon="⚡", layout="wide")
st.title("🇧🇷 LFAS: High-Performance Address Search")

# 1. Sidebar for Stats & File Upload
with st.sidebar:
    uploaded_file = st.file_uploader("Upload CSV", type="csv")
    top_k = st.number_input("Top K", value=10)
    

if uploaded_file:
    # Load the full file without .head()
    if 'df' not in st.session_state:
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

        build_duration = time.time() - start_time
        st.session_state['engine'] = engine
        st.session_state['build_time'] = build_duration
        st.success(f"Successfully indexed {total_rows:,} records in {build_duration:.2f}s!")

# 3. Multi-Field Search UI
st.subheader("🔍 Field-Aware Query")
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

# 4. Results Display
if submitted:
    if 'engine' not in st.session_state:
        st.error("Please build the index first!")
    else:
        # Filter out empty inputs
        active_query = {k: v for k, v in search_payload.items() if v.strip()}
        
        if not active_query:
            st.warning("Please enter at least one search term.")
        else:
            start_s = time.time()
            results = st.session_state['engine'].search_complex(active_query, top_k)
            st.session_state.last_search_time = (time.time() - start_s) * 1000
            
            st.write(f"### Results ({len(results)})")
            
            for doc_id, score in results:
                # Use columns to show score and the address details
                with st.container(border=True):
                    sc, info = st.columns([1, 4])
                    sc.metric("Score", f"{score:.2f}")
                    
                    # Presenting the full Record data
                    record = df.iloc[doc_id]
                    info.write(f"**{record.get('tipo_logradouro', '')} {record.get('rua', '')}, {record.get('numero', 'S/N')}**")
                    info.write(f"{record.get('bairro', '')} — {record.get('municipio', '')}, {record.get('estado', '')}")
                    info.caption(f"CEP: {record.get('cep', '')} | ID: {doc_id}")