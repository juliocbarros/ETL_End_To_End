import streamlit as st 
from pipeline_00 import pipeline


st.title('Read Files')

if st.button('Read'):
    with st.spinner('Read...'):
        logs = pipeline()
        #Displays logs in Streamlit
        for log in logs:
            st.write(log)

