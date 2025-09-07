"""
This is the main application file for the Indego Bike Demand streamlit app. Imports relevant
utility functions from streamlit_utils.py.
"""

import pandas as pd
import streamlit as st

from streamlit_utils import load_model_data, create_duration_tab

# TEST
import os

print("CWD:", os.getcwd())
print("App __file__:", __file__)
print("Files in repo root:", os.listdir(os.getcwd()))
print("Files in models dir:", os.listdir(os.path.join(os.getcwd(), "models")))

# Apply custom CSS; Minimal padding & blue highlights for tabs and slider objects
st.markdown(
    """
    <style>
        .block-container {
            padding-top: 5rem;
            padding-bottom: 3rem;
            padding-left: 5rem;
            padding-right: 5rem;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

# Load model .nc files
idata_duration = load_model_data("duration_model_results")

# --- Main App Logic ---
st.title("Indego Bike Demand Dashboard")

# Create tabs for different models
tab1, tab2, tab3, tab4 = st.tabs(
    ["Ride Duration", "Starting Station Popularity", "Daily Forecast", "Station to Station Demand"]
)

with tab1:
    st.header("Ride Duration Probability Analysis")
    st.write(
        "Use the slider to select a bike ride duration range (in minutes) and find the probability \
        of a ride duration falling within it."
    )

    min_duration_val, max_duration_val = st.slider(
        "Select a ride duration range (minutes):", min_value=0, max_value=180, value=(10, 30)
    )

    if idata_duration:
        # Pass the slider values to the function
        create_duration_tab(idata_duration, min_duration_val, max_duration_val)
