"""
This is the main application file for the Indego Bike Demand streamlit app. Imports relevant
utility functions from streamlit_utils.py.
"""

import pandas as pd
import streamlit as st

import streamlit_utils as su

# Apply custom CSS; Minimal padding
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

st.set_page_config(page_title="Indego Bike Demand Dashboard", layout="wide")

# Load model .nc files
idata_duration = su.load_model_data("duration_model_results")
idata_station = su.load_model_data("station_pop_model_results")
idata_ts = su.load_model_data("time_series")

# Load auxiliary data files
aux_data = su.load_auxiliary_data()

# Create tabs for different models
tab1, tab2, tab3, tab4 = st.tabs(
    ["Ride Duration", "Starting Station Popularity", "Daily Forecast", "Station to Station Demand"]
)

with tab1:
    st.write(
        "Use the slider to select a bike ride duration range (in minutes) and find the probability \
        of a ride duration falling within it."
    )

    # User input for ride duration range
    min_duration_val, max_duration_val = st.slider(
        "Select a ride duration range (minutes):", min_value=0, max_value=180, value=(10, 30)
    )

    if idata_duration:
        su.create_duration_tab(
            idata=idata_duration, min_duration=min_duration_val, max_duration=max_duration_val
        )

with tab2:
    st.write(
        "Select a day of the week to see the predicted average number of trips starting from each \
        station."
    )
    # User input for day of the week and map style
    day_selection = st.selectbox(
        "Choose a day of the week:", ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
    )
    map_style = st.selectbox(
        "Choose a map style:", ("Light", "Street Map", "Satellite"), key="map_style_select"
    )
    tile_map = {
        "Light": "CartoDB positron",
        "Street Map": "OpenStreetMap",
        "Satellite": "Esri.WorldImagery",
    }

    if idata_station and aux_data:
        su.create_station_popularity_tab(
            idata=idata_station,
            stations_df=aux_data["stations_df"],
            station_ids=aux_data["station_ids"],
            day_of_week=day_selection,
            tile_layer=tile_map[map_style],
        )

with tab3:
    smoothing_sigma_val = st.slider(
        "Select Gaussian smoothing sigma:",
        min_value=0,
        max_value=10,
        value=0,
        step=1,
        help="Controls the amount of smoothing applied to the trend and forecast lines in the \
             'Daily Forecast' tab. 0 means no smoothing.",
    )
    if idata_ts and aux_data:
        su.create_forecast_tab(
            idata=idata_ts, daily_rides=aux_data["daily_rides"], smoothing_sigma=smoothing_sigma_val
        )
