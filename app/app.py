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

st.set_page_config(page_title="BayesCycle", layout="wide")
st.sidebar.markdown(
    """
    <h1 style="font-family: 'Helvetica Neue', sans-serif; color: #2E86C1; font-size: 28px;">
        🚲 BayesCycle
    </h1>
    """,
    unsafe_allow_html=True,
)
st.sidebar.info(
    "This app provides a multi-faceted Bayesian analysis of Philadelphia's Indego bike-share \
    system, allowing users to explore ride durations, station popularity, daily demand forecasts, \
    and trip flows."
)
st.sidebar.warning(
    "**Disclaimer:** The data used for this app is static and includes records up to June \
    2025. The models and forecasts are based on this historical data."
)

# Load model .nc files
idata_duration = su.load_model_data("duration_model_results")
idata_station = su.load_model_data("station_pop_model_results")
idata_ts = su.load_model_data("time_series")
idata_gravity = su.load_model_data("geospatial")

# Load auxiliary data files
aux_data = su.load_auxiliary_data()

# Create tabs for different models
tab1, tab2, tab3, tab4 = st.tabs(
    ["Ride Duration", "Starting Station Popularity", "Station to Station Demand", "Daily Forecast"]
)

# Duration Tab
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

# Starting Station Popularity Tab
with tab2:
    st.write(
        "Select a day of the week to see the predicted average number of trips starting from each \
        station."
    )
    # User input for day of the week and map style
    day_selection = st.selectbox(
        "Choose a day of the week:", ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
    )
    map_style_pop = st.selectbox(
        "Choose a map style:", ("Light", "Street Map", "Satellite"), key="map_style_pop"
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
            tile_layer=tile_map[map_style_pop],
        )

# Station to Station Demand Tab
# NOTE: This was originally the fourth tab, but since the forecasting tab takes several seconds to
# load, I moved this tab to be third so that it loads much quickly (instead of having to wait for
# that tab to load first)
with tab3:
    map_style_flow = st.selectbox(
        "Choose a map style:", ("Light", "Street Map", "Satellite"), key="map_style_flow"
    )
    if idata_gravity and idata_station and aux_data:
        su.create_trip_flow_tab(
            gravity_idata=idata_gravity,
            station_idata=idata_station,
            aux_data=aux_data,
            tile_layer=tile_map[map_style_flow],
        )

# Time Series Forecast Tab
with tab4:
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
