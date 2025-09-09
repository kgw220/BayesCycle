"""
This module includes relevant utility functions for my Indego Bike Demand streamlit app.
"""

import os

import arviz as az
import branca.colormap as bcm
import folium
import numpy as np
import pandas as pd
import pickle
import plotly.graph_objects as go
import streamlit as st
import streamlit.components.v1 as components

from folium.plugins import AntPath
from scipy.ndimage import gaussian_filter
from scipy.stats import nbinom, lognorm
from sklearn.metrics.pairwise import haversine_distances
from streamlit_folium import st_folium
from typing import Any, Dict, List, Optional, Tuple


@st.cache_data
def load_model_data(model_name: str) -> az.InferenceData:
    """
    Loads the fitted PyMC model results from a saved .nc file.

    Parameters:
    -----------
    model_name : str
        The name of the model to load (without the .nc extension).

    Returns:
    --------
    az.InferenceData
        The loaded InferenceData object containing the model results.
    """
    try:
        script_dir = os.path.dirname(__file__)
        project_root = os.path.abspath(os.path.join(script_dir, os.pardir))
        model_path = os.path.join(project_root, "models", f"{model_name}.nc")
        idata = az.from_netcdf(model_path)
        return idata
    except FileNotFoundError:
        st.error(f"Model file not found at {model_path}.")
        return None


@st.cache_data
def load_auxiliary_data() -> Optional[Dict]:
    """
    Loads auxiliary data files needed for the app, including the unique
    station details and the list of station IDs used during model training.

    Returns
    -------
    dict or None
        A dictionary containing the stations DataFrame and the list of station IDs,
        or None if any files are not found.
    """
    try:
        script_dir = os.path.dirname(os.path.realpath(__file__))

        # Load the subsetted bike data pickle file (Data from past 2 years)
        bike_data_path = os.path.join(script_dir, "..", "data", "daily_ride_counts.pkl")
        df_daily_rides = pd.read_pickle(bike_data_path)

        # Load the dataframe with information for each station (lat/long and name)
        stations_df_path = os.path.join(script_dir, "..", "data", "df_stations_unique.pkl")
        df_stations = pd.read_pickle(stations_df_path)

        # Load the list of station IDs used in the hierarchical model
        station_ids_path = os.path.join(script_dir, "..", "data", "station_id_list.pkl")
        with open(station_ids_path, "rb") as f:
            station_ids = pickle.load(f)

        # Load the OD matrix, used to support the geospatial gravity model
        od_matrix_path = os.path.join(script_dir, "..", "data", "od_matrix.pkl")
        od_matrix = pd.read_pickle(od_matrix_path)

        # Load the list of station IDs used in the hierarchical model
        df_distances_path = os.path.join(script_dir, "..", "data", "station_distances.pkl")
        df_distances = pd.read_pickle(df_distances_path)

        return {
            "stations_df": df_stations,
            "station_ids": station_ids,
            "daily_rides": df_daily_rides,
            "od_matrix": od_matrix,
            "distances": df_distances,
        }
    except FileNotFoundError as e:
        st.error(f"Auxiliary data file not found: {e}. Please ensure all data files are in place.")
        return None


def create_duration_tab(idata: az.InferenceData, min_duration: int, max_duration: int) -> None:
    """
    Creates the Streamlit tab for analyzing and visualizing ride durations.

    Parameters:
    -----------
    idata : az.InferenceData
        The fitted InferenceData object for the duration model
    min_duration : int
        The lower bound of the duration range from the slider
    max_duration : int
        The upper bound of the duration range from the slider

    """
    # Extract model parameters
    posterior_samples = az.extract(idata)
    mu_fit = posterior_samples["mu"].mean().item()
    sigma_fit = posterior_samples["sigma"].mean().item()

    # Calculate the probability
    prob_in_range = lognorm.cdf(max_duration, s=sigma_fit, scale=np.exp(mu_fit)) - lognorm.cdf(
        min_duration, s=sigma_fit, scale=np.exp(mu_fit)
    )
    # Display the probability as a metric
    st.metric(
        label=f"Probability of ride being between {min_duration} and {max_duration} mins",
        value=f"{prob_in_range:.2}",
    )

    # Create the interactive plotly graph
    x_plot = np.linspace(0, 180, 600)
    pdf_values = lognorm.pdf(x_plot, s=sigma_fit, scale=np.exp(mu_fit))

    # Generate data points for the highlighted area
    x_fill = np.linspace(min_duration, max_duration, 100)
    y_fill = lognorm.pdf(x_fill, s=sigma_fit, scale=np.exp(mu_fit))

    # Create the figure
    fig = go.Figure()

    # Add the main PDF curve
    fig.add_trace(
        go.Scatter(
            x=x_plot,
            y=pdf_values,
            mode="lines",
            name="Probability Distribution",
            line=dict(color="#00BFFF", width=3),
            showlegend=False,
        )
    )

    # Add the highlighted area under the curve
    fig.add_trace(
        go.Scatter(
            x=np.concatenate([x_fill, x_fill[::-1]]),
            y=np.concatenate([y_fill, np.zeros(len(y_fill))]),
            fill="toself",
            fillcolor="rgba(255, 255, 255, 0.2)",
            line=dict(color="rgba(255,255,255,0)"),
            hoverinfo="skip",
            showlegend=False,
        )
    )

    fig.update_layout(
        title="Bike Ride Duration Probability Distribution",
        xaxis_title="Ride Duration (minutes)",
        yaxis_title="Probability Density",
    )

    st.plotly_chart(fig, use_container_width=True)


def create_station_popularity_tab(
    idata: az.InferenceData,
    stations_df: pd.DataFrame,
    station_ids: List[int],
    day_of_week: str,
    tile_layer: str,
) -> None:
    """
    Creates the Streamlit tab for visualizing station popularity on a map.

    Parameters:
    -----------
    idata : az.InferenceData
        The fitted InferenceData object for the station popularity model
    stations_df : pd.DataFrame
        DataFrame containing details for all unique stations (ID, name, lat, lon)
    station_ids : List[int]
        The ordered list of station IDs that the hierarchical model was trained on
    day_of_week : str
        The selected day of the week to display (e.g., "Mon")
    tile_layer : str
        The name of the Folium tile layer to use for the map's base layer
    """
    # --- Recreate the Prediction DataFrame ---
    # Extract the posterior mean for each of the model's parameters
    posterior_samples = az.extract(idata)
    station_pop_est = posterior_samples["station_log_popularity"].mean(dim="sample").values
    day_effect_est = posterior_samples["day_effect"].mean(dim="sample").values
    # Model was trained on ~97.7 weeks of data
    n_weeks = 97.7
    days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

    # Create an empty DataFrame to hold the daily predictions for each station
    prediction_df = pd.DataFrame(index=station_ids, columns=days)
    prediction_df.index.name = "Station_ID"

    # Loop through each station and day to calculate the predicted demand
    for station_idx, station_id in enumerate(station_ids):
        for day_idx, day_name in enumerate(days):
            # Combine the station's unique popularity with the shared day effect
            log_expected_total_count = station_pop_est[station_idx] + day_effect_est[day_idx]
            expected_total_count = np.exp(log_expected_total_count)

            # Normalize the total count by the number of weeks to get the average daily demand
            avg_daily_demand = expected_total_count / n_weeks
            prediction_df.loc[station_id, day_name] = avg_daily_demand

    # Merge the prediction data with the station details (name, lat, lon)
    prediction_df = pd.merge(prediction_df, stations_df, left_index=True, right_on="Station_ID")

    # Drop rows with missing coordinates before plotting
    prediction_df.dropna(subset=["latitude", "longitude"], inplace=True)

    # --- Create the Folium Map ---
    # Center the map on the average coordinates of all stations
    map_center = [prediction_df["latitude"].mean(), prediction_df["longitude"].mean()]
    m = folium.Map(location=map_center, zoom_start=12, tiles=tile_layer)

    # Set up a continuous colormap for the markers based on demand for the selected day
    min_demand = prediction_df[day_of_week].min()
    max_demand = prediction_df[day_of_week].max()
    colormap = bcm.LinearColormap(
        ["yellow", "orange", "red"],
        vmin=min_demand,
        vmax=max_demand,
        caption=f"Avg. Daily Trips for {day_of_week}",
    )

    # Add a colored CircleMarker to the map for each station
    for _, row in prediction_df.iterrows():
        folium.CircleMarker(
            location=[row["latitude"], row["longitude"]],
            radius=5,
            color=colormap(row[day_of_week]),
            fill=True,
            fill_color=colormap(row[day_of_week]),
            fill_opacity=0.7,
            # The tooltip shows the station name and its exact predicted trips
            tooltip=f"<b>{row['Station_Name']}</b><br>Predicted Trips: {row[day_of_week]:.1f}",
        ).add_to(m)

    # Add the colormap legend to the map
    m.add_child(colormap)

    # Add custom CSS to style the legend and colormap for better visibility
    style_block = """
    <style>
        div.branca-colormap,
        div.branca-colorbar,
        div.legend {
            background-color: rgba(255, 255, 255, 0.85) !important;  
            border: 2px solid #666 !important;
            border-radius: 8px !important;
            padding: 6px 10px !important;
            color: #000 !important;
            font-weight: 600 !important;
            box-shadow: 0 2px 6px rgba(0,0,0,0.25) !important;
        }

        div.branca-colormap .caption,
        div.branca-colorbar .caption,
        div.legend .caption {
            display: block;
            text-align: center;
            margin-bottom: 4px;
        }
    </style>
    """
    m.get_root().header.add_child(folium.Element(style_block))

    st_folium(m, use_container_width=True, height=600, returned_objects=[])


def create_forecast_tab(
    idata: az.InferenceData, daily_rides: pd.Series, smoothing_sigma: int
) -> None:
    """
    Creates the Streamlit tab for visualizing the daily demand forecast.

    Parameters
    ----------
    idata : az.InferenceData
        The fitted InferenceData object for the time-series model
    daily_rides : pd.Series
        The historical time series of daily ride counts
    smoothing_sigma : int
        The sigma value for the Gaussian smoothing filter. If 0, no smoothing is applied
    """
    st.write(
        "This chart shows the historical data broken down into the learned long-term trend and the \
        overall model fit (trend + weekly pattern)."
    )

    # Extract the posterior mean for the trend and the full model fit
    posterior_samples = az.extract(idata)
    mean_log_trend = posterior_samples["trend"].mean(dim="sample").values
    day_of_week_hist = daily_rides.index.dayofweek.values
    mean_fit = np.exp(
        mean_log_trend
        + posterior_samples["week_effect"].mean(dim="sample").values[day_of_week_hist]
    )
    mean_trend_original_scale = np.exp(mean_log_trend)

    # Apply Gaussian smoothing if specified
    if smoothing_sigma > 0:
        smoothed_trend = gaussian_filter(mean_trend_original_scale, sigma=smoothing_sigma)
        smoothed_fit = gaussian_filter(mean_fit, sigma=smoothing_sigma)
    else:
        smoothed_trend = mean_trend_original_scale
        smoothed_fit = mean_fit

    # Create the Decomposition Plot
    fig_fit = go.Figure()
    fig_fit.add_trace(
        go.Scatter(
            x=daily_rides.index,
            y=daily_rides.values,
            mode="markers",
            name="Observed Rides",
            marker=dict(color="gray", opacity=0.5),
        )
    )
    fig_fit.add_trace(
        go.Scatter(
            x=daily_rides.index,
            y=smoothed_trend,
            mode="lines",
            name="Estimated Trend (baseline)",
            line=dict(color="blue"),
            opacity=0.7,
        )
    )
    fig_fit.add_trace(
        go.Scatter(
            x=daily_rides.index,
            y=smoothed_fit,
            mode="lines",
            name="Mean Model Fit (with weekly seasonality)",
            line=dict(color="orange"),
            opacity=0.3,
        )
    )
    fig_fit.update_layout(
        title="Bike Demand Model",
        xaxis_title="Date",
        yaxis_title="Number of Rides",
        xaxis=dict(
            dtick="M1",
            tickformat="%Y-%b",
            tickangle=-45,
            tickmode="linear",
        ),
        margin=dict(b=100),
    )
    st.plotly_chart(fig_fit, use_container_width=True)

    st.subheader("90-Day Forecast since June 2025")

    # Manually simulate the forecast
    progress_bar = st.progress(0, text="Simulating future ride paths...")

    last_day_trend = posterior_samples["trend"].isel(trend_dim_0=-1)
    week_effect_samples = posterior_samples["week_effect"]
    alpha_samples = posterior_samples["alpha"]
    trend_sigma = 0.05
    n_samples = len(last_day_trend)
    n_forecast_days = 90
    forecast_dates = pd.date_range(
        start=daily_rides.index[-1] + pd.Timedelta(days=1), periods=n_forecast_days
    )
    forecast_day_of_week = forecast_dates.dayofweek

    # Initialize array to store N complete forecast paths
    forecast_values = np.zeros((n_samples, n_forecast_days))

    # Loop through each posterior sample to create a full distribution of forecasts
    for i in range(n_samples):
        current_trend = last_day_trend.isel(sample=i).values
        current_week_effect_sample = week_effect_samples.isel(sample=i).values
        current_alpha_sample = alpha_samples.isel(sample=i).values
        for t in range(n_forecast_days):
            # Apply a random walk step to the trend
            current_trend += np.random.normal(0, trend_sigma)
            day_effect = current_week_effect_sample[forecast_day_of_week[t]]
            # Calculate the expected mean (lambda/mu) on the original scale
            expected_count = np.exp(current_trend + day_effect)
            # NOTE: The nbinom.rvs function needs p, not mu and alpha. Convert to p with mu and alpha
            # Negative Binomial parameter conversion: p = mu / (mu + alpha)
            p = expected_count / (expected_count + current_alpha_sample)
            # Sample the final count using the Negative Binomial distribution
            forecast_values[i, t] = nbinom.rvs(n=current_alpha_sample, p=1 - p)

        progress_bar.progress((i + 1) / n_samples, text=f"Simulating path {i+1}/{n_samples}")

    progress_bar.empty()

    # Apply gaussian smoothing if specified
    mean_forecast = forecast_values.mean(axis=0)
    if smoothing_sigma > 0:
        smoothed_forecast = gaussian_filter(mean_forecast, sigma=smoothing_sigma)
        smoothed_observed = gaussian_filter(daily_rides.values, sigma=smoothing_sigma)
    else:
        smoothed_forecast = mean_forecast
        smoothed_observed = daily_rides.values

    # Create the Forecast Plot
    fig_fc = go.Figure()
    fig_fc.add_trace(
        go.Scatter(
            x=daily_rides.index,
            y=smoothed_observed,
            mode="lines",
            name="Observed Rides",
            line=dict(color="gray"),
        )
    )
    fig_fc.add_trace(
        go.Scatter(
            x=forecast_dates,
            y=smoothed_forecast,
            mode="lines",
            name="Mean Forecast",
            line=dict(color="red"),
        )
    )

    fig_fc.update_layout(
        title="Daily Bike Ride Demand Forecast",
        xaxis_title="Date",
        yaxis_title="Number of Rides",
        xaxis=dict(
            dtick="M1",
            tickformat="%Y-%b",
            tickangle=-45,
            tickmode="linear",
        ),
        margin=dict(b=100),
    )
    st.plotly_chart(fig_fc, use_container_width=True)


def create_trip_flow_tab(
    gravity_idata: az.InferenceData,
    station_idata: az.InferenceData,
    aux_data: Dict[str, Any],
    tile_layer: str,
) -> None:
    """
    Creates the Streamlit tab for visualizing the trip flow (gravity) model.

    Parameters:
    -----------
    gravity_idata : az.InferenceData
        Fitted InferenceData for the gravity model
    station_idata : az.InferenceData
        Fitted InferenceData for the station popularity model
    aux_data : Dict[str, Any]
        A dictionary containing auxiliary data like stations_df, od_matrix, etc
    tile_layer : str
        The name of the Folium tile layer to use for the map's base layer
    """
    st.write(
        "Select an origin station and one or more destination stations to visualize the predicted \
        average daily trip flows between them."
    )

    # Recreate the model_df
    stations_df, station_ids = aux_data["stations_df"], aux_data["station_ids"]
    od_matrix, df_distances = aux_data["od_matrix"], aux_data["distances"]

    # Filter out stations that do not appear as starting or ending stations in the OD matrix
    od_matrix = od_matrix.loc[station_ids, station_ids]
    df_distances = df_distances[
        df_distances["origin_id"].isin(station_ids)
        & df_distances["destination_id"].isin(station_ids)
    ]

    # Create a lookup dictionary that maps each station ID to its internal model index (0, 1, 2...)
    station_lookup = {id: i for i, id in enumerate(station_ids)}

    # Extract the posterior samples from the fitted hierarchical model
    posterior_station = az.extract(station_idata)
    station_pop_est = posterior_station["station_log_popularity"].mean(dim="sample").values
    # Convert the log popularity scores to the original scale and store them in a Series, indexed
    # by the model index
    station_popularity = pd.Series(np.exp(station_pop_est), index=range(len(station_ids)))
    # Create the final mapping from the real station ID to its estimated popularity score
    station_id_to_pop_map = {
        st_id: station_popularity[idx] for st_id, idx in station_lookup.items()
    }

    # Convert the OD matrix to a long format DataFrame for easier merging. Then, merge popularity
    # scores and distance data for each OD pair
    model_df = od_matrix.unstack().reset_index(name="trip_count")
    model_df.columns = ["origin_id", "destination_id", "trip_count"]
    model_df["origin_pop"] = model_df["origin_id"].map(station_id_to_pop_map)
    model_df["dest_pop"] = model_df["destination_id"].map(station_id_to_pop_map)
    model_df = model_df.merge(df_distances, on=["origin_id", "destination_id"])
    model_df = model_df.query(
        "origin_pop > 0 and dest_pop > 0 and distance_km > 0 and origin_id != destination_id"
    ).fillna(0)

    # Extra coefficients from the gravity model to calculate predicted trips
    coeffs = az.summary(
        gravity_idata, var_names=["alpha", "beta_origin", "beta_dest", "gamma_dist"]
    )["mean"]
    model_df["predicted_log_trips"] = (
        coeffs["alpha"]
        + coeffs["beta_origin"] * np.log(model_df["origin_pop"])
        + coeffs["beta_dest"] * np.log(model_df["dest_pop"])
        - coeffs["gamma_dist"] * np.log(model_df["distance_km"])
    )
    model_df["predicted_trips"] = np.exp(model_df["predicted_log_trips"])
    model_df["avg_daily_trips"] = model_df["predicted_trips"] / 730

    # User input for origin and destination stations
    station_names = sorted(stations_df["Station_Name"].unique())
    origin_name = st.selectbox("Choose an origin station:", station_names)
    available_destinations = sorted(
        model_df[
            model_df["origin_id"]
            == stations_df[stations_df["Station_Name"] == origin_name]["Station_ID"].iloc[0]
        ]["destination_id"]
        .map(stations_df.set_index("Station_ID")["Station_Name"])
        .unique()
    )
    selected_destinations = st.multiselect(
        "Choose destination stations to highlight:", available_destinations
    )

    # --- Create the Flow Map ---
    # Create lookup dictionaries for station coordinates and names
    coord_lookup = (
        stations_df.set_index("Station_ID")[["latitude", "longitude"]]
        .apply(tuple, axis=1)
        .to_dict()
    )
    name_lookup = stations_df.set_index("Station_ID")["Station_Name"].to_dict()
    name_to_id_lookup = {v: k for k, v in name_lookup.items()}

    # Get the corresponding station IDs for the selected origin and destinations
    origin_id = name_to_id_lookup.get(origin_name)
    destination_ids = [name_to_id_lookup.get(name) for name in selected_destinations]

    # Only create the map if a valid origin is selected
    if origin_id:
        # Center the map on the origin station
        map_center = coord_lookup.get(origin_id)
        m = folium.Map(location=map_center, zoom_start=13, tiles=tile_layer)

        # Add a marker for the origin station
        folium.Marker(
            location=map_center,
            popup=f"Origin: {origin_name}",
            icon=folium.Icon(color="red", icon="star"),
        ).add_to(m)
        # Filter the model_df to only include flows from the selected origin to the chosen
        # # destinations
        filtered_flows = model_df[
            (model_df["origin_id"] == origin_id)
            & (model_df["destination_id"].isin(destination_ids))
        ]
        # Only add flow lines if there are any valid flows to display
        if not filtered_flows.empty:
            min_trips, max_trips = (
                filtered_flows["avg_daily_trips"].min(),
                filtered_flows["avg_daily_trips"].max(),
            )
            # Set up a continuous colormap for the flow lines based on avg_daily_trips
            colormap = bcm.LinearColormap(
                ["blue", "yellow", "red"],
                vmin=min_trips,
                vmax=max_trips if max_trips > min_trips else min_trips + 1,
                caption="Avg. Daily Trips",
            )
            # Add an AntPath for each selected destination
            for _, row in filtered_flows.iterrows():
                origin_coords = coord_lookup.get(row["origin_id"])
                dest_coords = coord_lookup.get(row["destination_id"])
                dest_name = name_lookup.get(row["destination_id"])

                if origin_coords and dest_coords:
                    AntPath(
                        locations=[origin_coords, dest_coords],
                        color=colormap(row["avg_daily_trips"]),
                        weight=5,
                        delay=1000,
                        dash_array=[10, 20],
                        tooltip=f"To: {dest_name}<br>Predicted Daily Trips: {row['avg_daily_trips']:.2f}",
                    ).add_to(m)
                    folium.Marker(
                        location=dest_coords,
                        popup=f"Destination: {dest_name}",
                        icon=folium.Icon(color="blue", icon="info-sign"),
                    ).add_to(m)

            m.add_child(colormap)

            # Add custom CSS to style the legend and colormap for better visibility
            style_block = """
            <style>
                div.branca-colormap,
                div.branca-colorbar,
                div.legend {
                    background-color: rgba(255, 255, 255, 0.85) !important;  
                    border: 2px solid #666 !important;
                    border-radius: 8px !important;
                    padding: 6px 10px !important;
                    color: #000 !important;
                    font-weight: 600 !important;
                    box-shadow: 0 2px 6px rgba(0,0,0,0.25) !important;
                }

                div.branca-colormap .caption,
                div.branca-colorbar .caption,
                div.legend .caption {
                    display: block;
                    text-align: center;
                    margin-bottom: 4px;
                }
            </style>
            """
            m.get_root().header.add_child(folium.Element(style_block))

        components.html(m._repr_html_(), height=600)
