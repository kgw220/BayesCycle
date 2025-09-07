"""
This module includes relevant utility functions for my Indego Bike Demand streamlit app.
"""

import os

import arviz as az
import numpy as np
import plotly.graph_objects as go
import streamlit as st

from scipy.stats import lognorm
from typing import Optional, Tuple


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

    st.metric(
        label=f"Probability of ride being between {min_duration} and {max_duration} mins",
        value=f"{prob_in_range:.2%}",
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
