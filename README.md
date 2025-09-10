# BayesCycle

## Background
This project provides a (relatively) comprehensive Bayesian analysis of Philadelphia's Indego bike-share system as a whole. Using publicly available trip data, I fitted a series of probabilistic models to understand and forecast system dynamics, from individual ride characteristics to the overall network flow. The visualizations are designed to help city planners, transportation analysts, or curious citizens explore patterns in ridership and quantify the uncertainty inherent in demand forecasting.

The data is sourced from the official Indego Open Data portal, which provides quarterly trip records for the entire system.

## Goal
The primary goal is to provide a comprehensive, interactive tool for understanding and forecasting bike-share demand in Philadelphia with different factors usingBayesian statistical modeling to move beyond simple averages, providing a full probabilistic view of the system's behavior. This allows for a deeper understanding of the factors influencing ridership and provides a more honest assessment of forecast uncertainty.

## Methodology
This project is built on a series of four interconnected Bayesian models, each designed to analyze a different aspect of the bike-share system. 

### 1. Ride Duration Model
This model analyzes the distribution of thousands of individual trip durations to understand how long a typical ride lasts.
* **Model:** A Log-Normal probability distribution is fit to the data using PyMC.
* **Insight:** Instead of a simple average, this provides the ability to answer probabilistic questions, such as, "What is the probability a ride will last longer than X minutes?"

### 2. Station Popularity Model
This model estimates the popularity or baseline demand for every station in the network, while accounting for weekly patterns (spoiler alert, there was more demand during weekdays than weekends!).
* **Model:** A Hierarchical Poisson model is used to estimate popularity for all stations simultaneously.
* **Insight:** This approach allows stations with less data to "borrow statistical strength" from the rest of the network, leading to more stable and reliable estimates. It reveals which stations are the true hubs of activity and how their usage changes throughout the week. 

### 3. Daily Demand Forecast Model
This model analyzes the total number of rides per day across the entire system to forecast overall demand.
* **Model:** A Bayesian structural time-series model is built to decompose the aggregated daily ride count into its underlying long-term trend and the aformentioned weekly seasonality.
* **Insight:** This produces a 90-day probabilistic forecast to capture the year-over-year growth and seasonal fluctuations.

### 4. Trip Flow (Gravity) Model
This final model explains and predicts the number of trips between any two stations in the network, which I think is what most people would find most interesting.
* **Model:** A geospatial "gravity model" is used, where trip volume is predicted based on the origin and destination stations' popularity (as determined by the previous hierarchical model) and the distance between them.
* **Insight:** This model quantifies the "friction of distance" and the "attractiveness" of popular stations, revealing the primary travel corridors of the city's bike-share network.

## App Features
The app is hosted here: https://bayescycle.streamlit.app/

The Streamlit app is divided into four tabs, one for each model:

### Ride Duration
This tab displays an interactive probability distribution of ride durations. A slider allows one to select a time range, and the app calculates the probability of a ride falling within that range, visualizing the result as a shaded area under the curve.

### Station Popularity
This tab features an interactive Folium map showing the location of all bike stations. One can select a day of the week, and the markers on the map will be color-coded based on the predicted average number of trips for that day, revealing daily demand hotspots.

### Trip Flows
This tab provides an interactive flow map to explore the gravity model's predictions. One can select an origin station and one or more destination stations, and the map will display animated, colored lines representing the predicted average daily flow of trips between them.

### Daily Forecast
This tab presents two time-series charts. The first decomposes the historical data into its estimated long-term trend and the overall model fit. The second chart shows the 90-day forecast for total system-wide ridership. A slider allows users to apply a Gaussian smoothing filter to the lines to better visualize the patterns, since the raw patterns are quike "spiky" in nature (this is due to the small increases and decreases in long term demand between each week for weekdays/weekends).

## Repo Structure
The `task_notebooks` directory holds the Jupyter Notebooks that walk through the development of each of the four Bayesian models, alongside some code with using the results. `/app` holds the main script for the Streamlit app, with a related utilities function. The `data` directory holds any relevant data files, from the raw data from Indego, to some other custom files that I used throughout my project. Finally, the `models` directory holds each of my four models in the `.nc` format.

## Future Work
While this project provides a comprehensive overview, several enhancements could be made. The current analysis relies on a static dataset; a future version could be enhanced with a daily automated pipeline to ingest the latest trip data. It seems that there is possibly an API to automate this process, but I could not find any proper documentation on it/using it.

Additionally, the models could be made more sophisticated. For example, weather data from an external API could be incorporated as a predictor in the daily forecast or gravity models to see how factors like temperature and precipitation influence ridership. Also, the gravity model purposely excludes measuring the number of rides that start from one station and end at said station, but this information may be good to know. The final logical step would be to combine all four models (or any future models) into some kind of  single, Monte Carlo simulation to model the real-time availability of bikes at each station throughout a typical day. I'm a bit too busy though, but if I feel motivated, I will come back to this at a later date. At the very least, I hope to rerun the data pulling and model fitting steps occasionally as new quartely data is added.

##  Data Dictionaries
Both starting dataframes come from Indego themselves: https://www.rideindego.com/about/data/

Each `.csv` file contains data for one quarter of the year.

### Indego Bike Ride Data
| Column Name | Data Type | Description | 
 | ----- | ----- | ----- | 
| trip_id | integer | Locally unique integer that identifies the trip. | 
| duration | integer | Length of trip in minutes. | 
| start_time | datetime | The date/time when the trip began. | 
| end_time | datetime | The date/time when the trip ended. | 
| start_station | integer | The station ID where the trip originated. | 
| start_lat | float | The latitude of the station where the trip originated. | 
| start_lon | float | The longitude of the station where the trip originated. | 
| end_station | integer | The station ID where the trip terminated. | 
| end_lat | float | The latitude of the station where the trip terminated. | 
| end_lon | float | The longitude of the station where the trip terminated. | 
| bike_id | integer | Locally unique integer that identifies the bike. | 
| plan_duration | integer | The number of days for the passholder's plan (0 for single-ride Walk-up). | 
| trip_route_category | string | "Round Trip" or "One Way". | 
| passholder_type | string | The name of the passholder’s plan. | 
| bike_type | string | The kind of bike used (e.g., standard or electric). | 

**Data Processing Notes:**

* Staff servicing and test trips are removed.

* Trips below 1 minute and trip lengths capped at 24 hours.

* A "Virtual Station" is used by staff for special events or remote check-ins/outs.

* Data for 2015, the year the program started, was not able to be downloaded.

### Station Information

This table provides details for each station, updated as of 2025-07-01.

| Column | Data Type | Description | 
 | ----- | ----- | ----- | 
| Station ID | integer | Unique integer that identifies the station. | 
| Station Name | string | The public name of the station. | 
| Go live date | date | The date the station was first available. | 
| Status | string | The current status of the station (e.g., "Active"). | 

**Data Processing Notes:**

* Some stations are no longer active, so they were filtered out in many cases.

