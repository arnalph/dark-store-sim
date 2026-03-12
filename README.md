# Dark Store Operational Efficiency Simulator

## Project Overview

This project is a high-fidelity operational simulation engine designed to model and optimize warehouse "pick-path" efficiency and SKU-slotting strategies within a dark store environment. Built with a focus on data-driven decision-making, the simulator allows operators to visualize bottlenecks and test various inventory placement policies before physical implementation.

## Key Features

* **Dynamic Simulation Engine:** Models warehouse picking processes and SKU-slotting strategies using Python.


* **Hybrid Control System:** Features a real-time policy-switching dashboard to toggle between frequency-based and profit-margin ranking strategies.


* **Spatial Heatmapping:** Visualizes warehouse "hot zones" to identify high-traffic areas and enable SKU rebalancing.


* **Persistent Data Architecture:** Utilizes a Supabase (PostgreSQL) backend with an Amazon transaction pooler to manage high-concurrency simulation logs.


* **Configuration Layer:** Maintains core system constants and warehouse parameters via a YAML configuration layer for easy environment adjustments.



## Tech Stack

* **Language:** Python 


* **Frontend:** Streamlit 


* **Database:** Supabase / PostgreSQL 


* **Infrastructure:** Amazon Transaction Pooler 


* **Configuration:** YAML 



## Operational Impact

The simulator serves as a tool for reducing operational overhead by optimizing pick-paths.

* **Performance Metric:** Implementing dynamic SKU-slotting based on real-time order frequency demonstrated a reduction in simulated pick-path distances compared to static storage methods.


* **Optimization:** Enables location-specific optimization through geospatial and spatial analysis.



## Setup and Installation

1. Clone the repository.
2. Install dependencies: `pip install -r requirements.txt`.
3. Configure your Supabase credentials in the `.env` or YAML configuration layer.
4. Run the simulator: `streamlit run app.py`.