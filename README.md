# Dark Store Operational Efficiency Simulator

## Overview
Built a simulation engine to optimize warehouse picking efficiency by testing SKU-slotting strategies before real-world implementation.

In simulations, dynamic SKU placement based on real-time order frequency reduced average pick-path distance by **~15%** compared to static storage methods.

---

## Problem
Warehouse operations often rely on static SKU placement, leading to inefficient picking routes and increased operational time.

Testing new layouts in real environments is costly and risky.

---

## Solution
Developed a configurable simulation system that models warehouse operations and allows operators to experiment with different inventory strategies.

The system enables:
- Comparison of SKU-slotting policies  
- Visualization of high-traffic zones  
- Rapid iteration without operational disruption  

---

## Key Features

- **Simulation Engine**  
  Models picking workflows and SKU-slotting strategies using Python  

- **Policy Switching Dashboard**  
  Compare frequency-based vs profit-margin-based placement in real time  

- **Spatial Heatmaps**  
  Identify warehouse “hot zones” for optimization  

- **Data Layer**  
  Supabase (PostgreSQL) backend handling high-concurrency logs  

- **Configurable Architecture**  
  YAML-based system for flexible warehouse/environment setup  

---

## System Design (Simplified)

```

Orders → Simulation Engine → Policy Logic → Path Calculation → Output Metrics + Heatmaps

````

---

## Tech Stack
- Python  
- Streamlit  
- Supabase (PostgreSQL)  
- YAML  

---

## Key Insight
Dynamic SKU-slotting based on demand frequency significantly reduces picker travel distance, improving operational efficiency without requiring infrastructure changes.

---

## Production Considerations
For a production-grade deployment:
- Introduce workflow orchestration (e.g., scheduled runs, retries)  
- Integrate real-time warehouse data streams  
- Add monitoring for simulation accuracy vs real-world outcomes  

---

## Setup and Installation
1. Clone the repository  
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   
3. Configure your Supabase credentials in the `.env` or YAML configuration layer
4. Run the simulator:

   ```bash
   streamlit run app.py
   ````
