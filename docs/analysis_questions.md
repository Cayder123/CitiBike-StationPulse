# Buổi 7 Analytics Roadmap: Research Questions, Metrics & Caveats

This document serves as the preparatory blueprint for **Buổi 7 (Spark Transformations & Analytical Aggregations)**.

---

## 1. Core Research Questions

1. **Peak Demand Dynamics**:
   - What are the peak trip departure hours during weekdays versus weekends?
   - How do commuting patterns (7:00 AM – 9:30 AM and 5:00 PM – 7:30 PM) compare across residential and commercial transit hubs?
2. **Station Imbalance & Starvation/Overflow**:
   - Which docking stations suffer from consistent depletion (outflow > inflow)?
   - Which stations experience dock overcrowding (inflow > outflow), forcing riders to seek alternative stations?
3. **User Segment Behaviors (Member vs. Casual)**:
   - What is the distribution of trip durations and distances between annual members and casual riders?
   - Do casual riders demonstrate higher preference for electric bikes compared to annual members?
4. **Spatial Flow & Corridor Patterns**:
   - Which origin-destination station pairs represent the highest volume transit corridors?

---

## 2. Core Imbalance Metric: Station Net-Flow

In Buổi 7, station flow dynamics will be computed using PySpark DataFrame and Spark SQL aggregations:

$$\text{Net-Flow}(s, \Delta t) = \text{Arrivals}(s, \Delta t) - \text{Departures}(s, \Delta t)$$

Where:
- $\text{Departures}(s, \Delta t)$: Count of recorded trips where `start_station_id = s` and `started_at` falls in time window $\Delta t$.
- $\text{Arrivals}(s, \Delta t)$: Count of recorded trips where `end_station_id = s` and `ended_at` falls in time window $\Delta t$.
- **Interpretation**:
  - $\text{Net-Flow} > 0$: Net bike accumulation (Risk of full docks / no returns).
  - $\text{Net-Flow} < 0$: Net bike depletion (Risk of empty docks / starvation).
  - $\text{Net-Flow} \approx 0$: Balanced station equilibrium.

---

## 3. Important Analytical Caveats & Operational Realities

When presenting station net-flow findings, team members must explicitly highlight the following limitations:

1. **Net-flow is a Proxy, Not Real Dock Inventory**:
   Historical trip data only records trips completed by riders. It does **not** record operational rebalancing activities conducted by Citi Bike van and valet teams (who move bikes from crowded stations to depleted stations in real time).
2. **Station Capacity Constraints**:
   A station cannot accumulate bikes beyond its physical dock capacity (e.g. 30 docks). Once a station is full, prospective riders are turned away, truncating true organic arrival demand in the dataset.
3. **Maintenance & Out-of-Service Bikes**:
   Bikes flagged for repair or locked out do not show up as available in the fleet, skewing instantaneous dock capacity.
4. **Dockless E-Bike Dropoffs**:
   Electric bikes can occasionally be parked outside physical docks within authorized zones, meaning trips may record coordinates without explicit station IDs.
