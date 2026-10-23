# StationPulse Analytics: January 2024 Scope, Metrics & Caveats

This document defines the analysis scope as the Jersey City `JC-202401` trip archive. No additional month is included.

---

## 1. Core Research Questions

1. **Peak Demand Dynamics**:
   - Which hours have the most recorded departures in January 2024?
   - How do recorded departures differ by weekday versus weekend?
2. **Station Imbalance Proxy**:
   - Which stations have the largest net recorded outflow or inflow in this month?
   - At what hours do individual stations show the largest net-flow changes?
3. **User Segment Behaviors (optional extension)**:
   - What is the distribution of trip durations and distances between annual members and casual riders?
   - Do casual riders demonstrate higher preference for electric bikes compared to annual members?
4. **Spatial Flow & Corridor Patterns (optional extension)**:
   - Which origin-destination station pairs represent the highest volume transit corridors?

---

## 2. Core Imbalance Metric: Station Net-Flow

Station flow is computed using PySpark DataFrame aggregations from trips whose event timestamp falls in January 2024:

$$\text{Net-Flow}(s, \Delta t) = \text{Arrivals}(s, \Delta t) - \text{Departures}(s, \Delta t)$$

Where:
- $\text{Departures}(s, \Delta t)$: Count of recorded trips where `start_station_id = s` and `started_at` falls in time window $\Delta t$.
- $\text{Arrivals}(s, \Delta t)$: Count of recorded trips where `end_station_id = s` and `ended_at` falls in time window $\Delta t$.
- **Interpretation**:
  - $\text{Net-Flow} > 0$: Net bike accumulation (Risk of full docks / no returns).
  - $\text{Net-Flow} < 0$: Net bike depletion (Risk of empty docks / starvation).
  - $\text{Net-Flow} \approx 0$: Balanced station equilibrium.

The reproducible command is `python src/analyze_trips.py`. Outputs are written to `data/processed/citibike_trips/2024-01/`. The archive contains some station endpoints outside Jersey City/Hoboken; the current analysis retains all station IDs present in the selected archive. Timestamps are grouped by the source's local wall-clock hour (the source fields do not include a timezone offset).

## 3. January 2024 Observations

- Weekday departures peak at 17:00 (189.48 departures per weekday on average), followed by 18:00 (186.17). The morning peak is 08:00 (162.13).
- Weekend departures peak at 15:00 (110.88 departures per weekend day on average), followed by 14:00 (107.50).
- Across the month, Grove St PATH (`JC115`) has the largest positive net-flow (+698); Brunswick St (`JC023`) has the most negative net-flow (-101).
- These are descriptive counts from one month. They do not establish recurring seasonal patterns or actual station inventory.

---

## 4. Important Analytical Caveats & Operational Realities

When presenting station net-flow findings, team members must explicitly highlight the following limitations:

1. **Net-flow is a Proxy, Not Real Dock Inventory**:
   Historical trip data only records trips completed by riders. It does **not** record operational rebalancing activities conducted by Citi Bike van and valet teams (who move bikes from crowded stations to depleted stations in real time).
2. **Station Capacity Constraints**:
   A station cannot accumulate bikes beyond its physical dock capacity (e.g. 30 docks). Once a station is full, prospective riders are turned away, truncating true organic arrival demand in the dataset.
3. **Maintenance & Out-of-Service Bikes**:
   Bikes flagged for repair or locked out do not show up as available in the fleet, skewing instantaneous dock capacity.
4. **Dockless E-Bike Dropoffs**:
   Electric bikes can occasionally be parked outside physical docks within authorized zones, meaning trips may record coordinates without explicit station IDs.
