### Factory Performance & Predictive Maintenance Dashboard

## Overview
This project is an end-to-end industrial analytics case study built using one year of simulated factory data.  
The goal of the project is to evaluate machine performance using OEE (Overall Equipment Effectiveness)and to monitor machine health using PLC vibration data.

The project reflects a real-world manufacturing analytics workflow and combines Excel, SQL, Python, and Power BI.

---

## Project Objectives
- Measure overall factory performance using OEE
- Analyze Availability, Performance, and Quality
- Compare efficiency across machines
- Understand downtime and production behavior
- Identify early machine health risks using vibration trends

---

## Data Description
The analysis is based on the following datasets:
- Production data (planned vs actual output)
- Quality data (good and rejected units)
- Maintenance data (downtime records)
- PLC sensor data (time-based vibration readings)

---

## Key Insights
- The factory operates at an average OEE of ~92%, indicating strong operational efficiency.
- Availability and quality are consistently high across all machines.
- Machine-wise OEE comparison highlights small but actionable performance differences.
- Vibration levels remain stable around 3.5 mm/s, well below the defined risk threshold.
- No sustained abnormal vibration trends were observed, suggesting healthy machine conditions.
- Downtime analysis helps identify opportunities for maintenance optimization.

---

## Tools & Technologies
- Excel    – Data study and normal cleaning
- MySQL    – Data storage and KPI calculations
- Python   – Time-series analysis and predictive maintenance logic and visualization
- Power BI – Interactive dashboards and visualization

---

## Dashboard Overview
- Executive Summary Page
  - Overall OEE
  - Availability, Performance, Quality
  - OEE by Machine

- Availability & Downtime Page
  - Downtime by machine(minutes)
  - Availability by machines
  
- Production & Quality Page
  - SUM of Actual Units
  - Good vs Rejected Units
  - Production by machines
  - Quality by machines

- Predictive Maintenance Page
  - Vibration trend with threshold line
  - Machine health status (Normal / Risk)

---

## Conclusion
This project demonstrates how manufacturing and PLC data can be transformed into meaningful insights for both management and maintenance teams. It showcases practical industrial analytics concepts, KPI calculation, and basic predictive maintenance analysis.

---

## Author
Muhammed Midhilaj E