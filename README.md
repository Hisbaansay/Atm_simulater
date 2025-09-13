## 📌 Overview
This project is a lightweight **Air Traffic Management Simulator** implemented in **Python (asyncio)**.  
It simulates multiple aircraft sending telemetry data to a central **control tower**, which validates messages, logs them to CSV, and ensures structured reporting.  

The goal is to demonstrate **concurrency, reliability, validation, and structured logging** — key skills for safety-critical systems such as air traffic management.

---

## ⚙️ Features
- Simulates multiple aircraft streaming telemetry (latitude, longitude, altitude, speed, heading).
- Control tower validates messages against defined safety limits.
- Structured logging into **CSV format** for later analysis.
- Graceful shutdown with aircraft sending final `BYE` messages.
- Configurable runtime, number of aircraft, and message frequency.

---

## 🖥️ Example Run
```bash
python atm_sim_py.py
