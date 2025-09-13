#!/usr/bin/env python3
"""
Air Traffic Management (ATM) Simulator - Python / asyncio

- Multiple aircraft produce position/status messages.
- A control tower consumes messages from an asyncio.Queue.
- Messages are validated (lat/lon/alt bounds) and logged.
- Clean shutdown after a configured runtime.

Run:
  python atm_sim_py.py
Options:
  Adjust CONFIG at the top or run with env vars if you extend it.
"""

import asyncio
import csv
import random
import signal
import sys
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional

# ----------------- Configuration -----------------
class CONFIG:
    NUM_AIRCRAFT = 5
    RUNTIME_SECONDS = 12           # total simulation time
    MIN_SEND_MS = 120              # min interval between aircraft messages
    MAX_SEND_MS = 500              # max interval
    LOG_CSV = "atm_log.csv"        # CSV log file
    PRINT_PROGRESS_EVERY = 50      # tower prints every N processed messages

# ----------------- Data Model --------------------
@dataclass
class Message:
    aircraft_id: int
    latitude: float      # degrees
    longitude: float     # degrees
    altitude_ft: float   # feet
    speed_kt: float      # knots
    heading_deg: float   # 0..360
    status: str          # "OK", "WARN", "BYE"
    ts: str              # ISO8601 UTC timestamp

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def validate(msg: Message) -> bool:
    if not (-90.0 <= msg.latitude <= 90.0): return False
    if not (-180.0 <= msg.longitude <= 180.0): return False
    if not (0.0 <= msg.altitude_ft <= 60000.0): return False
    if not (0.0 <= msg.speed_kt <= 700.0): return False
    if not (0.0 <= msg.heading_deg <= 360.0): return False
    return True

# ----------------- Aircraft Task -----------------
async def aircraft_task(aircraft_id: int, q: asyncio.Queue, running: asyncio.Event):
    rng = random.Random(aircraft_id * 7331)
    # Initialize plausible starting state
    lat = rng.uniform(-60, 60)
    lon = rng.uniform(-120, 120)
    alt = rng.uniform(10000, 39000)  # feet
    spd = rng.uniform(250, 480)      # knots
    hdg = rng.uniform(0, 360)        # degrees

    while running.is_set():
        # Slight random walk to simulate movement
        lat += rng.uniform(-0.05, 0.05)
        lon += rng.uniform(-0.05, 0.05)
        alt += rng.uniform(-400, 400)
        alt = max(0, min(alt, 60000))
        spd += rng.uniform(-10, 10)
        spd = max(0, min(spd, 700))
        hdg = (hdg + rng.uniform(-5, 5)) % 360

        # Occasional WARN to mimic minor deviations
        status = "OK"
        if rng.random() < 0.02:
            status = "WARN"

        msg = Message(
            aircraft_id=aircraft_id,
            latitude=lat,
            longitude=lon,
            altitude_ft=alt,
            speed_kt=spd,
            heading_deg=hdg,
            status=status,
            ts=now_iso(),
        )
        await q.put(msg)

        # Wait a randomized interval
        await asyncio.sleep(rng.uniform(CONFIG.MIN_SEND_MS/1000, CONFIG.MAX_SEND_MS/1000))

    # Send final BYE
    await q.put(Message(
        aircraft_id=aircraft_id,
        latitude=0.0, longitude=0.0, altitude_ft=0.0, speed_kt=0.0, heading_deg=0.0,
        status="BYE", ts=now_iso()
    ))

# ----------------- Control Tower -----------------
class ControlTower:
    def __init__(self, q: asyncio.Queue, csv_path: Optional[str] = None):
        self.q = q
        self.csv_path = csv_path
        self.processed = 0
        self.rejected = 0
        self.byes = 0
        self.writer = None
        self.csv_file = None

    async def __aenter__(self):
        if self.csv_path:
            self.csv_file = open(self.csv_path, "w", newline="", encoding="utf-8")
            self.writer = csv.DictWriter(self.csv_file, fieldnames=list(asdict(Message(0,0,0,0,0,0,"",now_iso())).keys()) + ["valid"])
            self.writer.writeheader()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.csv_file:
            self.csv_file.close()

    async def run(self, running: asyncio.Event, num_aircraft: int):
        while running.is_set() or self.byes < num_aircraft:
            try:
                msg: Message = await asyncio.wait_for(self.q.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue

            if msg.status == "BYE":
                self.byes += 1
                print(f"[TOWER] Aircraft {msg.aircraft_id} terminated @ {msg.ts} (BYE {self.byes}/{num_aircraft})")
                continue

            is_valid = validate(msg)
            if is_valid:
                self.processed += 1
                if self.processed % CONFIG.PRINT_PROGRESS_EVERY == 0:
                    print(f"[TOWER] Processed {self.processed} messages (rejected={self.rejected})")
            else:
                self.rejected += 1
                print(f"[WARN] Rejected from AC{msg.aircraft_id} @ {msg.ts} (lat={msg.latitude:.2f}, lon={msg.longitude:.2f}, alt={msg.altitude_ft:.0f})")

            if self.writer:
                row = asdict(msg)
                row["valid"] = 1 if is_valid else 0
                self.writer.writerow(row)

        print(f"[TOWER] Final: processed={self.processed} rejected={self.rejected} byes={self.byes}")

# ----------------- Orchestrator ------------------
async def main():
    # Ctrl+C handling
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    stop_event.set()

    def handle_sig(*_):
        if stop_event.is_set():
            print("\n[INFO] Immediate stop requested. Exiting...")
            stop_event.clear()
        else:
            # second Ctrl+C will exit immediately
            sys.exit(1)

    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(s, handle_sig)
        except NotImplementedError:
            # Windows event loop may not support signal handlers; ignore gracefully
            pass

    q: asyncio.Queue[Message] = asyncio.Queue(maxsize=1000)

    # Start aircraft tasks
    aircraft_tasks = [
        asyncio.create_task(aircraft_task(i+1, q, stop_event))
        for i in range(CONFIG.NUM_AIRCRAFT)
    ]

    # Start tower
    async with ControlTower(q, CONFIG.LOG_CSV) as tower:
        # Run for configured duration
        print(f"[INFO] Running ATM sim for {CONFIG.RUNTIME_SECONDS}s with {CONFIG.NUM_AIRCRAFT} aircraft...")
        try:
            await asyncio.sleep(CONFIG.RUNTIME_SECONDS)
        finally:
            # signal aircraft to stop & drain BYEs
            stop_event.clear()
            await tower.run(stop_event, CONFIG.NUM_AIRCRAFT)

    # Ensure aircraft tasks complete
    await asyncio.gather(*aircraft_tasks, return_exceptions=True)
    print("[INFO] Simulation complete. Log written to:", CONFIG.LOG_CSV)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[INFO] Interrupted.")
