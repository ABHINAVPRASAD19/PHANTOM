# PHANTOM - Distributed AI Workload Scheduler

## Problem Statement
In an era where AI demand has driven hardware prices to record highs, we face a massive paradox: millions of high-end corporate laptops sit idle for 70% of the day, their CPU and RAM power going completely to waste. **PHANTOM** bridges this gap by harvesting dormant compute capacity from existing hardware. We don't need to buy more power; we just need to stop wasting the power we already have.

PHANTOM is a high-performance prototype for a distributed spot market scheduler designed to handle AI workloads across multiple nodes.

## Features
- **Real-time Orchestration:** FastAPI-based server managing WebSocket connections.
- **Dynamic Chunking:** Automatically breaks large files into manageable tasks.
- **Fair SJF Scheduling:** Implements Shortest Job First with fair interleaved distribution.
- **Live Monitoring:** Admin dashboard to track worker telemetry (CPU/RAM) and task progress.

## Setup & Installation

### Prerequisites
- Python 3.10+
- Browser (Opera recommended)

### Local Development
1. **Clone/Download** the repository.
2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```
3. **Start the Orchestrator:**
   ```bash
   python server/main.py
   ```
4. **Access the Portal:**
   Open `http://localhost:8000` in your browser.

## Project Structure
- `/server`: Core FastAPI logic and orchestration loop.
- `/ui`: Modern dashboards for Admins and Workers.
- `/agent`: (Future) Local agent scripts for hardware integration.

---
*Created for the 1st Solution GDC Hackathon.*
