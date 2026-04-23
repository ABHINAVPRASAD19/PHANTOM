import asyncio
import websockets
import json
import psutil
import socket
import time
import math
import multiprocessing
import os
from concurrent.futures import ProcessPoolExecutor

WEBSOCKET_URL = "ws://localhost:8000/ws/employee"
AGENT_NAME = socket.gethostname()

async def telemetry_loop(ws):
    while True:
        try:
            # Fetch 100% genuine OS telemetry via psutil!
            cpu_percent = psutil.cpu_percent(interval=None)
            mem = psutil.virtual_memory()
            
            payload = {
                "type": "telemetry",
                "max_cpu": psutil.cpu_count(logical=True),
                "cur_cpu": cpu_percent,
                "max_ram": round(mem.total / (1024**3), 1),
                "cur_ram": mem.percent,
                "gpu_name": "Hardware Accel"  # Fallback since OS GPU APIs differ greatly
            }
            await ws.send(json.dumps(payload))
            await asyncio.sleep(1)
        except websockets.exceptions.ConnectionClosed:
            break

def cpu_burn_task():
    """A blocking math loop designed to literally spike the physical OS CPU."""
    # Runs in a separate Process pool to fully saturate all cores!
    timeout = time.time() + 2  # Burn CPU for 2 solid seconds per chunk
    x = 0
    while time.time() < timeout:
        x += math.sqrt(64 * 3.14159) # heavy floating point
    return x

async def handle_server_messages(ws):
    loop = asyncio.get_running_loop()
    core_count = psutil.cpu_count(logical=True)
    
    async for message in ws:
        data = json.loads(message)
        
        if data.get("type") == "job_chunk":
            task_id = data.get("task_id")
            print(f"\n🔥 Received chunk [ {task_id} ] ({len(data.get('content',''))} bytes)")
            print(f"⚙️  Spiking all {core_count} local cores to process...")
            
            # Tell server we are officially processing
            await ws.send(json.dumps({"type": "status_update", "status": "BUSY"}))
            
            # Execute physical hardware burn across all CPU cores in parallel!
            # Since this is real math via ProcessPool, psutil CPU % will legitimately skyrocket 
            # to 100% on the host machine during the hackathon demo.
            with ProcessPoolExecutor(max_workers=core_count) as executor:
                futures = [loop.run_in_executor(executor, cpu_burn_task) for _ in range(core_count)]
                await asyncio.gather(*futures)
                
            print(f"✅ Finished chunk [ {task_id} ]. Standing by...\n")
            
            # Return result
            await ws.send(json.dumps({
                "type": "job_complete",
                "task_id": task_id,
                "result": "processed"
            }))

async def main():
    print(f"""
    ======================================
    🤖 SPOT AGENT INITIALIZATION
    ======================================
    Node Alias : {AGENT_NAME}
    OS Kernel  : {os.name}
    Hardware   : {psutil.cpu_count(logical=True)} Cores / {round(psutil.virtual_memory().total / (1024**3), 1)} GB RAM
    ======================================
    """)
    uri = f"{WEBSOCKET_URL}?name={AGENT_NAME}"
    print(f"🌐 Connecting to Orchestrator at {WEBSOCKET_URL}...")
    
    # Init cpu baseline
    psutil.cpu_percent()
    
    while True:
        try:
            async with websockets.connect(uri) as ws:
                print("✅ Handshake complete. Connected to Spot Cluster.")
                
                # Start parallel loops
                t_loop = asyncio.create_task(telemetry_loop(ws))
                msg_loop = asyncio.create_task(handle_server_messages(ws))
                
                done, pending = await asyncio.wait(
                    [t_loop, msg_loop],
                    return_when=asyncio.FIRST_COMPLETED
                )
                
                for task in pending:
                    task.cancel()
                    
        except websockets.exceptions.ConnectionClosedError:
            print("❌ Connection lost. Reconnecting in 3s...")
            await asyncio.sleep(3)
        except ConnectionRefusedError:
            print("❌ Orchestrator unreachable. Retrying in 3s...")
            await asyncio.sleep(3)

if __name__ == "__main__":
    asyncio.run(main())
