from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
import json
import asyncio
from typing import Dict
import uuid
import math
import time

import os
from pathlib import Path
from contextlib import asynccontextmanager

# Robust path discovery for Render/Local
BASE_DIR = Path(__file__).resolve().parent.parent
UI_DIR = BASE_DIR / "ui"

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup logic
    print(f"PHANTOM Orchestrator: Starting up. UI directory: {UI_DIR}")
    asyncio.create_task(distribute_jobs_loop())
    yield
    # Shutdown logic
    print("PHANTOM Orchestrator: Shutting down...")

app = FastAPI(title="Phantom", lifespan=lifespan)

if not UI_DIR.exists():
    print(f"CRITICAL ERROR: UI Directory not found at {UI_DIR}")
else:
    app.mount("/ui", StaticFiles(directory=str(UI_DIR), html=True), name="ui")

@app.get("/health")
async def health():
    return {"status": "healthy", "timestamp": time.time(), "agents_connected": len(agents_state)}

@app.get("/")
async def root():
    return RedirectResponse(url="/ui/login.html")

# Global State
admin_connections = []
employee_connections: Dict[str, WebSocket] = {}
agents_state: Dict[str, dict] = {}
job_queue = []
active_jobs: Dict[str, dict] = {}
workloads_state: Dict[str, dict] = {}
paused_workloads = set()

async def distribute_jobs_loop():
    """Background loop that constantly checks if there are jobs in queue and IDLE workers to execute them."""
    while True:
        # 1. Identify all workers that are actually available right now
        idle_worker_ids = [
            k for k, v in agents_state.items() 
            if v["status"] == "IDLE" 
            and v.get("cur_cpu", 100) < 85
        ]

        # 2. If no workers or no jobs, just wait
        if not idle_worker_ids or not job_queue:
            # Check if we need to update queue reasons if no nodes are connected
            if not agents_state and job_queue:
                for j in job_queue:
                    fname = j['task_id'].split("_chunk_")[0]
                    if fname in workloads_state:
                        workloads_state[fname]["queue_reason"] = "Awaiting Nodes to Connect"
                await broadcast_to_admins()
            await asyncio.sleep(1.0)
            continue

        # 3. FAIR DISTRIBUTION: Interleave chunks from different workloads
        # We'll track which files we've already handed out in THIS tick to ensure diversity.
        assigned_filenames_in_tick = set()
        
        for emp_id in idle_worker_ids:
            if not job_queue:
                break
                
            target_idx = -1
            
            # Strategy Phase A: Find a chunk from a file we haven't touched yet this tick
            for idx, j in enumerate(job_queue):
                fname = j['task_id'].split("_chunk_")[0]
                if fname not in paused_workloads and fname not in assigned_filenames_in_tick:
                    target_idx = idx
                    break
            
            # Strategy Phase B: If we've already given out 1 chunk of every file, 
            # just grab the next available non-paused chunk.
            if target_idx == -1:
                for idx, j in enumerate(job_queue):
                    fname = j['task_id'].split("_chunk_")[0]
                    if fname not in paused_workloads:
                        target_idx = idx
                        break
            
            if target_idx == -1:
                # All remaining jobs are paused
                break
                
            job = job_queue[target_idx]
            filename = job['task_id'].split("_chunk_")[0]
            mem_req = job.get('mem_required', 100)
            
            # Capacity Check
            worker_ram = agents_state[emp_id].get("available_ram", float('inf'))
            if worker_ram < mem_req:
                # This specific worker doesn't have the RAM for this specific job.
                # Skip this worker for this tick.
                continue

            # 4. DISPATCH
            conn = employee_connections.get(emp_id)
            if conn:
                job_queue.pop(target_idx) # Remove from queue
                assigned_filenames_in_tick.add(filename)
                
                try:
                    agents_state[emp_id]['status'] = "BUSY"
                    # simulate RAM consumption locally
                    if worker_ram != float('inf'):
                        agents_state[emp_id]["available_ram"] = max(0, worker_ram - mem_req)
                    active_jobs[emp_id] = job
                    
                    if filename in workloads_state:
                        workloads_state[filename]["queue_reason"] = ""
                        workloads_state[filename]["status"] = "active"

                    await conn.send_text(json.dumps({
                        "type": "job_chunk",
                        "task_id": job['task_id'],
                        "content": job['content'],
                        "api_key": job.get('api_key', ''),
                        "instruction": job.get('instruction', '')
                    }))
                    
                    # Tell admin the job has started processing (redundant but keeps it safe)
                    await broadcast_to_admins({
                        "type": "job_started", 
                        "filename": filename
                    })
                    
                    print(f"Assigned {job['task_id']} to {agents_state[emp_id]['hostname']}")
                except Exception as e:
                    print(f"Error assigning job: {e}")
                    job_queue.insert(0, job) # Put it back
                    if emp_id in active_jobs: del active_jobs[emp_id]
        
        # 5. SYNC
        await broadcast_to_admins()
        await asyncio.sleep(0.5)


@app.websocket("/ws/admin")
async def websocket_admin(websocket: WebSocket):
    global job_queue
    await websocket.accept()
    admin_connections.append(websocket)
    # Send immediate state on connect
    await websocket.send_text(json.dumps({"type": "state_update", "agents": agents_state}))
    
    try:
        while True:
            data = await websocket.receive_text()
            payload = json.loads(data)
            
            if payload.get("type") == "distribute_file":
                content = payload.get("content", "")
                filename = payload.get("filename", "unknown.txt")
                
                api_key = payload.get("api_key", "")
                mem_required = payload.get("mem_required", 100)
                instruction = payload.get("instruction", "")
                
                # CHUNKING LOGIC: Break the uploaded file into smaller batches
                chunk_size = 1500  # Give Gemini enough context per chunk
                chunks = [content[i:i+chunk_size] for i in range(0, len(content), chunk_size)]
                
                for i, chunk in enumerate(chunks):
                    job_queue.append({
                        "task_id": f"{filename}_chunk_{i+1}",
                        "content": chunk,
                        "api_key": api_key,
                        "mem_required": mem_required,
                        "instruction": instruction,
                        "chunks_total": len(chunks)
                    })
                
                # STRICT SJF SCHEDULING: Sort entire queue by total chunk magnitude
                job_queue.sort(key=lambda x: x.get("chunks_total", 999999))
                
                print(f"Queued {len(chunks)} jobs for {filename}")
                
                # PERSISTENCE: Store the high-level workload state
                workloads_state[filename] = {
                    "name": filename,
                    "size": len(content),
                    "chunks_total": len(chunks),
                    "chunks_done": 0,
                    "status": "queued",
                    "findings": []
                }
                await broadcast_to_admins()
                
            elif payload.get("type") == "cancel_task":
                filename = payload.get("filename", "")
                if filename:
                    job_queue = [j for j in job_queue if not j["task_id"].startswith(f"{filename}_chunk")]
                    print(f"Cancelled remaining jobs for {filename}")
                    
                    await broadcast_to_admins({
                        "type": "task_cancelled",
                        "filename": filename
                    })
                    if filename in workloads_state:
                        del workloads_state[filename]
                    await broadcast_to_admins()
                    
            elif payload.get("type") == "archive_task":
                filename = payload.get("filename", "")
                if filename and filename in workloads_state:
                    workloads_state[filename]["status"] = "archived"
                    await broadcast_to_admins()
                    
            elif payload.get("type") == "pause_task":
                filename = payload.get("filename", "")
                if filename and filename in workloads_state:
                    workloads_state[filename]["status"] = "paused"
                    paused_workloads.add(filename)
                    await broadcast_to_admins()
                    
            elif payload.get("type") == "resume_task":
                filename = payload.get("filename", "")
                if filename and filename in workloads_state:
                    if workloads_state[filename]["chunks_done"] < workloads_state[filename]["chunks_total"]:
                         workloads_state[filename]["status"] = "active"
                    else:
                         workloads_state[filename]["status"] = "completed"
                    paused_workloads.discard(filename)
                    await broadcast_to_admins()
                
    except WebSocketDisconnect:
        if websocket in admin_connections:
            admin_connections.remove(websocket)
    except Exception as e:
        print(f"Admin WebSocket error: {e}")
        if websocket in admin_connections:
            admin_connections.remove(websocket)

@app.websocket("/ws/employee")
async def websocket_employee(websocket: WebSocket):
    # Retrieve the name from the query params, e.g. ws://.../?name=Bob
    name = websocket.query_params.get("name", "Unknown-Agent")
    emp_id = str(uuid.uuid4())[:8]
    
    await websocket.accept()
    employee_connections[emp_id] = websocket
    
    agents_state[emp_id] = {
        "hostname": name,
        "status": "IDLE",
        "completed_tasks": 0,
        "credits": 0,
        "cpu": 0,
        "ram": 0
    }
    await broadcast_to_admins()
    
    try:
        while True:
            data = await websocket.receive_text()
            payload = json.loads(data)
            
            if payload.get("type") == "status_update":
                agents_state[emp_id]["status"] = payload["status"]
                await broadcast_to_admins()
                
            elif payload.get("type") == "job_complete":
                agents_state[emp_id]["completed_tasks"] += 1
                agents_state[emp_id]["credits"] += 10
                agents_state[emp_id]["status"] = "IDLE"  # Free up for the next chunk
                task_id = payload.get('task_id', '')
                result = payload.get('result', '')
                print(f"{name} completed task: {task_id}")
                
                # Tell admin a chunk is done
                filename = task_id.split("_chunk_")[0]
                
                # Free up in-flight tracker
                if emp_id in active_jobs:
                    del active_jobs[emp_id]
                    
                # Update server-side persistence
                if filename in workloads_state:
                    workloads_state[filename]["chunks_done"] += 1
                    if workloads_state[filename]["chunks_done"] >= workloads_state[filename]["chunks_total"]:
                        workloads_state[filename]["status"] = "completed"
                        if "completed_time" not in workloads_state[filename]:
                            workloads_state[filename]["completed_time"] = time.time() * 1000
                    else:
                        workloads_state[filename]["status"] = "active"
                
                await broadcast_to_admins({
                    "type": "chunk_completed",
                    "filename": filename
                })
                
                # Push the AI intelligence to the Admin console AND persist it
                if result and result.upper() != "CLEAN":
                    if filename in workloads_state:
                        workloads_state[filename]["findings"].append({
                            "worker": name,
                            "insight": result
                        })
                    
                    await broadcast_to_admins({
                        "type": "job_result",
                        "worker": name,
                        "result": result
                    })
                
                # Keep sending standard state updates
                await broadcast_to_admins()
                
            elif payload.get("type") == "heartbeat":
                pass # keep connection alive
                
            elif payload.get("type") == "telemetry":
                agents_state[emp_id].update({
                    "max_cpu": payload.get("max_cpu"),
                    "cur_cpu": payload.get("cur_cpu"),
                    "max_ram": payload.get("max_ram"),
                    "cur_ram": payload.get("cur_ram"),
                    "available_ram": payload.get("available_ram", 1000),
                    "gpu_name": payload.get("gpu_name")
                })
                await broadcast_to_admins()
                
    except WebSocketDisconnect:
        print(f"Employee {name} disconnected.")
        
        # IN-FLIGHT RECOVERY: If worker drops while holding a workload, re-queue it immediately!
        if emp_id in active_jobs:
            lost_job = active_jobs.pop(emp_id)
            job_queue.insert(0, lost_job)
            print(f"Recovered lost chunk {lost_job['task_id']} and re-inserted to queue!")
            
        if emp_id in agents_state:
            del agents_state[emp_id]
        if emp_id in employee_connections:
            del employee_connections[emp_id]
        await broadcast_to_admins()
    except Exception as e:
        print(f"Employee WebSocket error ({name}): {e}")
        if emp_id in active_jobs:
            lost_job = active_jobs.pop(emp_id)
            job_queue.insert(0, lost_job)
        if emp_id in agents_state:
            del agents_state[emp_id]
        if emp_id in employee_connections:
            del employee_connections[emp_id]
        await broadcast_to_admins()

async def broadcast_to_admins(custom_msg=None):
    if not admin_connections:
        return
        
    if custom_msg:
        msg_str = json.dumps(custom_msg)
    else:
        # Attach real-time specific worker mapping to workloads
        for wl in workloads_state.values():
            wl["active_nodes"] = []
            
        for emp_id, job in active_jobs.items():
            fname = job["task_id"].split("_chunk_")[0]
            if fname in workloads_state:
                agent_name = agents_state.get(emp_id, {}).get("hostname", "Unknown")
                if agent_name not in workloads_state[fname]["active_nodes"]:
                    workloads_state[fname]["active_nodes"].append(agent_name)
                    
        msg_str = json.dumps({
            "type": "state_update", 
            "agents": agents_state,
            "workloads": workloads_state
        })
        
    disconnected = []
    for conn in admin_connections:
        try:
            await conn.send_text(msg_str)
        except Exception:
            disconnected.append(conn)
            
    for conn in disconnected:
        if conn in admin_connections:
            admin_connections.remove(conn)

if __name__ == "__main__":
    import uvicorn
    import os
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
