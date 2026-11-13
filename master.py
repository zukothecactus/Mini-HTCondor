from fastapi import FastAPI, HTTPException, File, UploadFile, WebSocket, WebSocketDisconnect, websockets
from fastapi.responses import FileResponse
from pathlib import Path
from typing import Dict, Optional, Set
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from types import SimpleNamespace
import uvicorn
import asyncio
import os
import json
import shutil

from models import Job, Worker, JobStatus, WorkerStatus, JobUpdateRequest, HeartbeatRequest

#collector, negotiator and scheduler

LEASE_DURATION = 20  # seconds -> dead worker

#spool dir for sandboxes
SPOOL_DIR = Path("spool_files")
SPOOL_DIR.mkdir(exist_ok=True)
SPOOL_FILE = SPOOL_DIR / "spool.json"  # File to persist job state

#in-memory storage for jobs and workers (adding info, but also match making)
jobs: Dict[str, Job]= {}
workers: Dict[str, Worker] = {}

#WebSocket control channels from workers: {worker_id: WebSocket}
worker_control_sockets: Dict[str, WebSocket] = {}

#log streaming queues per job {job_id: Set[asyncio.Queue]}
'''this is the structure
{
    "job-123": {queue_a, queue_b},
    "job-456": {queue_c}
}
'''

job_log_subscribers: Dict[str, Set[asyncio.Queue]] = {}



def evaluate_classad(job: Job, worker: Worker) -> bool:
    """Evaluates a job's requirement string against worker attributes."""
    if not job.requirements:
        return True  # No requirements specified, match any worker
    
    # Flatten custom attributes into the main namespaces for easy dot-notation access
    """ explaining the logic here:
        worker.model_dump() returns a dict of the Pydantic model's fields and values. ({worker_id:"123", "status":"IDLE", ...})
        ** unpacks the dictionary, so we can merge it with the custom attributes.
        the merge happebs by unpacking both the model_dump() and the custom_attributes dict into a new dict. Python merges them and if there are any duplicate keys, the latter one (custom_attributes) will overwrite the former (model_dump()).
    ----------------------------------------------------------
        worker.model_dump() returns:
        {
        "offered_cpus_available": 4.0,
        "custom_attributes": {
            "os_family": "el9",
            "has_cvmfs": True
            }
        }
    ----------------------------------------------------------
        worker.custom_attributes are:
        {
        "os_family": "el9",
        "has_cvmfs": True
        }
    ----------------------------------------------------------
        the merge turns into this (w_dict and j_dict):
        {
        "offered_cpus_available": 4.0,
        "os_family": "el9",
        "has_cvmfs": True
        } 
    """
    w_dict = {**worker.model_dump(), **worker.custom_attributes}
    j_dict = {**job.model_dump(), **job.custom_attributes}

    #SimpleNamespace allows us to use dot notation for attribute access, kind of like dinamically generating a lightweight struct at runtime in C/C++
    # (worker.offered_cpus_available instead of worker_dict["offered_cpus_available"])
    locals_dict = {
        "worker": SimpleNamespace(**w_dict),
        "job": SimpleNamespace(**j_dict)
    }
    
    try:
        # __builtins__: None secures the eval context against arbitrary code execution
        match = eval(job.requirements, {"__builtins__": None}, locals_dict)
        return bool(match)
    except Exception as e:
        print(f"[MASTER] Error evaluating ClassAd for job {job.job_id} and worker {worker.worker_id}: {e}")
        return False
    
def load_state():
    '''read the spool file on startup and reconstuct Pydantic Job models'''
    if os.path.exists(SPOOL_FILE):
        try:
            with open(SPOOL_FILE, "r") as f:
                data = json.load(f)
                for job_id, job_dict in data.items():
                    jobs[job_id] = Job(**job_dict)
                print(f"[MASTER] Loaded {len(jobs)} jobs from spool file.")
        except Exception as e:
            print(f"[MASTER] Failed to load spool file: {e}")
            
def save_state():
    '''dumps the current job dict to disk'''
    try:
        with open(SPOOL_FILE, "w") as f:
            json_ready_jobs = {k: v.model_dump(mode='json') for k, v in jobs.items()}
            json.dump(json_ready_jobs, f, indent=2)
    except Exception as e:
        print(f"[MASTER] Failed to save spool file: {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start background negotiator / reconciliation task on app startup
    reconciliation_task = asyncio.create_task(master_reconciliation_loop())
    yield
    reconciliation_task.cancel()

app = FastAPI(title = "Mini-HTCondor master node", description = "Implementation of a master node with ClassAd matchmaking and lease reconciliation.", lifespan = lifespan)

async def master_reconciliation_loop():
    ''' periodic bk loop: checks worker health and requeues lost jobs'''
    while True:
        state_changed = False
        now = datetime.now(timezone.utc)
        for worker_id, worker in list(workers.items()):
            seconds_since_last_heartbeat = (now - worker.last_heartbeat).total_seconds()
            if seconds_since_last_heartbeat > LEASE_DURATION and worker.status!=WorkerStatus.DEAD:
                print(f"[MASTER] Worker {worker_id} is DEAD. Last heartbeat was {seconds_since_last_heartbeat:.2f} seconds ago. Requeuing its jobs...")
                #mark the worker as dead
                worker.status = WorkerStatus.DEAD
                
                for job in jobs.values():
                    if job.assigned_worker_id == worker_id and job.status in [JobStatus.ASSIGNED, JobStatus.RUNNING]:
                        job.assigned_worker_id = None
                        job.retry_count += 1
                        if job.retry_count >job.max_retries:
                            print(f"[MASTER] Re-queueing job {job.job_id} (Retry {job.retry_count}/{job.max_retries}).")
                            job.status = JobStatus.HELD
                        else:
                            print(f"[MASTER] Re-queueing job {job.job_id} (Retry {job.retry_count}/{job.max_retries}).")
                            job.status = JobStatus.QUEUED
                        state_changed = True
                        
        if state_changed:
            save_state()
        await asyncio.sleep(5)

#adding a job to the queue
@app.post("/jobs", response_model = Job)
def submit_job(job_req:Job):
    """client submits a new job to the cluster"""
    
    #pydantic automatically generates a job_id and a timestamp
    jobs[job_req.job_id] = job_req
    
    save_state()  # Persist the updated state to disk
    return job_req

@app.post("/workers/{worker_id}/heartbeat")
def worker_heartbeat(worker_id:str, 
                     heartbeat_req: HeartbeatRequest):
    """worker pings this endpoint every few secontd to let master know that it is alive"""
    
    if(worker_id not in workers):
        workers[worker_id] = Worker(worker_id = worker_id,
                                    status= heartbeat_req.status,
                                    offered_cpus_total=heartbeat_req.offered_cpus,
                                    offered_cpus_available=heartbeat_req.offered_cpus,
                                    offered_ram_total=heartbeat_req.offered_ram,
                                    offered_ram_available=heartbeat_req.offered_ram,
                                    custom_attributes=heartbeat_req.custom_attributes)
    else:
        workers[worker_id].last_heartbeat = datetime.now(timezone.utc)
        workers[worker_id].status = heartbeat_req.status
                
    return workers[worker_id]

@app.get("/jobs/next", response_model=Optional[Job])
def get_next_job(worker_id:str):
    """Matches pending QUEUED jobs against worker resources (ClassAd matching)."""
    
    if(worker_id not in workers):
        raise HTTPException(status_code=403, detail="Worker not registered")
    
    worker = workers[worker_id]
    if worker.status == WorkerStatus.DEAD:
        raise HTTPException(status_code=403, detail="Worker is marked as DEAD")
    
    #Fair-Share matchmaking algorithm
    user_usage = {}
    for j in jobs.values():
        if j.status in (JobStatus.ASSIGNED, JobStatus.RUNNING):
            user = j.submitter_id
            user_usage[user] = user_usage.get(user, 0) + j.required_cpus  # or j.required_ram, depending on the metric you want to use
    
    queued_jobs = [j for j in jobs.values() if j.status == JobStatus.QUEUED]

    #sort the queued jobs
    #primarily by submitter's current cluster usage (lower usage gets higher priority)
    #secondarily by th eusers own job priority (lower number means higher priority)
    #finally by job_creation time (FIFO fallback)
    
    queued_jobs.sort(key=lambda j: (
        user_usage.get(j.submitter_id, 0), 
        j.user_priority, 
        j.created_at
    ))
    
    #backfilling: if a job has an expected runtime that is shorter than the current time slot, it can be scheduled even if it is not the highest priority job.
    shadow_job = None
    
    #ClassAd matching
    for job in queued_jobs:
        #DAG dependency check
        dependencies_met = True
        for parent_id in job.depends_on:
            parent_job = jobs.get(parent_id)
            if not parent_job or parent_job.status != JobStatus.COMPLETED:
                dependencies_met = False
                break
        
        if not dependencies_met:
            continue  # Skip this job if dependencies are not met
        
        #checking resource requirements (regardless of priority or backfilling)
        if not evaluate_classad(job, worker):
            continue  # Skip this job if requirements are not met
        
        can_fit_eventually = (job.required_cpus <= worker.offered_cpus_total and job.required_ram <= worker.offered_ram_total)
        can_run_now = (job.required_cpus <= worker.offered_cpus_available and job.required_ram <= worker.offered_ram_available)


        if can_fit_eventually:
            
            #first job we find that can eventually fit is our top priority for this worker 
            #the 1st job that passes will also be the biggest one with the highest priority that is offered (sorting)
            if not shadow_job:
                shadow_job = job
            
            if can_run_now:
                is_top_job = (job.job_id == shadow_job.job_id)
            
                #if its not the top job, we're backfilling
                #we only allow short jobs to run
                if not is_top_job and job.estimated_runtime > 600:
                    continue  # Skip the long job (would starve the shadow job otherwise)
                
            #assign the job
            job.status = JobStatus.ASSIGNED
            job.assigned_worker_id = worker_id
            
            worker.offered_cpus_available -= job.required_cpus
            worker.offered_ram_available -= int(job.required_ram)
            
            if worker.offered_cpus_available == 0: 
                worker.status = WorkerStatus.BUSY

            save_state()  # Persist the updated state to disk
            return job
    
    return None #no jobs available

@app.put("/jobs/{job_id}")
def update_job(
    job_id:str,
    update_req: JobUpdateRequest
):
    """worker updates the outcome of a job"""
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job = jobs[job_id]
    
    #checking if this is the 1st time  the job is being marked as COMPLETED or FAILED (to avoid double counting)
    is_newly_finished = (job.status not in [JobStatus.COMPLETED, JobStatus.FAILED]) and (update_req.status in [JobStatus.COMPLETED, JobStatus.FAILED])
    
    #updating the state
    job.status = update_req.status
    
    if update_req.exit_code is not None:
        job.exit_code = update_req.exit_code
    if update_req.stdout is not None:
        job.stdout = update_req.stdout
    if update_req.stderr is not None:
        job.stderr = update_req.stderr
        
    if update_req.status == JobStatus.FAILED:
        for child_job in jobs.values():
            if job_id in child_job.depends_on: #if the failed job is a parent of another job, we mark it as HELD (cant run till the parent is fixed)
                child_job.status = JobStatus.HELD
                print(f"[MASTER] Job {child_job.job_id} HELD due to parent {job_id} failure.")        
        
    # 3. Only restore resources if the transition actually happened just now
    if is_newly_finished:
        if job.assigned_worker_id and job.assigned_worker_id in workers:
            w = workers[job.assigned_worker_id]
            #restoring the master node capacity for the worker (with a min() clamp, just in case)
            w.offered_cpus_available = min(w.offered_cpus_total, w.offered_cpus_available + job.required_cpus)
            w.offered_ram_available = min(w.offered_ram_total, w.offered_ram_available + int(job.required_ram))
            
            # For dynamic multi-tenancy, a worker is only IDLE if it has 100% of its resources free.
            # Otherwise, it remains BUSY (partially occupied).
            if w.offered_cpus_available == w.offered_cpus_total and w.offered_ram_available == w.offered_ram_total:
                w.status = WorkerStatus.IDLE
           
    save_state()  # Persist the updated state to disk
    return job


#file sandboxing endpoints
@app.post("/jobs/{job_id}/input")
async def upload_job_input(job_id: str, file: UploadFile = File(...)):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    file_path = SPOOL_DIR / f"{job_id}_in.tar.gz"
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
        
    jobs[job_id].has_input_sandbox = True
    if jobs[job_id].status == JobStatus.SPOOLING:
        jobs[job_id].status = JobStatus.QUEUED  # Move to QUEUED if it was waiting for input files
    save_state()  # Persist the updated state to disk
    return {"message": "Input files uploaded successfully."}

@app.get("/jobs/{job_id}/input")
def download_job_input(job_id: str):
    file_path = SPOOL_DIR / f"{job_id}_in.tar.gz"
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="Input sandbox not found")
    return FileResponse(file_path, media_type='application/gzip', filename=f"{job_id}_in.tar.gz")
     
@app.post("/jobs/{job_id}/output")
async def upload_job_output(job_id: str, file: UploadFile = File(...)):
    file_path = SPOOL_DIR / f"{job_id}_out.tar.gz"
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    jobs[job_id].has_output_sandbox = True
    save_state()  # Persist the updated state to disk
    return {"message": "Output files uploaded successfully."}

#job checkpointing endpoints
@app.post("/jobs/{job_id}/checkpoint")
async def upload_job_checkpoint(job_id: str, file: UploadFile = File(...)):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    file_path = SPOOL_DIR / f"{job_id}_checkpoint.tar.gz"
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    jobs[job_id].has_checkpoint = True
    save_state()  # Persist the updated state to disk
    return {"message": "Checkpoint files uploaded successfully."}

@app.get("/jobs/{job_id}/checkpoint")
def download_job_checkpoint(job_id: str):
    file_path = SPOOL_DIR / f"{job_id}_checkpoint.tar.gz"
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="Checkpoint sandbox not found")
    return FileResponse(file_path, media_type='application/gzip', filename=f"{job_id}_checkpoint.tar.gz")


#condor_tail (log streaming) endpoints-------------------------------------------
'''
Worker --------------------> Master
        log-ingest websocket

Worker <-------------------- Master
        control websocket

Client <-------------------- Master
        tail/log websocket
'''
#opening a WS for every worker
@app.websocket("/ws/worker/{worker_id}")
async def worker_control_es(websocket: WebSocket, worker_id:str):
    '''perisistant WebSocket control channel for each worker (outbound)'''
    await websocket.accept()
    worker_control_sockets[worker_id] = websocket
    print(f"[MASTER] Control WebSocket registered for {worker_id}")
    
    try:
        while True:
            raw = await websocket.receive_text()  # Keep the connection alive
            msg = json.loads(raw)
            msg_type = msg.get("type")
            job_id = msg.get("job_id")
            
            if msg_type == "log_chunk" and job_id:
                chunk = msg.get("data", "")
                for q in list(job_log_subscribers.get(job_id, [])):
                    await q.put(chunk)
            elif msg_type == "log_stream_end" and job_id:
                for q in list(job_log_subscribers.get(job_id, [])):
                    await q.put(None)  # None signals EOF
    except WebSocketDisconnect:
        print(f"[MASTER] Control WebSocket disconnected for {worker_id}")
    finally:
        if worker_control_sockets.get(worker_id) == websocket:
            del worker_control_sockets[worker_id]
        # Unblock any CLI clients tailing jobs on a worker that just disconnected
        for j_id, job in jobs.items():
            if job.assigned_worker_id == worker_id and j_id in job_log_subscribers:
                for q in list(job_log_subscribers[j_id]):
                    await q.put(f"\n[MASTER] Lost connection to worker {worker_id}.\n")
                    await q.put(None)  # Signal EOF to all subscribers

@app.websocket("/ws/jobs/{job_id}/stream")
async def client_tail_ws(websocket: WebSocket, job_id:str):
    """Client endpoint for `submit.py --tail <job_id>`."""
    await websocket.accept()

    if job_id not in jobs:
        await websocket.send_text(f"[MASTER] Error: Job {job_id} not found.\n")
        await websocket.close(code=1008) #policy violation (invalid job_id)
        return

    job = jobs[job_id]

    # if the job is already finished, flush stored logs immediately
    if job.status in (JobStatus.COMPLETED, JobStatus.FAILED):
        await websocket.send_text(f"[MASTER] Job {job_id} already {job.status.value}. Dumping logs:\n") 
        if job.stdout:
            await websocket.send_text(job.stdout)
        if job.stderr:
            await websocket.send_text(f"\n--- STDERR ---\n{job.stderr}")
        await websocket.close(code=1000) #normal closure
        return

    # 2. Wait if the job is still QUEUED or ASSIGNED
    if job.status in (JobStatus.QUEUED, JobStatus.ASSIGNED):
        await websocket.send_text(f"[MASTER] Job {job_id} is {job.status.value}. Waiting for execution...\n")
        while job.status in (JobStatus.QUEUED, JobStatus.ASSIGNED):
            await asyncio.sleep(0.5) #asyncio sleep to avoid blocking the fastapi event loop. Sleep to avoid busy waiting and allow other tasks to run

    # Re-check in case it failed immediately upon startup, dump logs immidiately if so
    if job.status in (JobStatus.COMPLETED, JobStatus.FAILED):
        if job.stdout:
            await websocket.send_text(job.stdout)
        if job.stderr:
            await websocket.send_text(f"\n--- STDERR ---\n{job.stderr}")
        await websocket.close(code=1000)
        return

    #the job is running now -> tail the logs
    #finding the worker that is running the job and its control channel (through which we talk with the worker)
    worker_id = job.assigned_worker_id
    control_ws = worker_control_sockets.get(worker_id)
    if not control_ws:
        await websocket.send_text(f"[MASTER] Worker {worker_id} does not have an active control channel.\n")
        await websocket.close(code=1011)
        return

    # 3. Register subscriber queue and instruct the Worker to start streaming
    queue: asyncio.Queue = asyncio.Queue()
    job_log_subscribers.setdefault(job_id, set()).add(queue)
    #master to worker
    await control_ws.send_json({
        "action": "start_tail", 
        "job_id": job_id
    })

    try:
        while True:
            chunk = await queue.get() #receive log chunk from the worker's log ingest websocket
            if chunk is None:  # Worker closed log stream (container exited)
                break
            await websocket.send_text(chunk) #forward the log chunk to the client
    except WebSocketDisconnect:
        pass
    finally:
        subs = job_log_subscribers.get(job_id) #get the set of queues subscribed to this job's logs
        if subs:
            subs.discard(queue) #remove this client's queue from the set of subscribers
            # Stop worker stream only if no other clients are tailing this job
            if not subs:
                del job_log_subscribers[job_id]
                if worker_id in worker_control_sockets:
                    try:
                        await worker_control_sockets[worker_id].send_json({
                            "action": "stop_tail", 
                             "job_id": job_id
                        })
                    except Exception:
                        pass
        try:
            await websocket.close()
        except Exception:
            pass

if __name__ == "__main__":
    uvicorn.run('master:app', host="0.0.0.0", port=8000, reload=True)