from fastapi import FastAPI, HTTPException
from typing import Dict, Optional
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from types import SimpleNamespace
import uvicorn
import asyncio
import os
import json

from models import Job, Worker, JobStatus, WorkerStatus, JobUpdateRequest, HeartbeatRequest

#collector, negotiator and scheduler

LEASE_DURATION = 20  # seconds -> dead worker
SPOOL_FILE = "spool.json"  # file to persist jobs and workers across restarts

#in-memory storage for jobs and workers (adding info, but also match making)
jobs: Dict[str, Job]= {}
workers: Dict[str, Worker] = {}

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
    
    #ClassAd matching
    for job in jobs.values():
        if(job.status == JobStatus.QUEUED):
            #checking resource requirements
            if evaluate_classad(job, worker):
                
                job.status = JobStatus.ASSIGNED
                job.assigned_worker_id = worker_id
                
                worker.offered_cpus_available -= job.required_cpus
                worker.offered_ram_available -= job.required_ram
                
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
    
    #checking if this is the 1st timethe job is being marked as COMPLETED or FAILED (to avoid double counting)
    is_newly_finished = (job.status not in [JobStatus.COMPLETED, JobStatus.FAILED]) and (update_req.status in [JobStatus.COMPLETED, JobStatus.FAILED])
    
    #updating the state
    job.status = update_req.status
    
    if update_req.exit_code is not None:
        job.exit_code = update_req.exit_code
    if update_req.stdout is not None:
        job.stdout = update_req.stdout
    if update_req.stderr is not None:
        job.stderr = update_req.stderr
        
    # 3. Only restore resources if the transition actually happened just now
    if is_newly_finished:
        if job.assigned_worker_id and job.assigned_worker_id in workers:
            w = workers[job.assigned_worker_id]
            #restoring the master node capacity for the worker (with a min() clamp, just in case)
            w.offered_cpus_available = min(w.offered_cpus_total, w.offered_cpus_available + job.required_cpus)
            w.offered_ram_available = min(w.offered_ram_total, w.offered_ram_available + job.required_ram)
            
            # For dynamic multi-tenancy, a worker is only IDLE if it has 100% of its resources free.
            # Otherwise, it remains BUSY (partially occupied).
            if w.offered_cpus_available == w.offered_cpus_total and w.offered_ram_available == w.offered_ram_total:
                w.status = WorkerStatus.IDLE
           
    save_state()  # Persist the updated state to disk
    return job

if __name__ == "__main__":
    uvicorn.run('master:app', host="0.0.0.0", port=8000, reload=True)