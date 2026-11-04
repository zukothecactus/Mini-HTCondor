from fastapi import FastAPI, HTTPException
from typing import Dict, Optional
from datetime import datetime, timezone
import uvicorn
import asyncio
from contextlib import asynccontextmanager

from models import Job, Worker, JobStatus, WorkerStatus, JobUpdateRequest

#collector, negotiator and scheduler

LEASE_DURATION = 20  # seconds -> dead worker

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start background negotiator / reconciliation task on app startup
    reconciliation_task = asyncio.create_task(master_reconciliation_loop())
    yield
    reconciliation_task.cancel()

app = FastAPI(title = "Mini-HTCondor master node", description = "Implementation of a master node with ClassAd matchmaking and lease reconciliation.", lifespan = lifespan)

#in-memory storage for jobs and workers (adding info, but also match making)
jobs: Dict[str, Job]= {}
workers: Dict[str, Worker] = {}

async def master_reconciliation_loop():
    ''' periodic bk loop: checks worker health and requeues lost jobs'''
    while True:
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
        await asyncio.sleep(5)

#adding a job to the queue
@app.post("/jobs", response_model = Job)
def submit_job(job_req:Job):
    """client submits a new job to the cluster"""
    
    #pydantic automatically generates a job_id and a timestamp
    jobs[job_req.job_id] = job_req
    return job_req

@app.post("/workers/{worker_id}/heartbeat")
def worker_heartbeat(worker_id:str, 
                     status: WorkerStatus = WorkerStatus.IDLE,
                     offered_cpus: float = 4.0,
                     offered_ram_available: int = 8192):
    """worker pings this endpoint every few secontd to let master know that it is alive"""
    
    if(worker_id not in workers):
        workers[worker_id] = Worker(worker_id = worker_id,
                                    status= status,
                                    offered_cpus_total=offered_cpus,
                                    offered_cpus_available=offered_cpus,
                                    offered_ram_total=offered_ram_available,
                                    offered_ram_available=offered_ram_available)
    else:
        workers[worker_id].last_heartbeat = datetime.now(timezone.utc)
        workers[worker_id].status = status
                
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
            if(job.required_cpus <= worker.offered_cpus and
               job.required_ram <= worker.offered_ram_available):
                job.status = JobStatus.ASSIGNED
                job.assigned_worker_id = worker_id
                
                worker.offered_cpus_available -= job.required_cpus
                worker.offered_ram_available -= job.required_ram
                
                if worker.offered_cpus_available == 0: 
                    worker.status = WorkerStatus.BUSY

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
            
    return job

if __name__ == "__main__":
    uvicorn.run('master:app', host="0.0.0.0", port=8000, reload=True)