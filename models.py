from enum import Enum
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
import uuid
from datetime import datetime, timezone

#ENUMS
class JobStatus(str, Enum):
    QUEUED = "QUEUED"
    ASSIGNED = "ASSIGNED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    HELD = "HELD" #if a job exceeds the max_retries
class WorkerStatus(str, Enum):
    IDLE = "IDLE"
    BUSY = "BUSY"
    DEAD = "DEAD"
    
#models
class Job(BaseModel):
    job_id:str = Field(default_factory=lambda: str(uuid.uuid4()))
    docker_image: str
    command: str
    status: JobStatus = JobStatus.QUEUED
    assigned_worker_id: Optional[str] = None #if a worker dies, all his jobs will be queued again
    
    #requiremens for the job
    required_cpus: float = 1.0
    required_ram: float = 512.0 #in MB
    
    #for the lease
    retry_count: int = 0
    max_retries: int = 3    
    
    #for ClassAd matching
    #an expression instead of our hardcoded logic
    requirements:str = "worker.offered_cpus_available >= job.required_cpus and worker.offered_ram_available >= job.required_ram"
    custom_attributes: Dict[str, Any] = Field(default_factory=dict) #for any additional attributes that the user wants to add to the job
    
    #file sandboxing flags
    has_input_sandbox: bool = False
    has_output_sandbox: bool = False
    
    #Mini-DAGMan: list of parent job_ids that must be completed before this job can run
    #DAGMan: Directed Acyclic Graph Manager, a meta-scheduler for HTCondor (the way it works is in the name itself)
    depends_on: List[str] = Field(default_factory=list)
    
    #execution results
    exit_code: Optional[int] = None
    stdout: Optional[str] = None
    stderr: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    
    #for job scheduling
    submitter_id: str = "default_user"
    user_priority: int = 0 #lower number means higher priority
    estimated_runtime: int = 3600 #in seconds, for backfilling
    
class Worker(BaseModel):
    worker_id:str = Field(default_factory=lambda: str(uuid.uuid4()))
    status: WorkerStatus = WorkerStatus.IDLE
    last_heartbeat: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    
    #offered resources
    offered_cpus_total: float = 4.0
    offered_cpus_available: float = 4.0
    offered_ram_total: int = 8192 #8gb
    offered_ram_available: int = 8192
    
    #ClassAd attributes
    custom_attributes: Dict[str, Any] = Field(default_factory=dict) #for any additional attributes that the user wants to add to the worker
    
#request models
class JobUpdateRequest(BaseModel):
    status: JobStatus
    exit_code: Optional[int] = None
    stdout: Optional[str] = None
    stderr: Optional[str] = None  
    
class HeartbeatRequest(BaseModel):
    status: WorkerStatus
    offered_cpus: float = 4.0
    offered_ram: int = 8192
    custom_attributes: Dict[str, Any] = Field(default_factory=dict)
