import threading
import time
import uuid
import requests
import docker
import tempfile
import tarfile
import os
import shutil
from typing import Dict, Any

MASTER_URL = "http://localhost:8000"
WORKER_ID = f"worker-{uuid.uuid4().hex[:8]}"  # Unique identifier for this worker
HEARTBEAT_INTERVAL = 5  # seconds

#key = host path (windows), value = container path (linux)
LOCAL_DATASET = {
    os.path.abspath("./shared_cvmfs_data"): "/mnt/cvmfs/cms_open_data"
}

#initializing a docker client
try:
    docker_client = docker.from_env()
except docker.errors.DockerException:
    print("Error: Docker is not running. Please start Docker Engine.")
    exit(1)
    
active_jobs_lock = threading.Lock()
active_jobs_count = 0

def get_worker_status() -> str:
    #determine status based on running threads (active jobs)
    with active_jobs_lock:
        return 'BUSY' if active_jobs_count > 0 else 'IDLE'

def heartbeat_daemon():
    while True:
        status = get_worker_status()
        try:
            requests.post(
                f"{MASTER_URL}/workers/{WORKER_ID}/heartbeat",
                json={
                    "status": status,
                    "offered_cpus": 4.0,
                    "offered_ram": 8192, 
                    "custom_attributes": {
                        "os_family": "el9", #enterprise linux 9, for example
                        "has_cvmfs": True,  #cern virtual file system for software distribution
                        "cached_datasets": ["Run2012BC_DoubleMuParked_Muons"],
                        "gpu_available": False
                    }
                }
            )
        except requests.exceptions.RequestException:
            pass
        time.sleep(HEARTBEAT_INTERVAL)
        
def fetch_job() -> Dict[str, Any]:
    """requests the next available job from the master node (central manager)"""
    try:
        response = requests.get(f"{MASTER_URL}/jobs/next", params={"worker_id": WORKER_ID})
        if response.status_code == 200 and response.json():
            print(f"[{WORKER_ID}] Received job: {response.json()['job_id']}")
            return response.json()    
    except requests.exceptions.ConnectionError:
        print(f"[{WORKER_ID}] Failed to fetch job. Master node may be down.")
        pass
    return None

def update_job(job_id: str, status: str, exit_code: int = 0, stdout: str = "", stderr: str = ""):
    payload = {
        #job_id is already in the URL, and the master reads it from there
        "status": status,
        "exit_code": exit_code,
        "stdout": stdout,
        "stderr": stderr
    }
    try:
        requests.put(f"{MASTER_URL}/jobs/{job_id}", json=payload)
    except requests.exceptions.RequestException as e:
        print(f"[{WORKER_ID}] Failed to send job outcome: {e}")
  
def execute_job_thread(job: Dict[str, Any]):
    global active_jobs_count
    with active_jobs_lock:
        active_jobs_count+=1
        
    #1.job info
    job_id = job["job_id"]
    image = job["docker_image"] #input file
    command = job["command"] #executable command
    
    #1.1 limit enforcements
    nano_cpus = int(job["required_cpus"] * 1e9)  
    mem_limit = f"{int(job['required_ram'])}m"  # Forces "512m" instead of "512.0m"
    
    print(f"[{WORKER_ID}] Picked up job {job_id}. Pulling {image}...")
    
    #2. Job processing: pull the docker image and run the command
    try:
        #so that the image exists locally
        docker_client.images.pull(image)
        docker_client.images.pull(image)
        
        # Create a local directory within the project folder that Docker can easily mount
        workspace_dir = os.path.abspath(f"./worker_workspaces/{job_id}")
        os.makedirs(workspace_dir, exist_ok=True)
        
        try:
            print(f"[{WORKER_ID}] Using local workspace directory: {workspace_dir}")
            if job.get("has_input_sandbox"):
                print(f"[{WORKER_ID}] Downloading input sandbox for {job_id}...")
                resp = requests.get(f"{MASTER_URL}/jobs/{job_id}/input", stream=True)
                if resp.status_code == 200:
                    tar_path = os.path.join(workspace_dir, "input.tar.gz")
                    
                    with open(tar_path, "wb") as buffer:
                        for chunk in resp.iter_content(chunk_size=8192):
                            buffer.write(chunk)
                        
                    with tarfile.open(tar_path, "r:gz") as tar:
                        tar.extractall(path=workspace_dir)
                    os.remove(tar_path)
            
            # dynamically build the volume mount dictionary
            volumes_dict = {workspace_dir: {'bind': '/workspace', 'mode': 'rw'}}
            for host_path, guest_path in LOCAL_DATASET.items():
                if os.path.exists(host_path):
                    volumes_dict[host_path] = {'bind': guest_path, 'mode': 'ro'}
                else:
                    print(f"[{WORKER_ID}] Warning: Local dataset path '{host_path}' does not exist. Skipping mount.")
        
            # run the container detached
            print(f"[{WORKER_ID}] Executing: {command} in sandboxed environment.")
            container = docker_client.containers.run(
                image = image, 
                command = command,
                nano_cpus = nano_cpus,
                mem_limit = mem_limit, 
                volumes = volumes_dict,
                working_dir = '/workspace',
                detach=True
            )
        
            start_time = time.time()
            grace_period = 10
            runtime_limit = job.get("estimated_runtime", 3600)
            evicted = False
            
            while True:
                container.reload()
                
                if container.status in ["exited", "dead"]:
                    result = container.wait()
                    exit_code = result.get("StatusCode", -1)
                    break
                
                elapsed_time = time.time() - start_time
                if elapsed_time > runtime_limit and not evicted:
                    print(f"[{WORKER_ID}] Job {job_id} exceeded {runtime_limit}s limit. Gracefully evicting...")
                    threading.Thread(target=container.stop, kwargs={'timeout': grace_period}).start()
                    evicted = True
                time.sleep(2)
            
            stdout = container.logs(stdout=True, stderr=False).decode("utf-8")
            stderr = container.logs(stdout=False, stderr=True).decode("utf-8")
        
            if evicted:
                stderr += f"\n[SYSTEM] Job forcefully evicted after exceeding {runtime_limit}s estimated runtime."
                exit_code = 124
                
            container.remove()
            
            if job.get("has_output_sandbox"):
                print(f"[{WORKER_ID}] Uploading output sandbox for {job_id}...")
                out_tar_path = os.path.join(workspace_dir, "output.tar.gz")
                with tarfile.open(out_tar_path, "w:gz") as tar:
                    for item in os.listdir(workspace_dir):
                        if item != "output.tar.gz":
                            tar.add(os.path.join(workspace_dir, item), arcname=item)
                
                with open(out_tar_path, "rb") as f:
                    requests.post(
                        f"{MASTER_URL}/jobs/{job_id}/output",
                        files={"file": ("output.tar.gz", f, "application/gzip")}
                    )
            
            final_status = "COMPLETED" if exit_code == 0 else "FAILED"
            update_job(job_id, status=final_status, exit_code=exit_code, stdout=stdout, stderr=stderr)
            
        finally:
            # Clean up the physical directory after the job completes or fails
            shutil.rmtree(workspace_dir, ignore_errors=True)
            
    except Exception as e:
        #catch image pull or container run errors
        print(f"[{WORKER_ID}] Execution failed: {str(e)}")
        #3. update the job status
        update_job(job_id, status="FAILED", exit_code=-1, stderr=str(e))
    finally:
        with active_jobs_lock:
            active_jobs_count-=1
        print(f"[{WORKER_ID}] Freed slot from job {job_id}. Active jobs: {active_jobs_count}") 
    
def run_worker():
    """main loop for the worker to send heartbeats and process jobs"""
    print(f"[{WORKER_ID}] starting up...")
    
    hb_thread = threading.Thread(target=heartbeat_daemon, daemon=True)
    hb_thread.start()
    
    while True:
        job = fetch_job()
        if job:
            # Dispatch the execution to a background thread and immediately loop again
            job_thread = threading.Thread(target=execute_job_thread, args=(job,))
            job_thread.start()
            
            time.sleep(0.5)  # Short sleep to avoid overwhelming the master api with requests
        else:
            time.sleep(2)  # No jobs available, wait before polling again
             
if __name__ == "__main__":
    run_worker()
