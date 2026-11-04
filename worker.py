import threading
import time
import uuid
import requests
import docker
from typing import Dict, Any

MASTER_URL = "http://localhost:8000"
WORKER_ID = f"worker-{uuid.uuid4().hex[:8]}"  # Unique identifier for this worker
HEARTBEAT_INTERVAL = 5  # seconds

#initializing a docker client
try:
    docker_client = docker.from_env()
except docker.errors.DockerException:
    print("Error: Docker is not running. Please start Docker Engine.")
    exit(1)
    
# Thread-safe worker status tracker
current_worker_status = "IDLE"
status_lock = threading.Lock()
def set_worker_status(status: str):
    global current_worker_status
    with status_lock:
        current_worker_status = status

def get_worker_status() -> str:
    with status_lock:
        return current_worker_status
    
def heartbeat_daemon():
    while True:
        status = get_worker_status()
        try:
            requests.post(
                f"{MASTER_URL}/workers/{WORKER_ID}/heartbeat",
                params={
                    "status": status,
                    "offered_cpus": 4.0,
                    "offered_ram": 8192
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
    
def run_worker():
    """main loop for the worker to send heartbeats and process jobs"""
    print(f"[{WORKER_ID}] starting up...")
    
    hb_thread = threading.Thread(target=heartbeat_daemon, daemon=True)
    hb_thread.start()
    
    while True:
        #1.ask for work
        job = fetch_job()
        if job:
            job_id = job["job_id"]
            image = job["docker_image"] #input file
            command = job["command"] #executable command
            print(f"[{WORKER_ID}] Picked up job {job_id}. Pulling {image}...")
            
            #1.1 limit enforcements
            nano_cpus = int(job["required_cpus"] * 1e9)  # CPU limit in nanoseconds
            mem_limit = f"{job['required_ram']}m"  # Memory limit in megabytes
            
            #2. inform the master node that the worker is busy
            set_worker_status("BUSY")
            
            #3. Job processing: pull the docker image and run the command
            try:
                #so that the image exists locally
                docker_client.images.pull(image)
                
                #run the dontainer detached (in the background) so we can wait for it to finish and capture the output
                print(f"[{WORKER_ID}] Executing: {command}")
                container = docker_client.containers.run(
                    image = image, 
                    command = command,
                    nano_cpus = nano_cpus,
                    mem_limit = mem_limit, 
                    detach=True)
                #command is executed in a independent space
                
                #block until the container finishes
                result = container.wait()
                exit_code = result.get("StatusCode", -1)
                
                #extract logs (output files)
                stdout = container.logs(stdout=True, stderr=False).decode("utf-8")
                stderr = container.logs(stdout=False, stderr=True).decode("utf-8")
                
                #clean up the container
                container.remove()
                
                #reports
                final_status = "COMPLETED" if exit_code == 0 else "FAILED"
                update_job(job_id, status=final_status, exit_code=exit_code, stdout=stdout, stderr=stderr)
                
            except Exception as e:
                #catch image pull or container run errors
                print(f"[{WORKER_ID}] Execution failed: {str(e)}")
                #4. update the job status
                update_job(job_id, status="FAILED", exit_code=-1, stderr=str(e))
            finally:
                set_worker_status("IDLE")
        else:
            print(f"[{WORKER_ID}] No jobs available. Waiting...")
            #wait before the next cycle
            time.sleep(2)
        
if __name__ == "__main__":
    run_worker()
