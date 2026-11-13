import argparse
import requests
import os
import tarfile
import tempfile
import sys
from websockets import ConnectionClosedOK
from websockets.sync.client import connect as ws_connect
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK

MASTER_URL = "http://localhost:8000"
WS_MASTER_URL = MASTER_URL.replace("http://", "ws://").replace("https://", "wss://")

def parse_sub_file(filepath):
    '''.sub file into a dictionary'''
    
    default_job_def = {
        "request_cpus": 1.0,
        "request_memory": 512.0,
        "requirements": "worker.offered_cpus_available >= job.required_cpus and worker.offered_ram_available >= job.required_ram",
        "transfer_input_files": "",
        "transfer_output_files": "true",
        "checkpoint_file": "",
        "estimated_runtime": 3600,
        "docker_image": "ubuntu:latest",
        "arguments": ""
    }
    
    queue_count = 1
    
    with open(filepath, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            
            #the queue command specifies how many times identical jobs to spawn
            if line.lower().startswith('queue'):
                parts = line.split()
                if len(parts) >1:
                    queue_count = int(parts[1])
                break # Stop reading after the queue command
            
            if '=' in line:
                key, value = line.split('=', 1)
                key = key.strip().lower()
                value = value.strip()
                default_job_def[key] = value   
                
    return default_job_def, queue_count

def submit_jobs(submit_filepath):
    '''translate the parsed dictionary into a REST payload (for the master) and handle sandboxing (input/output files)'''
    default_job_def, queue_count = parse_sub_file(submit_filepath)
    
    for i in range(queue_count):
        #mapping the dict to the pydatic models
        executable = default_job_def.get("executable", "echo 'No executable specified'")
        arguments = default_job_def.get("arguments", "")
        input_files_str = default_job_def.get("transfer_input_files", "")

        payload = {
            "docker_image": default_job_def.get("docker_image"),
            "command": f"{executable} {arguments}".strip(),
            "status": "SPOOLING" if input_files_str else "QUEUED",
            "required_cpus": float(default_job_def.get("request_cpus")),
            "required_ram": float(default_job_def.get("request_memory")),
            "requirements": default_job_def.get("requirements"),
            "estimated_runtime": int(default_job_def.get("estimated_runtime")),
            "has_output_sandbox": str(default_job_def.get("transfer_output_files")).lower() == "true",
            "checkpoint_file": default_job_def.get("checkpoint_file", ""),
        }
        
        resp = requests.post(f"{MASTER_URL}/jobs", json=payload)
        if resp.status_code != 200:
            print(f"Error submitting job: {resp.text}")
            continue
        
        job = resp.json()
        job_id = job.get("job_id")
        print(f"[{i+1}/{queue_count}] Job {job_id} submitted successfully.")
        
        #handling file sandboxing (input files)
        input_files_str = default_job_def.get("transfer_input_files", "")
        if input_files_str:
            input_files = [f.strip() for f in input_files_str.split(',') if f.strip()]
            
            #temporary compressed tarball for the input files
            with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as tmp_tar:
                tar_path = tmp_tar.name
                
            with tarfile.open(tar_path, "w:gz") as tar:
                for file_path in input_files:
                    if os.path.exists(file_path):
                        # arcname ensures we don't preserve the local absolute path structure
                        tar.add(file_path, arcname=os.path.basename(file_path))
                    else:
                        print(f"      -> Warning: Input file '{file_path}' not found locally!")
                        
            #upload the tarball to the master
            with open(tar_path, 'rb') as f_data:
                upload_resp = requests.post(
                    f"{MASTER_URL}/jobs/{job_id}/input",
                    files={"file": ("input.tar.gz", f_data, "application/gzip")}
                )
            
            os.remove(tar_path) # Clean up the local temp file
            
            if upload_resp.status_code == 200:
                print(f"      -> Input sandbox packaged and uploaded.")
            else:
                print(f"      -> Failed to upload sandbox: {upload_resp.text}")
                
def tail_job(job_id: str):
    """Connects to Master WebSocket and streams container logs to stdout."""
    uri = f"{WS_MASTER_URL}/ws/jobs/{job_id}/stream"
    try:
        with ws_connect(uri) as ws:
            while True:
                chunk = ws.recv()
                sys.stdout.write(chunk)
                sys.stdout.flush()
    except ConnectionClosedOK:
        pass
    except ConnectionClosedError as e:
        print(f"\n[TAIL] Stream closed unexpectedly: {e}")
    except KeyboardInterrupt:
        print("\n[TAIL] Detached from log stream (job continues running on cluster).")   
             
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mini-HTCondor Submit CLI")
    parser.add_argument("sub_file", nargs="?", help="Path to the .sub file")
    parser.add_argument("--tail", metavar="JOB_ID", help="Stream real-time logs of a job")
    args = parser.parse_args()

    if args.tail:
        tail_job(args.tail)
    elif args.sub_file:
        submit_jobs(args.sub_file)
    else:
        parser.error("Provide either a .sub file to submit or --tail <job_id>.")
            