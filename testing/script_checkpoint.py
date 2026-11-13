import sys
import time
import signal
import json
import os

CHECKPOINT_FILE = "checkpoint.json"
state = {"events_processed":0}

def sigterm_handler(signum, frame):
    #triggered by workers graceful eviction
    print("\n[SYSTEM] SIGTERM received! Serializing state to disk...", flush=True)
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(state, f)
    print("[SYSTEM] Checkpoint saved. Exiting gracefully.", flush=True)
    sys.exit(0)
    
# Register the signal handler with the OS
signal.signal(signal.SIGTERM, sigterm_handler)

if __name__ == "__main__":
    # 1. Resume from checkpoint if it exists
    if os.path.exists(CHECKPOINT_FILE):
        print("Found checkpoint! Resuming job...")
        with open(CHECKPOINT_FILE, "r") as f:
            state = json.load(f)
    else:
        print("No checkpoint found. Starting from scratch...")

    target_events = 20
    
    # 2. Main computation loop
    while state["events_processed"] < target_events:
        state["events_processed"] += 1
        print(f"Processing Monte Carlo event {state['events_processed']}/{target_events}...", flush=True)
        time.sleep(1.0)  # Simulate heavy vectorization work
        
    print("Simulation completed successfully!")
    
    # Clean up the checkpoint file once finished so we don't pollute the final output sandbox
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)