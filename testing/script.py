import sys
import time
import random
import json
import os

def simulate_events(num_events):
    print(f"Starting Monte Carlo generation of {num_events} events...")
    hits = 0
    
    for i in range(num_events):
        x = random.uniform(-1, 1)
        y = random.uniform(-1, 1)
        if x**2 + y**2 <= 1.0:
            hits += 1
            
        #fake delay to simulate heavy computation and keep the container alive long enough for the master to register the runnning state
        if i>0 and i%(num_events//5) == 0:
            print(f"Progress: {i}/{num_events} events generated.")
            time.sleep(2.0) 
            
    estimate = 4 * hits / num_events
    print(f"Estimated value of pi: {estimate}")
    return estimate

if __name__ == "__main__":
    #we allow the injection of event counts via CLI args
    num_events = 1000000  # default
    if len(sys.argv) > 1:
        try:
            num_events = int(sys.argv[1])
        except ValueError:
            print("Invalid number of events specified. Using default of 1,000,000.")
            pass
    
    print("--- Mini-HTCondor Job Started ---")
    print(f"Working Directory: {os.getcwd()}")
    
    start_time = time.time()
    result = simulate_events(num_events)
    elapsed = time.time() - start_time
    
    print(f"Simulation complete in {elapsed:.2f} seconds.")
    print(f"Calculated Value: {result}")
    
    #to test the output sandbox packaging
    output_data = {
        "events_processed": num_events,
        "calculated_value": result,
        "compute_time_seconds": elapsed,
        "status": "success"
    }
    with open("results.json", "w") as f:
        json.dump(output_data, f, indent=4)
        
    print("Metrics successfully written to results.json")
    print("--- Job Finished ---")   
    
    