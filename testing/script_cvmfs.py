import os

data_path = "/mnt/cvmfs/cms_open_data/Run2012BC_DoubleMuParked_Muons.root"
print(f"Attempting to read CVMFS dataset at: {data_path}")

if os.path.exists(data_path):
    # Get file size in Megabytes to prove we can see the real physics data
    file_size_mb = os.path.getsize(data_path) / (1024 * 1024)
    print(f"SUCCESS! Found dataset directly on shared filesystem.")
    print(f"File Size: {file_size_mb:.2f} MB")
    
    with open("results_cvmfs.txt", "w") as f:
        f.write(f"Analysis complete. Found {file_size_mb:.2f} MB of data.")
else:
    print("FAILED. Dataset not found on worker node.")
    exit(1)