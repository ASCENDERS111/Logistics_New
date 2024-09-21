import subprocess
import time

# Run ff.py
print("Running CLT_Main.py...")
subprocess.run(["python", "CLT_Main.py"])

# Wait for 5 seconds (optional, if you need to delay between scripts)
time.sleep(1)

# Run gg.py
print("Running Poonam_Main.py...")
subprocess.run(["python", "Poonam_Main.py"])

# Run gg.py
print("Running krati..Script...")
subprocess.run(["python", "krati.py"])

print("All scripts have finished running.")
