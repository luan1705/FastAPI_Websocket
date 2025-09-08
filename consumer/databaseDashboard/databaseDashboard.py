import subprocess
import time

# Danh sách file cần chạy
files = [
    "eboard_table.py",
    "eboard_foreign.py",
    "indices.py"
]

# Lưu process vào dict
processes = {}

def start_process(file_name):
    print(f"🚀 Starting {file_name} ...")
    p = subprocess.Popen(["python", file_name])
    processes[file_name] = p

# Chạy lần đầu
for f in files:
    start_process(f)

try:
    while True:
        time.sleep(5)
        for f, p in list(processes.items()):
            if p.poll() is not None:  # process đã chết
                print(f"⚠️ {f} crashed. Restarting...")
                start_process(f)

except KeyboardInterrupt:
    print("\n🛑 Stopping all processes...")
    for p in processes.values():
        p.terminate()
