import json, os, platform, time
from pathlib import Path
TASK_RUN_ID = "82b329d3-bc56-43f6-9622-43b57c7cc343"
started_at = time.time()
filesystems = []
for location in ("/kaggle/working", "/tmp"):
    st = os.statvfs(location)
    filesystems.append({"path":location,"device":os.stat(location).st_dev,"total_bytes":st.f_blocks*st.f_frsize,"available_bytes":st.f_bavail*st.f_frsize})
mem = {}
for line in Path("/proc/meminfo").read_text().splitlines():
    key, value = line.split(":", 1)
    if key in ("MemTotal", "MemAvailable"):
        mem[key+"_bytes"] = int(value.split()[0])*1024
result = {"task_run_id":TASK_RUN_ID,"started_at_unix":started_at,"finished_at_unix":time.time(),"python":platform.python_version(),"architecture":platform.machine(),"cpu_count":os.cpu_count(),"memory":mem,"filesystems":filesystems,"measurement_scope":"Fresh disposable CPU session; statvfs is capacity visibility, not a 100 GB write test or session-duration measurement. No Dataset uploads performed."}
Path("/kaggle/working/capacity-probe.json").write_text(json.dumps(result, indent=2)+"\n")
print("capacity probe completed; no user data or credentials read")
