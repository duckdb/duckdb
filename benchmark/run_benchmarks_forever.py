
import os, sys, time, subprocess

# move to the base directory
path = os.path.dirname(sys.argv[0])
if len(path) > 0:
    os.chdir(path)
os.chdir("..")

while True:
    proc = subprocess.Popen(['python', 'benchmark/run_benchmarks.py'])
    proc.wait()
    if proc.returncode == 0:
        proc = subprocess.Popen(['python', 'benchmark/benchmark_html.py'])
        proc.wait()
        if proc.returncode != 0:
            print("benchmark_html failed!")
            exit(1)
        os.system('./transfer.sh')
    time.sleep(60)
