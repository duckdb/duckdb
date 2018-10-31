
import os, time

# move to the base directory
path = os.path.dirname(sys.argv[0])
if len(path) > 0:
	os.chdir(path)
os.chdir("..")

while True:
	if os.system('python run_benchmarks.py') == 0:
		os.system('python benchmark_html.py')
	time.sleep(60)
