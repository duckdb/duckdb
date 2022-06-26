import os
import sys
import subprocess
import threading

if len(sys.argv) < 3:
    print("Expected python3 scripts/try_timeout.py --timeout=[timeout] --retry=[retries] [cmd] [options...]")
    print("Timeout should be given in seconds")
    exit(1)

timeout = int(sys.argv[1].replace("--timeout=", ""))
retries = int(sys.argv[2].replace("--retry=", ""))
cmd = sys.argv[3:]

class Command(object):
    def __init__(self, cmd):
        self.cmd = cmd
        self.process = None

    def run(self, timeout):
        self.process = None
        def target():
            self.process = subprocess.Popen(self.cmd)
            self.process.communicate()

        thread = threading.Thread(target=target)
        thread.start()

        thread.join(timeout)
        if thread.is_alive():
            print('Terminating process: process exceeded timeout of ' + str(timeout) + ' seconds')
            self.process.terminate()
            thread.join()
        if self.process is None:
            return 1
        return self.process.returncode

for i in range(retries):
    print("Attempting to run command \"" + ' '.join(cmd) + '"')
    command = Command(cmd)
    returncode = command.run(timeout)
    if returncode == 0:
        exit(0)

exit(1)
