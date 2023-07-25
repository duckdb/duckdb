import os
import sys
import time

if len(sys.argv) <= 1:
    print("Expected usage: python3 repeat_until_success.py [command]")
    exit(1)

ntries = 10
sleep_duration = 3
cmd = sys.argv[1]

for i in range(ntries):
    ret = os.system(cmd)
    if ret is None or ret == 0:
        exit(0)
    print("Command {{ " + cmd + " }} failed, retrying (" + str(i + 1) + "/" + str(ntries) + ")")
    time.sleep(sleep_duration)

exit(1)
