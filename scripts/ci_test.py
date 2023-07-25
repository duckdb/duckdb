import sys

fail = False
for i in range(len(sys.argv)):
    if sys.argv[i].startswith("--fail"):
        fail = True

if fail:
    print("Sorry man")
    assert 0
else:
    print("Yeah man")
