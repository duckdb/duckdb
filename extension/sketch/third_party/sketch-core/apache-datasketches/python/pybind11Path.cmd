@echo off
:: Takes path to the Python interpreter and returns the path to pybind11
%1 -m pip show pybind11 | %1 -c "import sys,re;[sys.stdout.write(re.sub('^Location:\\s+','',line)) for line in sys.stdin if re.search('^Location:\\s+',line)]"
