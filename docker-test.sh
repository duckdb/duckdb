#!/bin/bash

# Commands to build and run python crash

docker build -t collab-test -f Dockerfile_Colab .
docker run --ulimit core=0 -it --entrypoint /bin/bash collab-test

# Run container as collab host
# docker run --ulimit core=0 -p 127.0.0.1:9000:8080 collab-test

# See https://research.google.com/colaboratory/local-runtimes.html
# Basically,
# In Colab, click the "Connect" button and select "Connect to local runtime...".
# Enter the URL from the docker output in the dialog that appears and click the "Connect" button.
# After this, you should now be connected to your local runtime.

# Google colab doc is: https://colab.research.google.com/drive/1z33p9vj7BWJvou2_-z98iycXXm7W8eU1?usp=sharing

# From the docker shell
# export DUCKDEBUG=1
# vi src/main/client_context.cpp
# vi tools/pythonpkg/tests/fast/api/test_duckdb_interrupt.py
# pip install -e tools/pythonpkg
python tools/pythonpkg/tests/fast/api/test_duckdb_interrupt.py